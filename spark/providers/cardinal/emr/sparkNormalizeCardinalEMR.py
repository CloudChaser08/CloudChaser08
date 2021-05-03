import argparse
from datetime import datetime, date
from spark.runner import Runner
from spark.spark_setup import init
import spark.helpers.file_utils as file_utils
import spark.helpers.payload_loader as payload_loader
import spark.helpers.external_table_loader as external_table_loader
import spark.helpers.postprocessor as postprocessor
import spark.helpers.udf.post_normalization_cleanup as post_norm_cleanup
import spark.helpers.explode as explode
import spark.helpers.schema_enforcer as schema_enforcer
import spark.helpers.normalized_records_unloader as normalized_records_unloader
from spark.helpers.privacy.common import Transformer, TransformFunction

from spark.common.emr.vital_sign import schema_v6 as vital_sign_schema
from spark.common.emr.medication import schema_v6 as medication_schema
from spark.common.emr.clinical_observation import schema_v6 as clinical_observation_schema
from spark.common.emr.encounter import schema_v6 as encounter_schema
from spark.common.emr.diagnosis import schema_v7 as diagnosis_schema
from spark.common.emr.lab_result import schema_v6 as lab_result_schema
from spark.common.emr.procedure import schema_v6 as procedure_schema
from spark.common.emr.provider_order import schema_v6 as provider_order_schema

from spark.common.utility.output_type import DataType, RunType
from spark.common.utility.run_recorder import RunRecorder
from spark.common.utility import logger

import spark.providers.cardinal.emr.transactional_schemas as cardinal_schemas
from spark.helpers.privacy.emr import                   \
    encounter as priv_encounter,                        \
    diagnosis as priv_diagnosis,                        \
    procedure as priv_procedure,                        \
    provider_order as priv_prov_order,                  \
    lab_result as priv_lab_result,                      \
    medication as priv_medication,                      \
    clinical_observation as priv_clinical_observation,  \
    vital_sign as priv_vital_sign

# staging for deliverable
DELIVERABLE_LOC = 'hdfs:///deliverable/'

FEED_ID = '40'
VENDOR_ID = '42'

DEOBFUSCATION_KEY = 'Cardinal_MPI-0'

OUTPUT_PATH_TEST = 's3://salusv/testing/dewey/airflow/e2e/cardinal/emr/spark-output/'
OUTPUT_PATH_PRODUCTION = 's3://salusv/warehouse/parquet/emr/2018-02-12/'

script_path = __file__


def run(spark, runner, date_input, num_output_files=1, batch_id=None,
        test=False, airflow_test=False):
    global DELIVERABLE_LOC

    if test:
        input_path = file_utils.get_abs_path(
            script_path, '../../../test/providers/cardinal/emr/resources/input/'
        ) + '/'
        matching_path = file_utils.get_abs_path(
            script_path, '../../../test/providers/cardinal/emr/resources/matching/'
        ) + '/'
        DELIVERABLE_LOC = file_utils.get_abs_path(
            script_path, '../../../test/providers/cardinal/emr/resources/delivery/'
        ) + '/'
    elif airflow_test:
        input_path = 's3://salusv/testing/dewey/airflow/e2e/cardinal/emr/out/{}/'.format(
            batch_id if batch_id is not None else date_input.replace('-', '/')
        )
        matching_path = 's3://salusv/testing/dewey/airflow/e2e/cardinal/emr/matching/{}/'.format(
            batch_id if batch_id is not None else date_input.replace('-', '/')
        )
    else:
        input_path = 's3a://salusv/incoming/emr/cardinal/{}/'.format(
            batch_id if batch_id is not None else date_input.replace('-', '/')
        )
        matching_path = 's3a://salusv/matching/payload/emr/cardinal/{}/'.format(
            batch_id if batch_id is not None else date_input.replace('-', '/')
        )

    explode.generate_exploder_table(spark, 6, 'clin_obs_exploder')
    explode.generate_exploder_table(spark, 13, 'vit_sign_exploder')

    if not test:
        external_table_loader.load_ref_gen_ref(runner.sqlContext)

    hvm_available_history_date = \
        postprocessor.get_gen_ref_date(runner.sqlContext, FEED_ID, "HVM_AVAILABLE_HISTORY_DATE")
    earliest_valid_service_date = \
        postprocessor.get_gen_ref_date(runner.sqlContext, FEED_ID, "EARLIEST_VALID_SERVICE_DATE")
    hvm_historical_date = hvm_available_history_date if hvm_available_history_date else \
        earliest_valid_service_date if earliest_valid_service_date else date(1901, 1, 1)
    max_date = date_input

    payload_loader.load(runner, matching_path, ['hvJoinKey'])

    transactional_tables = [
        ('demographics', cardinal_schemas.demographic),
        ('encounter', cardinal_schemas.encounter),
        ('diagnosis', cardinal_schemas.diagnosis),
        ('lab', cardinal_schemas.lab),
        ('dispense', cardinal_schemas.order_dispense)
    ]

    for (table_name, table_schema) in transactional_tables:
        postprocessor.compose(
            postprocessor.trimmify, lambda df: postprocessor.nullify(
                df,
                null_vals=['', 'NULL'],
                preprocess_func=lambda c: c.upper() if c else c
            )
        )(
            runner.sqlContext.read.csv(input_path + table_name + '/', table_schema, sep='|')
        ).createOrReplaceTempView('transactions_' + table_name)

    # deduplicate demographics table
    runner.sqlContext.sql("""
    SELECT dem.*
    FROM transactions_demographics dem
    INNER JOIN (
      SELECT mx.patient_id, MAX(mx.hvJoinKey) AS hvJoinKey
      FROM transactions_demographics mx
      GROUP BY mx.patient_id
      ) upi ON dem.patient_id = upi.patient_id AND dem.hvJoinKey = upi.hvJoinKey
    """).createOrReplaceTempView('transactions_demographics')

    normalized_encounter = \
        runner.run_spark_script('normalize_encounter.sql', return_output=True, source_file_path=script_path)
    normalized_diagnosis = \
        runner.run_spark_script('normalize_diagnosis.sql', return_output=True, source_file_path=script_path)
    normalized_procedure = schema_enforcer.apply_schema(
        runner.run_spark_script(
            'normalize_procedure_enc.sql', return_output=True, source_file_path=script_path
        ), procedure_schema
    ).union(schema_enforcer.apply_schema(
        runner.run_spark_script(
            'normalize_procedure_disp.sql', return_output=True, source_file_path=script_path
        ), procedure_schema
    ))
    normalized_provider_order = runner.run_spark_script(
        'normalize_provider_order.sql', return_output=True, source_file_path=script_path
    )
    normalized_lab_result = runner.run_spark_script(
        'normalize_lab_result.sql', return_output=True, source_file_path=script_path
    )
    normalized_medication = runner.run_spark_script(
        'normalize_medication.sql', return_output=True, source_file_path=script_path
    )
    normalized_clinical_observation = runner.run_spark_script(
        'normalize_clinical_observation.sql', return_output=True, source_file_path=script_path
    )
    normalized_vital_sign = schema_enforcer.apply_schema(
        runner.run_spark_script(
            'normalize_vital_sign_enc.sql', return_output=True, source_file_path=script_path
        ), vital_sign_schema
    ).union(schema_enforcer.apply_schema(
        runner.run_spark_script(
            'normalize_vital_sign_lab.sql', return_output=True, source_file_path=script_path
        ), vital_sign_schema
    ))

    normalized_tables = [
        {
            'normalized_data': normalized_encounter,
            'table_name': 'normalized_encounter',
            'schema': encounter_schema,
            'script_name': 'emr/encounter_common_model_v6.sql',
            'data_type': 'encounter',
            'model_version': '06',
            'date_column': 'enc_start_dt',
            'privacy_filter': priv_encounter,
            'custom_whitelist_additions': lambda whitelists: whitelists + [
                {
                    'column_name': 'enc_typ_cd',
                    'comp_qual_names': ['enc_typ_cd_qual'],
                    'domain_name': 'emr_enc.enc_typ_cd',
                    'whitelist_col_name': 'gen_ref_cd',
                    'clean_up_freetext_fn': lambda x: x
                }
            ],
            'custom_privacy_transformer': Transformer(
                enc_rndrg_prov_npi=[
                    TransformFunction(lambda x: x, ['enc_rndrg_prov_npi'])
                ]
            ),
            'date_caps': [
                ('enc_start_dt', 'EARLIEST_VALID_SERVICE_DATE'),
                ('enc_end_dt', 'EARLIEST_VALID_SERVICE_DATE')
            ]
        }, {
            'normalized_data': normalized_diagnosis,
            'table_name': 'normalized_diagnosis',
            'schema': diagnosis_schema,
            'script_name': 'emr/diagnosis_common_model_v7.sql',
            'data_type': 'diagnosis',
            'model_version': '07',
            'date_column': 'diag_dt',
            'privacy_filter': priv_diagnosis,
            'custom_whitelist_additions': lambda whitelists: whitelists + [
                {
                    'column_name': 'diag_resltn_desc',
                    'domain_name': 'emr_diag.diag_resltn_desc',
                    'whitelist_col_name': 'gen_ref_itm_desc',
                    'clean_up_freetext_fn': lambda x: x
                },
                {
                    'column_name': 'diag_meth_cd',
                    'comp_col_names': ['diag_meth_cd_qual'],
                    'domain_name': 'emr_diag.diag_meth_cd',
                    'whitelist_col_name': 'gen_ref_cd',
                    'clean_up_freetext_fn': lambda x: x
                }
            ],
            'custom_privacy_transformer': Transformer(
                diag_rndrg_prov_npi=[
                    TransformFunction(lambda x: x, ['diag_rndrg_prov_npi'])
                ]
            ),
            'date_caps': [
                ('diag_dt', 'EARLIEST_VALID_DIAGNOSIS_DATE'),
                ('diag_resltn_dt', 'EARLIEST_VALID_DIAGNOSIS_DATE')
            ]
        }, {
            'normalized_data': normalized_procedure,
            'table_name': 'normalized_procedure',
            'schema': procedure_schema,
            'script_name': 'emr/procedure_common_model_v6.sql',
            'data_type': 'procedure',
            'model_version': '06',
            'date_column': 'proc_dt',
            'privacy_filter': priv_procedure,
            'custom_privacy_transformer': Transformer(
                proc_rndrg_prov_npi=[
                    TransformFunction(lambda x: x, ['proc_rndrg_prov_npi'])
                ]
            ),
            'date_caps': [
                ('enc_dt', 'EARLIEST_VALID_SERVICE_DATE'),
                ('proc_dt', 'EARLIEST_VALID_SERVICE_DATE'),
                ('data_captr_dt', 'EARLIEST_VALID_SERVICE_DATE')
            ]
        }, {
            'normalized_data': normalized_provider_order,
            'table_name': 'normalized_provider_order',
            'schema': provider_order_schema,
            'script_name': 'emr/provider_order_common_model_v6.sql',
            'data_type': 'provider_order',
            'model_version': '06',
            'date_column': 'prov_ord_dt',
            'privacy_filter': priv_prov_order,
            'custom_privacy_transformer': Transformer(
                prov_ord_cd=[
                    TransformFunction(post_norm_cleanup.clean_up_procedure_code, ['prov_ord_cd'])
                ]
            ),
            'date_caps': [
                ('enc_dt', 'EARLIEST_VALID_SERVICE_DATE'),
                ('prov_ord_dt', 'EARLIEST_VALID_SERVICE_DATE'),
                ('prov_ord_start_dt', 'EARLIEST_VALID_SERVICE_DATE'),
                ('prov_ord_end_dt', 'EARLIEST_VALID_SERVICE_DATE'),
                ('prov_ord_complt_dt', 'EARLIEST_VALID_SERVICE_DATE'),
                ('prov_ord_cxld_dt', 'EARLIEST_VALID_SERVICE_DATE'),
                ('data_captr_dt', 'EARLIEST_VALID_SERVICE_DATE')
            ]
        }, {
            'normalized_data': normalized_lab_result,
            'table_name': 'normalized_lab_result',
            'schema': lab_result_schema,
            'script_name': 'emr/lab_result_common_model_v6.sql',
            'data_type': 'lab_result',
            'model_version': '06',
            'date_column': 'lab_test_execd_dt',
            'privacy_filter': priv_lab_result,
            'date_caps': [
                ('lab_test_execd_dt', 'EARLIEST_VALID_SERVICE_DATE')
            ]
        }, {
            'normalized_data': normalized_medication,
            'table_name': 'normalized_medication',
            'schema': medication_schema,
            'script_name': 'emr/medication_common_model_v6.sql',
            'data_type': 'medication',
            'model_version': '06',
            'date_column': 'medctn_admin_dt',
            'privacy_filter': priv_medication,
            'custom_privacy_transformer': Transformer(
                medctn_ordg_prov_npi=[
                    TransformFunction(lambda x: x, ['medctn_ordg_prov_npi'])
                ]
            ),
            'custom_whitelist_additions': lambda whitelists: whitelists + [
                {
                    'column_name': 'medctn_strth_txt_qual',
                    'domain_name': 'emr_medctn.medctn_strth_txt_qual',
                }
            ],
            'date_caps': [
                ('enc_dt', 'EARLIEST_VALID_SERVICE_DATE'),
                ('medctn_ord_dt', 'EARLIEST_VALID_SERVICE_DATE'),
                ('medctn_admin_dt', 'EARLIEST_VALID_SERVICE_DATE'),
                ('medctn_end_dt', 'EARLIEST_VALID_SERVICE_DATE'),
                ('medctn_ord_cxld_dt', 'EARLIEST_VALID_SERVICE_DATE'),
                ('data_captr_dt', 'EARLIEST_VALID_SERVICE_DATE')
            ]
        }, {
            'normalized_data': normalized_clinical_observation,
            'table_name': 'normalized_clinical_observation',
            'schema': clinical_observation_schema,
            'script_name': 'emr/clinical_observation_common_model_v6.sql',
            'data_type': 'clinical_observation',
            'model_version': '06',
            'date_column': 'clin_obsn_dt',
            'privacy_filter': priv_clinical_observation,
            'custom_privacy_transformer': Transformer(
                clin_obsn_rndrg_prov_npi=[
                    TransformFunction(lambda x: x, ['clin_obsn_rndrg_prov_npi'])
                ]
            ),
            'custom_whitelist_additions': lambda whitelists: whitelists + [
                {
                    'column_name': 'clin_obsn_desc',
                    'domain_name': 'emr_clin_obsn.clin_obsn_desc',
                    'whitelist_col_name': 'gen_ref_itm_desc',
                    'clean_up_freetext_fn': lambda x: x
                }
            ],
            'date_caps': [
                ('clin_obsn_dt', 'EARLIEST_VALID_DIAGNOSIS_DATE'),
                ('clin_obsn_resltn_dt', 'EARLIEST_VALID_DIAGNOSIS_DATE')
            ]
        }, {
            'normalized_data': normalized_vital_sign,
            'table_name': 'normalized_vital_sign',
            'schema': vital_sign_schema,
            'script_name': 'emr/vital_sign_common_model_v6.sql',
            'data_type': 'vital_sign',
            'model_version': '06',
            'date_column': 'vit_sign_dt',
            'privacy_filter': priv_vital_sign,
            'date_caps': [
                ('enc_dt', 'EARLIEST_VALID_SERVICE_DATE'),
                ('vit_sign_dt', 'EARLIEST_VALID_SERVICE_DATE')
            ]
        }
    ]

    for table in normalized_tables:
        postprocessor.compose(
            lambda df: schema_enforcer.apply_schema(df, table['schema']),
            postprocessor.add_universal_columns(
                feed_id=FEED_ID, vendor_id=VENDOR_ID,
                filename='EMR.{}.zip'.format(
                    batch_id if batch_id is not None else datetime.strptime(date_input, '%Y-%m-%d').strftime('%m%d%Y')),
                model_version_number=table['model_version'],

                # rename defaults
                record_id='row_id', created='crt_dt', data_set='data_set_nm',
                data_feed='hvm_vdr_feed_id', data_vendor='hvm_vdr_id',
                model_version='mdl_vrsn_num'
            ),
            table['privacy_filter'].filter(
                runner.sqlContext, additional_transformer=table.get('custom_privacy_transformer'),
                update_whitelists=table.get('custom_whitelist_additions', lambda x: x)
            ),
            *[
                postprocessor.apply_date_cap(runner.sqlContext, date_col, max_date, FEED_ID, domain_name)
                for (date_col, domain_name) in table['date_caps']
            ]
        )(
            table['normalized_data']
        ).repartition(1 if test else 2001).createOrReplaceTempView(table['table_name'])

        # unload delivery file for cardinal
        normalized_records_unloader.unload_delimited_file(
            spark, runner, '{}{}/'.format(DELIVERABLE_LOC, table['data_type']), table['table_name'], test=test,
            num_files=num_output_files
        )

        # deobfuscate hvid
        postprocessor.deobfuscate_hvid(DEOBFUSCATION_KEY, nullify_non_integers=True)(
            runner.sqlContext.sql('select * from {}'.format(table['table_name']))
        ).createOrReplaceTempView(table['table_name'])

        # if not test:
        #     normalized_records_unloader.partition_and_rename(
        #         spark, runner, 'emr', table['script_name'], FEED_ID,
        #         table['table_name'], table['date_column'], date_input,
        #         staging_subdir='{}/'.format(table['data_type']),
        #         distribution_key='row_id', provider_partition='part_hvm_vdr_feed_id',
        #         date_partition='part_mth', hvm_historical_date=datetime(
        #             hvm_historical_date.year, hvm_historical_date.month, hvm_historical_date.day
        #         )
        #     )

    if not test and not airflow_test:
        logger.log_run_details(
            provider_name='Cardinal_Rain_Tree',
            data_type=DataType.EMR,
            data_source_transaction_path=input_path,
            data_source_matching_path=matching_path,
            output_path=OUTPUT_PATH_PRODUCTION,
            run_type=RunType.MARKETPLACE,
            input_date=date_input
        )


def main(args):
    # init
    spark, sql_context = init("Cardinal Rain Tree EMR")

    # initialize runner
    runner = Runner(sql_context)

    if args.airflow_test:
        output_path = OUTPUT_PATH_TEST
        deliverable_path = 's3://salusv/testing/dewey/airflow/e2e/cardinal/emr/delivery/{}/'.format(
            args.batch_id
        )
    else:
        output_path = OUTPUT_PATH_PRODUCTION
        deliverable_path = 's3://salusv/deliverable/cardinal_raintree_emr-0/{}/'.format(
            args.batch_id
        )

    run(spark, runner, args.date, num_output_files=args.num_output_files,
        batch_id=args.batch_id, airflow_test=args.airflow_test)

    spark.stop()

    if args.airflow_test:
        normalized_records_unloader.distcp(deliverable_path, DELIVERABLE_LOC)
    else:
        hadoop_time = normalized_records_unloader.timed_distcp(deliverable_path, DELIVERABLE_LOC)
        RunRecorder().record_run_details(additional_time=hadoop_time)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str)
    parser.add_argument('--batch_id', type=str)
    parser.add_argument('--num_output_files', type=int, default=1)
    parser.add_argument('--airflow_test', default=False, action='store_true')
    args = parser.parse_known_args()[0]
    main(args)
