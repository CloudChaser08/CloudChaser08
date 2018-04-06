#! /usr/bin/python
import argparse
import datetime
from dateutil.relativedelta import relativedelta
import subprocess
import logging

from pyspark.sql.functions import col, rand, concat, when, lit
from pyspark.sql.utils import AnalysisException

from spark.runner import Runner
from spark.spark_setup import init
import spark.providers.allscripts.emr.transaction_schemas as transaction_schemas
from spark.common.emr.encounter import schema_v7 as encounter_schema
from spark.common.emr.diagnosis import schema_v7 as diagnosis_schema
from spark.common.emr.procedure import schema_v7 as procedure_schema
from spark.common.emr.provider_order import schema_v7 as provider_order_schema
from spark.common.emr.lab_order import schema_v6 as lab_order_schema
from spark.common.emr.lab_result import schema_v7 as lab_result_schema
from spark.common.emr.medication import schema_v7 as medication_schema
from spark.common.emr.clinical_observation import schema_v7 as clinical_observation_schema
from spark.common.emr.vital_sign import schema_v7 as vital_sign_schema
import spark.helpers.file_utils as file_utils
import spark.helpers.explode as explode
import spark.helpers.multithreaded_s3_transfer as multi_s3_transfer
import spark.helpers.payload_loader as payload_loader
import spark.helpers.schema_enforcer as schema_enforcer
import spark.helpers.external_table_loader as external_table_loader
import spark.helpers.postprocessor as postprocessor
import spark.helpers.normalized_records_unloader as normalized_records_unloader
import spark.helpers.udf.post_normalization_cleanup as post_norm_cleanup
import spark.helpers.privacy.common as priv_common
import spark.helpers.privacy.emr.encounter as encounter_priv
import spark.helpers.privacy.emr.diagnosis as diagnosis_priv
import spark.helpers.privacy.emr.procedure as procedure_priv
import spark.helpers.privacy.emr.provider_order as provider_order_priv
import spark.helpers.privacy.emr.lab_order as lab_order_priv
import spark.helpers.privacy.emr.lab_result as lab_result_priv
import spark.helpers.privacy.emr.medication as medication_priv
import spark.helpers.privacy.emr.clinical_observation as clinical_observation_priv
import spark.helpers.privacy.emr.vital_sign as vital_sign_priv

script_path = __file__

FEED_ID = '25'
VENDOR_ID = '35'

def run(spark, runner, date_input, models=None, test=False, airflow_test=False):
    date_input = '-'.join(date_input.split('-')[:2])
    date_obj = datetime.date(*[int(el) for el in (date_input + '-01').split('-')])

    matching_date = '2017-09' if date_input <= '2017-09' else date_input

    max_cap = (date_obj + relativedelta(months=1) - relativedelta(days=1)).strftime('%Y-%m-%d')

    if test:
        input_path = file_utils.get_abs_path(
            script_path, '../../../test/providers/allscripts/emr/resources/input/{}/'.format(
                date_input.replace('-', '/')
            )
        ) + '/'
        matching_path = file_utils.get_abs_path(
            script_path, '../../../test/providers/allscripts/emr/resources/matching/{}/'.format(
                matching_date.replace('-', '/')
            )
        ) + '/'
        warehouse_path_template = file_utils.get_abs_path(
            script_path, '../../../test/providers/allscripts/emr/resources/warehouse/{}/part_hvm_vdr_feed_id=25/*'
        )
    elif airflow_test:
        input_path = 's3a://salusv/testing/dewey/airflow/e2e/allscripts/emr/out/{}/'.format(
            date_input.replace('-', '/')
        )
        matching_path = 's3a://salusv/testing/dewey/airflow/e2e/allscripts/emr/payload/{}/'.format(
            matching_date.replace('-', '/')
        )
        warehouse_path_template = 's3a://salusv/testing/dewey/airflow/e2e/allscripts/emr/warehouse/{}/' \
                         'part_hvm_vdr_feed_id=25/*'
    else:
        input_path = 's3a://salusv/incoming/emr/allscripts/{}/'.format(
            date_input.replace('-', '/')
        )
        matching_path = 's3a://salusv/matching/payload/emr/allscripts/{}/'.format(
            matching_date.replace('-', '/')
        )
        warehouse_path_template = 's3a://salusv/warehouse/parquet/emr/2018-03-23/{}/part_hvm_vdr_feed_id=25/*'

    if not test:
        external_table_loader.load_ref_gen_ref(runner.sqlContext)

    explode.generate_exploder_table(spark, 2, 'diag_exploder')
    explode.generate_exploder_table(spark, 2, 'proc_exploder')
    explode.generate_exploder_table(spark, 4, 'medication_exploder')
    explode.generate_exploder_table(spark, 3, 'clin_obsn_exploder')

    for table in transaction_schemas.all_tables:
        if table.name in ['providers', 'patients', 'clients']:
            runner.sqlContext.read.csv(
                input_path + table.name + '/', sep='|', schema=table.schema
            ).createOrReplaceTempView('transactional_' + table.name)
        else:
            # add non-skewed provider columns
            raw_table = runner.sqlContext.read.csv(
                input_path + table.name + '/', sep='|', schema=table.schema
            ).distinct()

            for column in table.skewed_columns:
                raw_table = raw_table.withColumn(
                    'hv_{}'.format(column), when(
                        col(column).isNull(),
                        concat(lit('nojoin_'), rand())
                    ).otherwise(col(column))
                )

            raw_table.createOrReplaceTempView('transactional_' + table.name)

    payload_loader.load(runner, matching_path, extra_cols=['personId', 'claimId'])

    normalized_encounter = runner.run_spark_script(
        'normalize_encounter_app.sql', [
            ['max_cap', max_cap]
        ], return_output=True, source_file_path=script_path
    ).union(
        runner.run_spark_script(
            'normalize_encounter_enc.sql', [
                ['max_cap', max_cap]
            ], return_output=True, source_file_path=script_path
        )
    )
    normalized_diagnosis = runner.run_spark_script(
        'normalize_diagnosis.sql', [
            ['max_cap', max_cap]
        ], return_output=True, source_file_path=script_path
    )
    normalized_procedure = runner.run_spark_script(
        'normalize_procedure_ord.sql', [
            ['max_cap', max_cap]
        ], return_output=True, source_file_path=script_path
    ).union(
        runner.run_spark_script(
            'normalize_procedure_prb.sql', [
                ['max_cap', max_cap]
            ], return_output=True, source_file_path=script_path
        )
    )
    normalized_provider_order = runner.run_spark_script(
        'normalize_provider_order_ord.sql', [
            ['max_cap', max_cap]
        ], return_output=True, source_file_path=script_path
    ).union(
        runner.run_spark_script(
            'normalize_provider_order_vac.sql', [
                ['max_cap', max_cap]
            ], return_output=True, source_file_path=script_path
        )
    )
    normalized_lab_order = runner.run_spark_script(
        'normalize_lab_order.sql', [
            ['max_cap', max_cap]
        ], return_output=True, source_file_path=script_path
    )
    normalized_lab_result = runner.run_spark_script(
        'normalize_lab_result.sql', [
            ['max_cap', max_cap]
        ], return_output=True, source_file_path=script_path
    )
    normalized_medication = runner.run_spark_script(
        'normalize_medication.sql', [
            ['max_cap', max_cap]
        ], return_output=True, source_file_path=script_path
    )
    normalized_clinical_observation = runner.run_spark_script(
        'normalize_clinical_observation.sql', [
            ['max_cap', max_cap]
        ], return_output=True, source_file_path=script_path
    )
    normalized_vital_sign = runner.run_spark_script(
        'normalize_vital_sign.sql', [
            ['max_cap', max_cap]
        ], return_output=True, source_file_path=script_path
    )

    normalized_tables = [
        {
            'name': 'encounter',
            'data': normalized_encounter,
            'privacy': encounter_priv,
            'schema': encounter_schema,
            'model_version': '07',
            'join_key': 'hv_enc_id',
            'date_caps': [
                ('enc_start_dt', 'EARLIEST_VALID_SERVICE_DATE', '9999-12-31'),
                ('enc_end_dt', 'EARLIEST_VALID_SERVICE_DATE', '9999-12-31'),
                ('data_captr_dt', 'EARLIEST_VALID_SERVICE_DATE', max_cap)
            ]
        }, {
            'name': 'diagnosis',
            'data': normalized_diagnosis,
            'privacy': diagnosis_priv,
            'schema': diagnosis_schema,
            'model_version': '07',
            'join_key': 'hv_diag_id',
            'date_caps': [
                ('enc_dt', 'EARLIEST_VALID_SERVICE_DATE', max_cap),
                ('diag_dt', 'EARLIEST_VALID_DIAGNOSIS_DATE', max_cap),
                ('diag_onset_dt', 'EARLIEST_VALID_DIAGNOSIS_DATE', max_cap),
                ('diag_resltn_dt', 'EARLIEST_VALID_DIAGNOSIS_DATE', max_cap),
                ('data_captr_dt', 'EARLIEST_VALID_SERVICE_DATE', max_cap)
            ],
            'update_whitelists': lambda whitelists: whitelists + [{
                'column_name': 'diag_snomed_cd',
                'domain_name': 'SNOMED',
                'whitelist_col_name': 'gen_ref_cd'
            }]
        }, {
            'name': 'procedure',
            'data': normalized_procedure,
            'privacy': procedure_priv,
            'schema': procedure_schema,
            'model_version': '07',
            'join_key': 'hv_proc_id',
            'date_caps': [
                ('enc_dt', 'EARLIEST_VALID_SERVICE_DATE', max_cap),
                ('proc_dt', 'EARLIEST_VALID_SERVICE_DATE', max_cap),
                ('data_captr_dt', 'EARLIEST_VALID_SERVICE_DATE', max_cap)
            ],
            'update_whitelists': lambda whitelists: whitelists + [{
                'column_name': 'proc_snomed_cd',
                'domain_name': 'SNOMED',
                'whitelist_col_name': 'gen_ref_cd'
            }]
        }, {
            'name': 'provider_order',
            'data': normalized_provider_order,
            'privacy': provider_order_priv,
            'schema': provider_order_schema,
            'model_version': '07',
            'join_key': 'hv_prov_ord_id',
            'date_caps': [
                ('enc_dt', 'EARLIEST_VALID_SERVICE_DATE', max_cap),
                ('prov_ord_dt', 'EARLIEST_VALID_SERVICE_DATE', max_cap),
                ('prov_ord_complt_dt', 'EARLIEST_VALID_SERVICE_DATE', max_cap),
                ('data_captr_dt', 'EARLIEST_VALID_SERVICE_DATE', max_cap)
            ]
        }, {
            'name': 'lab_order',
            'data': normalized_lab_order,
            'privacy': lab_order_priv,
            'schema': lab_order_schema,
            'model_version': '06',
            'join_key': 'hv_lab_ord_id',
            'date_caps': [
                ('enc_dt', 'EARLIEST_VALID_SERVICE_DATE', max_cap),
                ('lab_ord_dt', 'EARLIEST_VALID_SERVICE_DATE', max_cap),
                ('data_captr_dt', 'EARLIEST_VALID_SERVICE_DATE', max_cap)
            ],
            'additional_transformer': priv_common.Transformer(
                lab_ord_alt_cd=[
                    priv_common.TransformFunction(post_norm_cleanup.clean_up_procedure_code, ['lab_ord_alt_cd'])
                ]
            )
        }, {
            'name': 'lab_result',
            'data': normalized_lab_result,
            'privacy': lab_result_priv,
            'schema': lab_result_schema,
            'model_version': '07',
            'join_key': 'hv_lab_result_id',
            'date_caps': [
                ('enc_dt', 'EARLIEST_VALID_SERVICE_DATE', max_cap),
                ('lab_test_execd_dt', 'EARLIEST_VALID_SERVICE_DATE', max_cap),
                ('lab_result_dt', 'EARLIEST_VALID_SERVICE_DATE', max_cap),
                ('data_captr_dt', 'EARLIEST_VALID_SERVICE_DATE', max_cap)
            ]
        }, {
            'name': 'medication',
            'data': normalized_medication,
            'privacy': medication_priv,
            'schema': medication_schema,
            'model_version': '07',
            'join_key': 'hv_medctn_id',
            'date_caps': [
                ('enc_dt', 'EARLIEST_VALID_SERVICE_DATE', max_cap),
                ('medctn_admin_dt', 'EARLIEST_VALID_SERVICE_DATE', max_cap),
                ('medctn_start_dt', 'EARLIEST_VALID_SERVICE_DATE', max_cap),
                ('medctn_end_dt', 'EARLIEST_VALID_SERVICE_DATE', max_cap),
                ('data_captr_dt', 'EARLIEST_VALID_SERVICE_DATE', max_cap)
            ]
        }, {
            'name': 'clinical_observation',
            'data': normalized_clinical_observation,
            'privacy': clinical_observation_priv,
            'schema': clinical_observation_schema,
            'model_version': '07',
            'join_key': 'hv_clin_obsn_id',
            'date_caps': [
                ('enc_dt', 'EARLIEST_VALID_SERVICE_DATE', max_cap),
                ('clin_obsn_dt', 'EARLIEST_VALID_SERVICE_DATE', max_cap),
                ('clin_obsn_onset_dt', 'EARLIEST_VALID_DIAGNOSIS_DATE', max_cap),
                ('data_captr_dt', 'EARLIEST_VALID_SERVICE_DATE', max_cap)
            ],
            'update_whitelists': lambda whitelists: whitelists + [{
                'column_name': 'clin_obsn_snomed_cd',
                'domain_name': 'SNOMED',
                'whitelist_col_name': 'gen_ref_cd'
            }]
        }, {
            'name': 'vital_sign',
            'data': normalized_vital_sign,
            'privacy': vital_sign_priv,
            'schema': vital_sign_schema,
            'model_version': '07',
            'join_key': 'hv_vit_sign_id',
            'date_caps': [
                ('enc_dt', 'EARLIEST_VALID_SERVICE_DATE', max_cap),
                ('vit_sign_dt', 'EARLIEST_VALID_SERVICE_DATE', max_cap),
                ('data_captr_dt', 'EARLIEST_VALID_SERVICE_DATE', max_cap)
            ]
        }
    ]

    for table in ([t for t in normalized_tables if t['name'] in models] if models else normalized_tables):
        postprocessor.compose(
            postprocessor.trimmify, postprocessor.nullify,
            schema_enforcer.apply_schema_func(table['schema'], cols_to_keep=['allscripts_date_partition']),
            postprocessor.add_universal_columns(
                feed_id=FEED_ID, vendor_id=VENDOR_ID, filename=date_obj.strftime(
                    'Allscripts_HV_%b%y'
                ), model_version_number=table['model_version'],

                # rename defaults
                record_id='row_id', created='crt_dt', data_set='data_set_nm',
                data_feed='hvm_vdr_feed_id', data_vendor='hvm_vdr_id',
                model_version = 'mdl_vrsn_num'
            ),
            table['privacy'].filter(
                runner.sqlContext, update_whitelists=table.get('update_whitelists', lambda x: x),
                additional_transformer=table.get('additional_transformer')
            ),
            *(
                [
                    postprocessor.apply_date_cap(
                        runner.sqlContext, date_col, max_cap_date, '25', domain_name
                    ) for (date_col, domain_name, max_cap_date) in table['date_caps']
                ] + [
                    schema_enforcer.apply_schema_func(table['schema'], cols_to_keep=['allscripts_date_partition'])
                ]
            )
        )(table['data']).createOrReplaceTempView('normalized_{}'.format(table['name']))

        hvm_historical_date = postprocessor.coalesce_dates(
            runner.sqlContext, FEED_ID, datetime.date(1901, 1, 1),
            'HVM_AVAILABLE_HISTORY_START_DATE',
            'EARLIEST_VALID_SERVICE_DATE'
        )

        # deduplicate
        new_data = runner.sqlContext.sql(
            'select * from normalized_{}'.format(table['name'])
        ).alias('new_data')

        try:
            warehouse_data = runner.sqlContext.read.parquet(
                warehouse_path_template.format(table['name'])
            ).alias('warehouse_data')

            deduplicated_data = warehouse_data.join(
                new_data,
                col(
                    'warehouse_data.{}'.format(table['join_key'])
                ) == col(
                    'new_data.{}'.format(table['join_key'])
                ), 'left'
            ).filter(
                col('new_data.{}'.format(table['join_key'])).isNull()
            ).select(
                *['warehouse_data.{}'.format(column) for column in warehouse_data.columns]
            ).distinct().union(new_data)
        except AnalysisException as e:
            if 'Path does not exist' in str(e):
                # Warehouse data does not exist. This may be the first run.
                logging.warning("No warehouse data found - deduplication will be skipped.")
                deduplicated_data = new_data
            else:
                raise

        normalized_records_unloader.unload(
            spark, runner, deduplicated_data, 'allscripts_date_partition', max_cap,
            FEED_ID, provider_partition_name='part_hvm_vdr_feed_id',
            date_partition_name='part_mth', hvm_historical_date=datetime.datetime(
                hvm_historical_date.year, hvm_historical_date.month, hvm_historical_date.day
            ), staging_subdir=table['name'], test_dir=(file_utils.get_abs_path(
                script_path, '../../../test/providers/allscripts/emr/resources/output/'
            ) if test else None), unload_partition_count=100, skip_rename=True,
            distribution_key='row_id'
        )


def main(args):
    # init
    spark, sqlContext = init("Allscripts EMR")

    # initialize runner
    runner = Runner(sqlContext)

    models = args.models.split(',') if args.models else [
        'encounter', 'diagnosis', 'procedure', 'provider_order', 'lab_order', 'lab_result',
        'medication', 'clinical_observation', 'vital_sign'
    ]

    run(spark, runner, args.date, models=models, airflow_test=args.airflow_test)

    spark.stop()

    if args.airflow_test:
        output_path = 's3://salusv/testing/dewey/airflow/e2e/allscripts/emr/spark-output/'
    else:
        output_path = 's3://salusv/warehouse/parquet/emr/2018-03-23/'

    # backup allscripts normalized data before distcp
    try:
        subprocess.check_call(['aws', 's3', 'ls', 's3://salusv/warehouse/parquet/emr/2018-03-23/{}/part_hvm_vdr_feed_id=25/'.format(
            models[0]
        )])
        files_exist = True
    except subprocess.CalledProcessError as e:
        if str(e).endswith('status 1'):
            files_exist = False
        else:
            raise

    if files_exist:
        for model in models:
            subprocess.check_call([
                'aws', 's3', 'rm', '--recursive', 's3://salusv/backup/allscripts_emr/{}/{}/'.format(args.date, model)
            ])
            multi_s3_transfer.multithreaded_copy(
                's3://salusv/warehouse/parquet/emr/2018-03-23/{}/part_hvm_vdr_feed_id=25/'.format(model),
                's3://salusv/backup/allscripts_emr/{1}/{0}/'.format(model, args.date)
            )
            subprocess.check_call([
                'aws', 's3', 'rm', '--recursive',
                's3://salusv/warehouse/parquet/emr/2018-03-23/{}/part_hvm_vdr_feed_id=25/'.format(model)
            ])

    normalized_records_unloader.distcp(output_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str)
    parser.add_argument('--airflow_test', default=False, action='store_true')
    parser.add_argument('--models', type=str, default=None)
    args = parser.parse_args()
    main(args)
