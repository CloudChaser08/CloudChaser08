import argparse
from datetime import date, datetime
from spark.runner import Runner
from spark.spark_setup import init
from spark.common.emr.encounter import schema_v7 as encounter_schema
from spark.common.emr.diagnosis import schema_v7 as diagnosis_schema
from spark.common.emr.procedure import schema_v8 as procedure_schema
from spark.common.emr.provider_order import schema_v7 as provider_order_schema
from spark.common.emr.lab_result import schema_v7 as lab_result_schema
from spark.common.emr.medication import schema_v7 as medication_schema
from spark.common.emr.clinical_observation import schema_v7 as clinical_observation_schema
from spark.common.emr.vital_sign import schema_v7 as vital_sign_schema
import spark.helpers.file_utils as file_utils
import spark.helpers.explode as exploder
import spark.helpers.payload_loader as payload_loader
import spark.helpers.normalized_records_unloader as normalized_records_unloader
import spark.helpers.external_table_loader as external_table_loader
import spark.helpers.schema_enforcer as schema_enforcer
import spark.helpers.postprocessor as postprocessor
import spark.helpers.privacy.common as priv_common
import spark.helpers.privacy.emr.encounter as encounter_priv
import spark.helpers.privacy.emr.diagnosis as diagnosis_priv
import spark.helpers.privacy.emr.procedure as procedure_priv
import spark.helpers.privacy.emr.provider_order as provider_order_priv
import spark.helpers.privacy.emr.lab_result as lab_result_priv
import spark.helpers.privacy.emr.medication as medication_priv
import spark.helpers.privacy.emr.clinical_observation as clinical_observation_priv
import spark.helpers.privacy.emr.vital_sign as vital_sign_priv

from pyspark.sql.window import Window
from pyspark.sql.functions import col, lit, row_number, split, explode

from spark.common.utility.output_type import DataType, RunType
from spark.common.utility.run_recorder import RunRecorder
from spark.common.utility import logger


FEED_ID = '5'
VENDOR_ID = '5'

OUTPUT_PATH = 's3://salusv/warehouse/parquet/emr/2017-08-23/'


def run(spark, runner, date_input, test=False, airflow_test=False):
    script_path = __file__
    max_cap = date_input
    max_cap_obj = datetime.strptime(max_cap, '%Y-%m-%d')

    input_tables = [
        'd_costar',
        'd_cpt',
        'd_drug',
        'd_icd10',
        'd_icd9',
        'd_lab_directory',
        'd_multum_to_ndc',
        'd_patient',
        'd_provider',
        'd_vaccine_cpt',
        'f_diagnosis',
        'f_encounter',
        'f_injection',
        'f_lab',
        'f_medication',
        'f_procedure'
    ]
    input_paths = {}
    if test:
        for t in input_tables:
            input_paths[t] = file_utils.get_abs_path(
                script_path, '../../../test/providers/amazingcharts/emr/resources/input/{}/'.format(t)
            )
        matching_path = file_utils.get_abs_path(
            script_path, '../../../test/providers/amazingcharts/emr/resources/payload/'
        )
    elif airflow_test:
        for t in input_tables:
            input_paths[t] = 's3://salusv/testing/dewey/airflow/e2e/amazingcharts/input/{}/{}/'.format(
                date_input.replace('-', '/'), t
            )
        matching_path = 's3://salusv/testing/dewey/airflow/e2e/amazingcharts/payload/{}/'.format(
            date_input.replace('-', '/')[:-3]
        )
    else:
        for t in input_tables:
            input_paths[t] = 's3://salusv/incoming/emr/amazingcharts/{}/{}/'.format(
                date_input.replace('-', '/')[:-3], t
            )
        input_paths['d_multum_to_ndc'] = 's3://salusv/incoming/emr/amazingcharts/{}/{}/'.format(
            date_input[:-6], 'd_multum_to_ndc'
        )
        matching_path = 's3://salusv/matching/payload/emr/amazingcharts/{}/'.format(
            date_input.replace('-', '/')[:-3] # amazingcharts match payload is delivered to a monthly location, so strip the day off date_input
        )

    if not test:
        external_table_loader.load_ref_gen_ref(runner.sqlContext)

    import spark.providers.amazingcharts.emr.load_transactions as load_transactions
    load_transactions.load(spark, runner, input_paths, date_input, test=test)

    payload_loader.load(runner, matching_path, ['personId'])
    # De-duplicate the payloads so that there is only one
    # personid (we use personid as our join key, not hvJoinKey)
    window = Window.partitionBy(col('personId')).orderBy(col('age'))
    runner.sqlContext.sql('select * from matching_payload')     \
          .withColumn('row_num', row_number().over(window))     \
          .where(col('row_num') == lit(1))                      \
          .drop('row_num')                                      \
          .cache_and_track('matching_payload_deduped')          \
          .createOrReplaceTempView('matching_payload_deduped')

    spark.table('matching_payload_deduped').count()

    exploder.generate_exploder_table(spark, 2, 'proc_2_exploder')
    exploder.generate_exploder_table(spark, 5, 'clin_obsn_exploder')
    exploder.generate_exploder_table(spark, 18, 'vital_sign_exploder')

    normalized_encounter = runner.run_spark_script(
        'normalize_encounter.sql',
        [], return_output=True
    )
    normalized_diagnosis = runner.run_spark_script(
        'normalize_diagnosis.sql',
        [], return_output=True
    )
    normalized_procedure_1 = runner.run_spark_script(
        'normalize_procedure_1.sql',
        [], return_output=True
    )
    normalized_procedure_2 = runner.run_spark_script(
        'normalize_procedure_2.sql',
        [], return_output=True
    )
    normalized_procedure_3 = runner.run_spark_script(
        'normalize_procedure_3.sql',
        [], return_output=True
    )
    normalized_lab_result = runner.run_spark_script(
        'normalize_lab_result.sql',
        [], return_output=True
    )
    normalized_medication = runner.run_spark_script(
        'normalize_medication.sql',
        [], return_output=True
    )
    normalized_clinical_observation = runner.run_spark_script(
        'normalize_clinical_observation.sql',
        [], return_output=True
    )
    normalized_vital_sign = runner.run_spark_script(
        'normalize_vital_sign.sql',
        [], return_output=True
    )

    normalized_procedure_1 = normalized_procedure_1.withColumn('proc_cd', explode(split(col('proc_cd'), '\s+'))) \
            .where("length(proc_cd) != 0").cache()

    join_keys = [
        normalized_procedure_1.proc_dt == normalized_procedure_3.proc_dt,
        normalized_procedure_1.hvid == normalized_procedure_3.hvid,
        normalized_procedure_1.proc_cd == normalized_procedure_3.proc_cd
    ]
    unique_proc_1 = normalized_procedure_1.join(normalized_procedure_3, join_keys, 'left_anti')

    normalized_procedure = schema_enforcer.apply_schema(unique_proc_1, procedure_schema).union(
        schema_enforcer.apply_schema(normalized_procedure_2, procedure_schema).union(
            schema_enforcer.apply_schema(normalized_procedure_3, procedure_schema)))

    normalized_tables = [
        {
            'name': 'medication',
            'data': normalized_medication,
            'privacy': medication_priv,
            'schema': medication_schema,
            'model_version': '07',
            'join_key': 'hv_medctn_id',
            'date_caps': [
                ('medctn_ord_dt', 'EARLIEST_VALID_SERVICE_DATE', max_cap),
                ('medctn_admin_dt', 'EARLIEST_VALID_SERVICE_DATE', max_cap),
                ('medctn_start_dt', 'EARLIEST_VALID_SERVICE_DATE', max_cap),
                ('medctn_end_dt', 'EARLIEST_VALID_SERVICE_DATE', max_cap),
                ('medctn_last_rfll_dt', 'EARLIEST_VALID_SERVICE_DATE', max_cap)
            ],
            'partition_date_col': 'medctn_ord_dt'
        }, {
            'name': 'lab_result',
            'data': normalized_lab_result,
            'privacy': lab_result_priv,
            'schema': lab_result_schema,
            'model_version': '07',
            'join_key': 'hv_lab_result_id',
            'date_caps': [
                ('lab_test_smpl_collctn_dt', 'EARLIEST_VALID_SERVICE_DATE', max_cap),
                ('lab_result_dt', 'EARLIEST_VALID_SERVICE_DATE', max_cap),
                ('data_captr_dt', 'EARLIEST_VALID_SERVICE_DATE', max_cap)
            ],
            'partition_date_col': 'lab_result_dt'
        }, {
            'name': 'encounter',
            'data': normalized_encounter,
            'privacy': encounter_priv,
            'schema': encounter_schema,
            'model_version': '07',
            'join_key': 'hv_enc_id',
            'date_caps': [
                ('enc_start_dt', 'EARLIEST_VALID_SERVICE_DATE', max_cap),
                ('data_captr_dt', 'EARLIEST_VALID_SERVICE_DATE', max_cap)
            ],
            'partition_date_col': 'enc_start_dt'
        }, {
            'name': 'diagnosis',
            'data': normalized_diagnosis,
            'privacy': diagnosis_priv,
            'schema': diagnosis_schema,
            'model_version': '07',
            'join_key': 'hv_diag_id',
            'date_caps': [
                ('diag_dt', 'EARLIEST_VALID_DIAGNOSIS_DATE', max_cap),
                ('diag_onset_dt', 'EARLIEST_VALID_DIAGNOSIS_DATE', max_cap),
                ('diag_resltn_dt', 'EARLIEST_VALID_DIAGNOSIS_DATE', max_cap),
                ('data_captr_dt', 'EARLIEST_VALID_SERVICE_DATE', max_cap)
            ],
            'partition_date_col': 'diag_dt',
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
            'model_version': '08',
            'join_key': 'hv_proc_id',
            'date_caps': [
                ('enc_dt', 'EARLIEST_VALID_SERVICE_DATE', max_cap),
                ('proc_dt', 'EARLIEST_VALID_SERVICE_DATE', max_cap),
                ('data_captr_dt', 'EARLIEST_VALID_SERVICE_DATE', max_cap)
            ],
            'partition_date_col': 'proc_dt'
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
                ('data_captr_dt', 'EARLIEST_VALID_SERVICE_DATE', max_cap)
            ],
            'partition_date_col': 'clin_obsn_dt'
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
            ],
            'partition_date_col': 'vit_sign_dt'
        }
    ]
    for table in normalized_tables:
        normalized_table = postprocessor.compose(
            postprocessor.trimmify, postprocessor.nullify,
            schema_enforcer.apply_schema_func(table['schema']),
            postprocessor.add_universal_columns(
                feed_id=FEED_ID, vendor_id=VENDOR_ID, filename=max_cap_obj.strftime(
                    'AmazingCharts_HV_%b%y'
                ), model_version_number=table['model_version'],

                # rename defaults
                record_id='row_id', created='crt_dt', data_set='data_set_nm',
                data_feed='hvm_vdr_feed_id', data_vendor='hvm_vdr_id',
                model_version='mdl_vrsn_num'
            ),
            table['privacy'].filter(
                runner.sqlContext, update_whitelists=table.get('update_whitelists', lambda x: x),
                additional_transformer=table.get('additional_transformer')
            ),
            *(
                [
                    postprocessor.apply_date_cap(
                        runner.sqlContext, date_col, max_cap_date, FEED_ID, domain_name
                    ) for (date_col, domain_name, max_cap_date) in table['date_caps']
                ] + [
                    schema_enforcer.apply_schema_func(table['schema'])
                ]
            )
        )(table['data'])
        normalized_table.createOrReplaceTempView('normalized_{}'.format(table['name']))

        hvm_historical_date = postprocessor.coalesce_dates(
            runner.sqlContext, FEED_ID, date(1901, 1, 1),
            'HVM_AVAILABLE_HISTORY_START_DATE',
            'EARLIEST_VALID_SERVICE_DATE'
        )

        normalized_records_unloader.unload(
            spark, runner, normalized_table, table['partition_date_col'], max_cap,
            FEED_ID, provider_partition_name='part_hvm_vdr_feed_id',
            date_partition_name='part_mth', hvm_historical_date=datetime(
                hvm_historical_date.year, hvm_historical_date.month, hvm_historical_date.day
            ), staging_subdir=table['name'], test_dir=(file_utils.get_abs_path(
                script_path, '../../../test/providers/amazingcharts/emr/resources/output/'
            ) if test else None), unload_partition_count=20, skip_rename=True,
            distribution_key='row_id'
        )

    if not test and not airflow_test:
        logger.log_run_details(
            provider_name='AmazingCharts',
            data_type=DataType.EMR,
            data_source_transaction_path=','.join(input_paths.values()),
            data_source_matching_path=matching_path,
            output_path=OUTPUT_PATH,
            run_type=RunType.MARKETPLACE,
            input_date=date_input
        )

def main(args):
    spark, sqlContext = init('AmazingCharts EMR Normalization')

    runner = Runner(sqlContext)

    run(spark, runner, args.date, airflow_test=args.airflow_test)

    spark.stop()

    if not args.airflow_test:
        hadoop_time = normalized_records_unloader.timed_distcp(OUTPUT_PATH)
        RunRecorder().record_run_details(additional_time=hadoop_time)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str)
    parser.add_argument('--airflow_test', default=False, action='store_true')
    args = parser.parse_args()
    main(args)

