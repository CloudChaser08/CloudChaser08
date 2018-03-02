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
import spark.helpers.normalized_records_unloader as normalized_records_unloader
from spark.helpers.privacy.common import update_whitelist
from spark.helpers.privacy.common import Transformer, TransformFunction
from spark.helpers.privacy.emr import                   \
    encounter as priv_encounter,                        \
    clinical_observation as priv_clinical_observation,  \
    procedure as priv_procedure,                        \
    lab_result as priv_lab_result,                      \
    diagnosis as priv_diagnosis,                        \
    medication as priv_medication

# staging for deliverable
DELIVERABLE_LOC = 'hdfs:///deliverable/'


def run(spark, runner, date_input, test=False, airflow_test=False):
    global DELIVERABLE_LOC

    date_obj = datetime.strptime(date_input, '%Y-%m-%d')

    setid = 'EMR.{}.zip'.format(date_obj.strftime('%m%d%Y'))

    script_path = __file__

    if test:
        demographics_input_path = file_utils.get_abs_path(
            script_path, '../../../test/providers/cardinal/emr/resources/input/demographics/'
        ) + '/'
        diagnosis_input_path = file_utils.get_abs_path(
            script_path, '../../../test/providers/cardinal/emr/resources/input/diagnosis/'
        ) + '/'
        encounter_input_path = file_utils.get_abs_path(
            script_path, '../../../test/providers/cardinal/emr/resources/input/encounter/'
        ) + '/'
        lab_input_path = file_utils.get_abs_path(
            script_path, '../../../test/providers/cardinal/emr/resources/input/lab/'
        ) + '/'
        dispense_input_path = file_utils.get_abs_path(
            script_path, '../../../test/providers/cardinal/emr/resources/input/dispense/'
        ) + '/'
        matching_path = file_utils.get_abs_path(
            script_path, '../../../test/providers/cardinal/emr/resources/matching/'
        ) + '/'
        DELIVERABLE_LOC = file_utils.get_abs_path(
            script_path, '../../../test/providers/cardinal/emr/resources/delivery/'
        ) + '/'
    elif airflow_test:
        demographics_input_path = 's3://salusv/testing/dewey/airflow/e2e/cardinal/emr/out/{}/demographics/'.format(
            date_input.replace('-', '/')
        )
        diagnosis_input_path = 's3://salusv/testing/dewey/airflow/e2e/cardinal/emr/out/{}/diagnosis/'.format(
            date_input.replace('-', '/')
        )
        encounter_input_path = 's3://salusv/testing/dewey/airflow/e2e/cardinal/emr/out/{}/encounter/'.format(
            date_input.replace('-', '/')
        )
        lab_input_path = 's3://salusv/testing/dewey/airflow/e2e/cardinal/emr/out/{}/lab/'.format(
            date_input.replace('-', '/')
        )
        dispense_input_path = 's3://salusv/testing/dewey/airflow/e2e/cardinal/emr/out/{}/dispense/'.format(
            date_input.replace('-', '/')
        )
        matching_path = 's3://salusv/testing/dewey/airflow/e2e/cardinal/emr/matching/{}/'.format(
            date_input.replace('-', '/')
        )
    else:
        demographics_input_path = 's3a://salusv/incoming/emr/cardinal/{}/demographics/'.format(
            date_input.replace('-', '/')
        )
        diagnosis_input_path = 's3a://salusv/incoming/emr/cardinal/{}/diagnosis/'.format(
            date_input.replace('-', '/')
        )
        encounter_input_path = 's3a://salusv/incoming/emr/cardinal/{}/encounter/'.format(
            date_input.replace('-', '/')
        )
        lab_input_path = 's3a://salusv/incoming/emr/cardinal/{}/lab/'.format(
            date_input.replace('-', '/')
        )
        dispense_input_path = 's3a://salusv/incoming/emr/cardinal/{}/dispense/'.format(
            date_input.replace('-', '/')
        )
        matching_path = 's3a://salusv/matching/payload/emr/cardinal/{}/'.format(
            date_input.replace('-', '/')
        )

    runner.run_spark_script('../../../common/emr/clinical_observation_common_model_v4.sql', [
        ['table_name', 'clinical_observation_common_model', False],
        ['properties', '', False],
        ['additional_columns', '', False]
    ])
    runner.run_spark_script('../../../common/emr/diagnosis_common_model_v5.sql', [
        ['table_name', 'diagnosis_common_model', False],
        ['properties', '', False],
        ['additional_columns', '', False]
    ])
    runner.run_spark_script('../../../common/emr/encounter_common_model_v4.sql', [
        ['table_name', 'encounter_common_model', False],
        ['properties', '', False],
        ['additional_columns', '', False]
    ])
    runner.run_spark_script('../../../common/emr/lab_result_common_model_v4.sql', [
        ['table_name', 'lab_result_common_model', False],
        ['properties', '', False],
        ['additional_columns', '', False]
    ])
    runner.run_spark_script('../../../common/emr/medication_common_model_v4.sql', [
        ['table_name', 'medication_common_model', False],
        ['properties', '', False],
        ['additional_columns', '', False]
    ])
    runner.run_spark_script('../../../common/emr/procedure_common_model_v4.sql', [
        ['table_name', 'procedure_common_model', False],
        ['properties', '', False],
        ['additional_columns', '', False]
    ])

    explode.generate_exploder_table(spark, 6, 'clin_obs_exploder')

    if not test:
        external_table_loader.load_ref_gen_ref(runner.sqlContext)

    hvm_available_history_date = postprocessor.get_gen_ref_date(runner.sqlContext, "40", "HVM_AVAILABLE_HISTORY_DATE")
    earliest_valid_service_date = postprocessor.get_gen_ref_date(runner.sqlContext, "40", "EARLIEST_VALID_SERVICE_DATE")
    hvm_historical_date = hvm_available_history_date if hvm_available_history_date else \
        earliest_valid_service_date if earliest_valid_service_date else date(1901, 1, 1)
    max_date = date_input

    payload_loader.load(runner, matching_path, ['hvJoinKey', 'claimId'])

    if date_input <= '2017-12-27':
        runner.run_spark_script('load_transactions.sql', [
            ['demographics_input_path', demographics_input_path],
            ['diagnosis_input_path', diagnosis_input_path],
            ['encounter_input_path', encounter_input_path],
            ['lab_input_path', lab_input_path],
            ['dispense_input_path', dispense_input_path]
        ])
    else:
        runner.run_spark_script('load_transactions_v2.sql', [
            ['demographics_input_path', demographics_input_path],
            ['diagnosis_input_path', diagnosis_input_path],
            ['encounter_input_path', encounter_input_path],
            ['lab_input_path', lab_input_path],
            ['dispense_input_path', dispense_input_path]
        ])

    transaction_tables = [
        'demographics_transactions', 'diagnosis_transactions', 'encounter_transactions',
        'lab_transactions', 'dispense_transactions', 'matching_payload'
    ]

    # trim and nullify all incoming transactions tables
    for table in transaction_tables:
        postprocessor.compose(
            postprocessor.trimmify, lambda df: postprocessor.nullify(
                df,
                null_vals=['','NULL'],
                preprocess_func=lambda c: c.upper() if c else c
            )
        )(runner.sqlContext.sql('select * from {}'.format(table))).createOrReplaceTempView(table)

    runner.run_spark_script('normalize_encounter.sql')
    runner.run_spark_script('normalize_diagnosis.sql')
    runner.run_spark_script('normalize_procedure_enc.sql')
    runner.run_spark_script('normalize_procedure_disp.sql')
    runner.run_spark_script('normalize_lab_result.sql')
    runner.run_spark_script('normalize_medication.sql')
    runner.run_spark_script('normalize_clinical_observation.sql')

    normalized_tables = [
        {
            'table_name': 'clinical_observation_common_model',
            'script_name': 'emr/clinical_observation_common_model_v2.sql',
            'data_type': 'clinical_observation',
            'date_column': 'clin_obsn_dt',
            'privacy_filter': priv_clinical_observation,
            'custom_privacy_transformer': Transformer(
                clin_obsn_diag_cd=[
                    TransformFunction(post_norm_cleanup.clean_up_diagnosis_code, ['clin_obsn_diag_cd', 'clin_obsn_diag_cd_qual', 'clin_obsn_dt'])
                ]
            ),
            'date_caps': [
                ('clin_obsn_dt', 'EARLIEST_VALID_SERVICE_DATE'),
                ('clin_obsn_resltn_dt', 'EARLIEST_VALID_SERVICE_DATE')
            ]
        },
        {
            'table_name': 'diagnosis_common_model',
            'script_name': 'emr/diagnosis_common_model_v3.sql',
            'data_type': 'diagnosis',
            'date_column': 'diag_dt',
            'privacy_filter': priv_diagnosis,
            'date_caps': [
                ('diag_dt', 'EARLIEST_VALID_DIAGNOSIS_DATE'),
                ('diag_resltn_dt', 'EARLIEST_VALID_DIAGNOSIS_DATE')
            ]
        },
        {
            'table_name': 'encounter_common_model',
            'script_name': 'emr/encounter_common_model_v2.sql',
            'data_type': 'encounter',
            'date_column': 'enc_start_dt',
            'privacy_filter': priv_encounter,
            'date_caps': [
                ('enc_start_dt', 'EARLIEST_VALID_SERVICE_DATE'),
                ('enc_end_dt', 'EARLIEST_VALID_SERVICE_DATE')
            ]
        },
        {
            'table_name': 'medication_common_model',
            'script_name': 'emr/medication_common_model_v2.sql',
            'data_type': 'medication',
            'date_column': 'medctn_admin_dt',
            'privacy_filter': priv_medication,
            'date_caps': [
                ('medctn_admin_dt', 'EARLIEST_VALID_SERVICE_DATE'),
                ('medctn_end_dt', 'EARLIEST_VALID_SERVICE_DATE')
            ]
        },
        {
            'table_name': 'procedure_common_model',
            'script_name': 'emr/procedure_common_model_v2.sql',
            'data_type': 'procedure',
            'date_column': 'proc_dt',
            'privacy_filter': priv_procedure,
            'date_caps': [
                ('enc_dt', 'EARLIEST_VALID_SERVICE_DATE'),
                ('proc_dt', 'EARLIEST_VALID_SERVICE_DATE')
            ]
        },
        {
            'table_name': 'lab_result_common_model',
            'script_name': 'emr/lab_result_common_model_v2.sql',
            'data_type': 'lab_result',
            'date_column': 'lab_test_execd_dt',
            'privacy_filter': priv_lab_result,
            'custom_privacy_transformer': Transformer(
                lab_test_diag_cd=[
                    TransformFunction(post_norm_cleanup.clean_up_diagnosis_code, ['lab_test_diag_cd', 'lab_test_diag_cd_qual', 'lab_test_execd_dt'])
                ]
            ),
            'custom_whitelist_additions': lambda whitelists: update_whitelist(
                whitelists, 'lab_test_nm', 'clean_up_freetext', False
            ),
            'date_caps': [
                ('lab_test_execd_dt', 'EARLIEST_VALID_SERVICE_DATE')
            ]
        }
    ]

    for table in normalized_tables:
        postprocessor.compose(
            postprocessor.add_universal_columns(
                feed_id='40', vendor_id='42', filename=setid,

                # rename defaults
                record_id='row_id', created='crt_dt', data_set='data_set_nm',
                data_feed='hvm_vdr_feed_id', data_vendor='hvm_vdr_id',
                model_version = 'mdl_vrsn_num'
            ),
            table['privacy_filter'].filter(
                runner.sqlContext, additional_transformer=table.get('custom_privacy_transformer'),
                update_whitelists=table.get('custom_whitelist_additions', lambda x: x)
            ),
            *[
                postprocessor.apply_date_cap(runner.sqlContext, date_col, max_date, '40', domain_name)
                for (date_col, domain_name) in table['date_caps']
            ]
        )(
            runner.sqlContext.sql('select * from {}'.format(table['table_name']))
        ).createOrReplaceTempView(table['table_name'])

        # unload delivery file for cardinal
        normalized_records_unloader.unload_delimited_file(
            spark, runner, '{}{}/'.format(DELIVERABLE_LOC, table['data_type']), table['table_name'], test=test
        )

        # deobfuscate hvid
        postprocessor.deobfuscate_hvid('Cardinal_MPI-0', nullify_non_integers=True)(
            runner.sqlContext.sql('select * from {}'.format(table['table_name']))
        ).createOrReplaceTempView(table['table_name'])

        if not test:
            normalized_records_unloader.partition_and_rename(
                spark, runner, 'emr', table['script_name'], '40',
                table['table_name'], table['date_column'], date_input,
                staging_subdir='{}/'.format(table['data_type']),
                distribution_key='row_id', provider_partition='part_hvm_vdr_feed_id',
                date_partition='part_mth', hvm_historical_date=datetime(
                    hvm_historical_date.year, hvm_historical_date.month, hvm_historical_date.day
                )
            )


def main(args):
    # init
    spark, sqlContext = init("Cardinal Rain Tree EMR")

    # initialize runner
    runner = Runner(sqlContext)

    run(spark, runner, args.date, airflow_test=args.airflow_test)

    spark.stop()

    if args.airflow_test:
        output_path = 's3://salusv/testing/dewey/airflow/e2e/cardinal/emr/spark-output/'
        deliverable_path = 's3://salusv/testing/dewey/airflow/e2e/cardinal/emr/delivery/{}/'.format(
            args.date.replace('-', '/')
        )
    else:
        output_path = 's3://salusv/warehouse/parquet/emr/2017-08-09/'
        deliverable_path = 's3://salusv/deliverable/cardinal_raintree_emr-0/{}/'.format(
            args.date.replace('-', '/')
        )

    normalized_records_unloader.distcp(output_path)
    normalized_records_unloader.distcp(deliverable_path, DELIVERABLE_LOC)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str)
    parser.add_argument('--airflow_test', default=False, action='store_true')
    args = parser.parse_args()
    main(args)
