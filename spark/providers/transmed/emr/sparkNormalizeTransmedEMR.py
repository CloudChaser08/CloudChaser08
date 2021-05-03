import argparse
from spark.runner import Runner
from spark.spark_setup import init
import spark.providers.transmed.emr.transaction_schemas as transaction_schemas
import spark.helpers.file_utils as file_utils
import spark.helpers.explode as explode
import spark.helpers.payload_loader as payload_loader
import spark.helpers.external_table_loader as external_table_loader
import spark.helpers.postprocessor as postprocessor
import spark.helpers.udf.post_normalization_cleanup as post_norm_cleanup
import spark.helpers.normalized_records_unloader as normalized_records_unloader
from spark.helpers.privacy.common import Transformer, TransformFunction
from spark.helpers.privacy.emr import                   \
    clinical_observation as priv_clinical_observation,  \
    procedure as priv_procedure,                        \
    lab_result as priv_lab_result,                      \
    diagnosis as priv_diagnosis

from spark.common.utility.output_type import DataType, RunType
from spark.common.utility.run_recorder import RunRecorder
from spark.common.utility import logger


OUTPUT_PATH_TEST = 's3://salusv/testing/dewey/airflow/e2e/transmed/emr/spark-output/'
OUTPUT_PATH_PRODUCTION = 's3://salusv/warehouse/parquet/emr/2017-08-23/'


def run(spark, runner, date_input, test=False, airflow_test=False):

    FEED_ID = '54'
    VENDOR_ID = '152'

    script_path = __file__

    if test:
        cancerepisode_input_path = file_utils.get_abs_path(
            script_path, '../../../test/providers/transmed/emr/resources/input/cancerepisode/'
        ) + '/'
        treatmentsite_input_path = file_utils.get_abs_path(
            script_path, '../../../test/providers/transmed/emr/resources/input/treatmentsite/'
        ) + '/'
        matching_path = file_utils.get_abs_path(
            script_path, '../../../test/providers/transmed/emr/resources/matching/'
        ) + '/'
    elif airflow_test:
        cancerepisode_input_path = 's3://salusv/testing/dewey/airflow/e2e/transmed/emr/out/{}/cancerepisode/'.format(
            date_input.replace('-', '/')
        )
        treatmentsite_input_path = 's3://salusv/testing/dewey/airflow/e2e/transmed/emr/out/{}/treatmentsite/'.format(
            date_input.replace('-', '/')
        )
        matching_path = 's3://salusv/testing/dewey/airflow/e2e/transmed/emr/payload/{}/'.format(
            date_input.replace('-', '/')
        )
    else:
        cancerepisode_input_path = 's3a://salusv/incoming/emr/transmed/{}/cancerepisode/'.format(
            date_input.replace('-', '/')
        )
        treatmentsite_input_path = 's3a://salusv/incoming/emr/transmed/{}/treatmentsite/'.format(
            date_input.replace('-', '/')
        )
        matching_path = 's3a://salusv/matching/payload/emr/transmed/{}/'.format(
            date_input.replace('-', '/')
        )

    if not test:
        external_table_loader.load_ref_gen_ref(runner.sqlContext)

    payload_loader.load(runner, matching_path, ['claimId'])

    def create_and_preprocess_transaction_table(table_name, table_input_path, table_schema):
        postprocessor.compose(
            postprocessor.add_input_filename(
                'source_file_name', persisted_df_id='{}_with_input_filename'.format(table_name)),
            postprocessor.trimmify, postprocessor.nullify
        )(runner.sqlContext.read.csv(
            table_input_path, schema=table_schema, sep='\t'
        )).createOrReplaceTempView(table_name)

    create_and_preprocess_transaction_table(
        'transactions_cancerepisode', cancerepisode_input_path, transaction_schemas.cancerepisode
    )
    create_and_preprocess_transaction_table(
        'transactions_treatmentsite', treatmentsite_input_path, transaction_schemas.treatmentsite
    )

    # create a UDF that can be used to parse out the diagnosis from
    # their primarysite column
    def parse_out_transmed_diagnosis(diag):
        return diag.split('-')[0].strip()

    runner.sqlContext.registerFunction(
        'parse_out_transmed_diagnosis', parse_out_transmed_diagnosis
    )

    runner.run_spark_script('normalize_diagnosis.sql', [
        ['date_input', date_input]
    ], return_output=True).createOrReplaceTempView(
        'normalized_diagnosis'
    )
    runner.run_spark_script('normalize_procedure.sql', [
        ['date_input', date_input]
    ], return_output=True).createOrReplaceTempView(
        'normalized_procedure'
    )

    max_breast_cancer_types = runner.sqlContext.sql(
        "SELECT MAX(SIZE(SPLIT(breastcancertype, ','))) as max_length FROM transactions_cancerepisode"
    ).collect()[0].max_length

    explode.generate_exploder_table(
        spark, max_breast_cancer_types if max_breast_cancer_types else 1, 'breast_cancer_type_exploder'
    )

    runner.run_spark_script('normalize_lab_result.sql', [
        ['date_input', date_input]
    ], return_output=True).createOrReplaceTempView(
        'normalized_lab_result'
    )

    explode.generate_exploder_table(spark, 3, 'result_cd_exploder')

    runner.run_spark_script('normalize_clinical_observation.sql', [
        ['date_input', date_input]
    ], return_output=True).createOrReplaceTempView(
        'normalized_clinical_observation'
    )

    normalized_tables = [
        {
            'table_name': 'normalized_clinical_observation',
            'script_name': 'emr/clinical_observation_common_model_v4.sql',
            'data_type': 'clinical_observation',
            'privacy_filter': priv_clinical_observation,
            'model_version': '04',
            'custom_transformer': Transformer(
                clin_obsn_diag_cd=[
                    TransformFunction(post_norm_cleanup.clean_up_diagnosis_code,
                                      ['clin_obsn_diag_cd', 'clin_obsn_diag_cd_qual', 'date_input'])
                ]
            )
        },
        {
            'table_name': 'normalized_diagnosis',
            'script_name': 'emr/diagnosis_common_model_v5.sql',
            'data_type': 'diagnosis',
            'privacy_filter': priv_diagnosis,
            'model_version': '05',
            'custom_transformer': Transformer(
                diag_cd=[
                    TransformFunction(post_norm_cleanup.clean_up_diagnosis_code,
                                      ['diag_cd', 'diag_cd_qual', 'date_input'])
                ]
            )
        },
        {
            'table_name': 'normalized_procedure',
            'script_name': 'emr/procedure_common_model_v4.sql',
            'data_type': 'procedure',
            'privacy_filter': priv_procedure,
            'model_version': '04',
            'custom_transformer': Transformer(
                proc_diag_cd=[
                    TransformFunction(post_norm_cleanup.clean_up_diagnosis_code,
                                      ['proc_diag_cd', 'proc_diag_cd_qual', 'date_input'])
                ],
                proc_cd=[
                    TransformFunction(lambda cd: cd, ['proc_cd'])
                ]
            )
        },
        {
            'table_name': 'normalized_lab_result',
            'script_name': 'emr/lab_result_common_model_v4.sql',
            'data_type': 'lab_result',
            'privacy_filter': priv_lab_result,
            'model_version': '04',
            'custom_transformer': Transformer(
                lab_test_diag_cd=[
                    TransformFunction(post_norm_cleanup.clean_up_diagnosis_code,
                                      ['lab_test_diag_cd', 'lab_test_diag_cd_qual', 'date_input'])
                ]
            )
        }
    ]

    for table in normalized_tables:
        postprocessor.compose(
            postprocessor.add_universal_columns(
                feed_id=FEED_ID, vendor_id=VENDOR_ID, filename=None,
                model_version_number=table['model_version'],

                # rename defaults
                record_id='row_id', created='crt_dt', data_set='data_set_nm',
                data_feed='hvm_vdr_feed_id', data_vendor='hvm_vdr_id',
                model_version='mdl_vrsn_num'
            ),
            table['privacy_filter'].filter(
                runner.sqlContext, additional_transformer=table['custom_transformer']
            )
        )(
            runner.sqlContext.sql('select * from {}'.format(table['table_name']))
        ).createOrReplaceTempView(table['table_name'])

        common_model_columns = [
            column.name for column in runner.sqlContext.sql(
                'SELECT * FROM {}'.format(table['table_name'])).schema.fields
            if column.name not in ['date_input', 'part_mth']
        ]

        if not test:
            normalized_records_unloader.partition_and_rename(
                spark, runner, 'emr', table['script_name'], FEED_ID,
                table['table_name'], 'date_input', date_input,
                staging_subdir='{}/'.format(table['data_type']),
                distribution_key='row_id', provider_partition='part_hvm_vdr_feed_id',
                date_partition='part_mth', columns=common_model_columns
            )
            for table_up in ['transactions_cancerepisode', 'transactions_treatmentsite']:
                runner.unpersist('{}_with_input_filename'.format(table_up))

    if not test and not airflow_test:
        logger.log_run_details(
            provider_name='Transmed',
            data_type=DataType.EMR,
            data_source_transaction_path="",
            data_source_matching_path=matching_path,
            output_path=OUTPUT_PATH_PRODUCTION,
            run_type=RunType.MARKETPLACE,
            input_date=date_input
        )


def main(args):
    # init
    spark, sql_context = init("Transmed EMR")

    # initialize runner
    runner = Runner(sql_context)

    run(spark, runner, args.date, airflow_test=args.airflow_test)

    spark.stop()

    if args.airflow_test:
        normalized_records_unloader.distcp(OUTPUT_PATH_TEST)
    else:
        hadoop_time = normalized_records_unloader.timed_distcp(OUTPUT_PATH_PRODUCTION)
        RunRecorder().record_run_details(additional_time=hadoop_time)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str)
    parser.add_argument('--airflow_test', default=False, action='store_true')
    args = parser.parse_known_args()
    main(args)
