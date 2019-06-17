import argparse
import os
from spark.runner import Runner
from spark.spark_setup import init
from spark.common.emr.encounter import schema_v8 as encounter_schema
from spark.common.emr.diagnosis import schema_v8 as diagnosis_schema
from spark.common.emr.procedure import schema_v10 as procedure_schema
from spark.common.emr.lab_test import schema_v1 as lab_test_schema
from spark.common.emr.medication import schema_v9 as medication_schema
from spark.common.emr.clinical_observation import schema_v9 as clinical_observation_schema
import spark.helpers.file_utils as file_utils
import spark.helpers.payload_loader as payload_loader
import spark.helpers.records_loader as records_loader
import spark.helpers.normalized_records_unloader as normalized_records_unloader
import spark.helpers.external_table_loader as external_table_loader
import spark.helpers.schema_enforcer as schema_enforcer
import spark.providers.practice_fusion.emr.records_schemas as records_schemas

FEED_ID = '136'

MODEL_SCHEMA = {
    'clinical_observation' : clinical_observation_schema,
    'diagnosis' : diagnosis_schema,
    'encounter' : encounter_schema,
    'lab_test' : lab_test_schema,
    'medication' : medication_schema,
    'procedure' : procedure_schema
}

MODELS = ['encounter', 'clinical_observation', 'diagnosis', 'lab_test',
          'medication', 'procedure']

def run(spark, runner, date_input, model=None, custom_input_path=None, custom_matching_path=None,
        test=False, end_to_end_test=False):

    script_path = __file__

    if test:
        input_path = file_utils.get_abs_path(
            script_path, '../../../test/providers/practice_fusion/emr/resources/input/'
        ) + '/'
        matching_path = file_utils.get_abs_path(
            script_path, '../../../test/providers/practice_fusion/emr/resources/matching/'
        ) + '/'
    elif end_to_end_test:
        input_path = 's3://salusv/testing/dewey/airflow/e2e/practice_fusion/out/{}/'.format(
            date_input.replace('-', '/')
        )
        matching_path = 's3://salusv/testing/dewey/airflow/e2e/practice_fusion/payload/{}/'.format(
            date_input.replace('-', '/')
        )
    else:
        input_path = 's3://salusv/incoming/emr/practice_fusion/{}/'.format(
            date_input.replace('-', '/')
        )
        matching_path = 's3://salusv/matching/payload/emr/practice_fusion/{}/'.format(
            date_input.replace('-', '/')
        )

    if custom_input_path:
        input_path = custom_input_path

    if custom_matching_path:
        matching_path = custom_matching_path

    if not test:
        external_table_loader.load_ref_gen_ref(runner.sqlContext)
    else:
        pass

    records_loader.load_and_clean_all_v2(runner, input_path, records_schemas, load_file_name=True)
    payload_loader.load(runner, matching_path, ['claimId', 'patientId', 'hvJoinKey'], table_name='payload')

    models = [model] if model else MODELS
    for mdl in models:
        normalized_output = runner.run_all_spark_scripts([
                ['VDR_FILE_DT', date_input, False],
            ], directory_path=os.path.dirname(script_path) + '/' + mdl
        )

        df = schema_enforcer.apply_schema(normalized_output, MODEL_SCHEMA[mdl],
                                        columns_to_keep=['part_hvm_vdr_feed_id', 'part_mth'])

        if not test:
            _columns = df.columns
            _columns.remove('part_hvm_vdr_feed_id')
            _columns.remove('part_mth')

            normalized_records_unloader.unload(
                spark, runner, df, 'part_mth', date_input, FEED_ID,
                provider_partition_name='part_hvm_vdr_feed_id',
                date_partition_name='part_mth', columns=_columns,
                staging_subdir=mdl, unload_partition_count=100,
                distribution_key='row_id'
            )

        else:
            df.collect()


def main(args):
    models = MODELS

    if args.models:
        models = args.models.split(',')

    for model in models:
        spark, sqlContext = init('Practice Fusion {} Normalization'.format(model.title()))

        runner = Runner(sqlContext)

        run(spark, runner, args.date, model, custom_input_path=args.input_path,
            custom_matching_path=args.matching_path, end_to_end_test=args.end_to_end_test)

        spark.stop()

        if args.end_to_end_test:
            output_path = 's3://salusv/testing/dewey/airflow/e2e/practice_fusion/spark-output-3/'
        else:
            output_path = 's3://salusv/opp_1186_warehouse/parquet/emr/2019-04-17/'

        if args.ouptut_path:
            output_path = args.output_path

        normalized_records_unloader.distcp(output_path)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str)
    parser.add_argument('--end_to_end_test', default=False, action='store_true')
    parser.add_argument('--input_path', help='Overwrite default input path with this value')
    parser.add_argument('--matching_path', help='Overwrite default matching path with this value')
    parser.add_argument('--ouptut_path', help='Overwrite default output path with this value')
    parser.add_argument('--models', help='Comma-separated list of models to normalize instead of all models')
    args = parser.parse_args()
    main(args)
