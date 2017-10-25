import argparse
from spark.runner import Runner
from spark.spark_setup import init
import spark.helpers.constants as constants
import spark.helpers.payload_loader as payload_loader
import spark.helpers.postprocessor as postprocessor
import spark.helpers.normalized_records_unloader as normalized_records_unloader


def run(spark, runner, date_input, airflow_test=False):

    script_path = __file__

    if airflow_test:
        input_path = 's3://salusv/testing/dewey/airflow/e2e/amazingcharts/emr/out/{}/'.format(
            date_input.replace('-', '/')
        )
        multum_to_ndc_path = 's3://salusv/testing/dewey/airflow/e2e/amazingcharts/emr/multum/{}/'.format(
            date_input.replace('-', '/')
        )
        matching_path = 's3://salusv/testing/dewey/airflow/e2e/amazingcharts/emr/matching/{}/'.format(
            date_input.replace('-', '/')
        )
    else:
        input_path = 's3a://salusv/incoming/emr/amazingcharts/{}/'.format(date_input.replace('-', '/'))
        multum_to_ndc_path = 's3a://salusv/incoming/emr/amazingcharts/{}/d_multum_to_ndc/'.format(date_input.split('-')[0])
        matching_path = 's3a://salusv/matching/payload/emr/amazingcharts/{}/'.format(date_input.replace('-', '/'))

    runner.run_spark_script('amazingcharts_skinny_model.sql', [
        ['table', 'normalized_data', False],
        ['properties', '', False]
    ], script_path)

    payload_loader.load(runner, matching_path, ['personId'])

    if date_input <= '2016-09-01':
        runner.run_spark_script('../../../common/load_hvid_parent_child_map.sql', [], script_path)
        runner.run_spark_script('fix_matching_payload.sql', [], script_path)

    runner.run_spark_script('load_transactions.sql', [
        ['d_multum_to_ndc_path', multum_to_ndc_path, False],
        ['input_path', input_path, False]
    ], script_path)

    transaction_tables = [
        'f_diagnosis', 'f_encounter', 'f_medication', 'f_procedure', 'f_lab', 'd_drug', 'd_cpt', 'd_multum_to_ndc'
    ]

    # trim and nullify all incoming transactions tables
    for table in transaction_tables:
        postprocessor.compose(
            postprocessor.trimmify,
            lambda df: postprocessor.nullify(df, null_vals=['', 'NULL'])
        )(runner.sqlContext.sql('select * from {}'.format(table))).createOrReplaceTempView(table)

    runner.run_spark_script('normalize.sql', [], script_path)

    runner.sqlContext.sql('select * from normalized_data').repartition(50).write.parquet(
        constants.hdfs_staging_dir + date_input.replace('-', '/')
    )


def main(args):
    # init
    spark, sqlContext = init("AmazingCharts")

    # initialize runner
    runner = Runner(sqlContext)

    run(spark, runner, args.date, airflow_test=args.airflow_test)

    spark.stop()

    if args.airflow_test:
        output_path = 's3://salusv/testing/dewey/airflow/e2e/amazingcharts/emr/spark-output/'
    else:
        output_path = 's3://salusv/warehouse/parquet/emr/amazingcharts/'

    normalized_records_unloader.distcp(output_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str)
    parser.add_argument('--airflow_test', default=False, action='store_true')
    args = parser.parse_args()
    main(args)
