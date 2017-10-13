import argparse
from spark.runner import Runner
from spark.spark_setup import init
import spark.helpers.payload_loader as payload_loader
import spark.helpers.postprocessor as postprocessor
import spark.helpers.normalized_records_unloader as normalized_records_unloader


def run(spark, runner, date_input):

    input_path = 's3a://salusv/incoming/emr/amazingcharts/{}/'.format(date_input.replace('-', '/'))
    matching_path = 's3a://salusv/matching/payload/emr/amazingcharts/{}/'.format(date_input.replace('-', '/'))

    runner.run_spark_script('amazingcharts_skinny_model.sql', [
        ['table_name', 'normalized_data', False],
        ['properties', '', False]
    ])

    payload_loader.load(runner, matching_path, ['personId'])

    runner.run_spark_script('load_transactions.sql', [
        ['input_path', input_path]
    ])

    transaction_tables = [
        'f_diagnosis', 'f_encounter', 'f_medication', 'f_procedure', 'f_lab', 'd_drug', 'd_cpt', 'd_multum_to_ndc'
    ]

    # trim and nullify all incoming transactions tables
    for table in transaction_tables:
        postprocessor.compose(
            postprocessor.trimmify, postprocessor.nullify
        )(runner.sqlContext.sql('select * from {}'.format(table))).createOrReplaceTempView(table)

    runner.run_spark_script('normalize.sql')

    normalized_records_unloader.partition_and_rename(
        spark, runner, 'emr', 'amazingcharts_skinny_model.sql', '40',
        'normalized_data', 'date_service', date_input
    )


def main(args):
    # init
    spark, sqlContext = init("AmazingCharts")

    # initialize runner
    runner = Runner(sqlContext)

    run(spark, runner, args.date, airflow_test=args.airflow_test)

    spark.stop()

    output_path = 's3://salusv/warehouse/parquet/emr/amazingcharts/'

    normalized_records_unloader.distcp(output_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str)
    args = parser.parse_args()
    main(args)
