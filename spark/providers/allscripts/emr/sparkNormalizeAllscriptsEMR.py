#! /usr/bin/python
import argparse
from spark.runner import Runner
from spark.spark_setup import init
import spark.providers.allscripts.emr.transaction_schemas as transaction_schemas
import spark.helpers.file_utils as file_utils
import spark.helpers.payload_loader as payload_loader
import spark.helpers.constants as constants
import spark.helpers.postprocessor as postprocessor
import spark.helpers.normalized_records_unloader as normalized_records_unloader


def run(spark, runner, date_input, test=False, airflow_test=False):

    script_path = __file__

    if test:
        input_path = file_utils.get_abs_path(
            script_path, '../../test/providers/allscripts/resources/input/year/month/day/'
        ) + '/'
        matching_path = file_utils.get_abs_path(
            script_path, '../../test/providers/allscripts/resources/matching/year/month/day/'
        ) + '/'
    elif airflow_test:
        input_path = 's3://salusv/testing/dewey/airflow/e2e/allscripts/emr/out/{}/'.format(
            date_input.replace('-', '/')
        )
        matching_path = 's3://salusv/testing/dewey/airflow/e2e/allscripts/emr/payload/{}/'.format(
            date_input.replace('-', '/')
        )
    else:
        input_path = 's3a://salusv/incoming/emr/allscripts/{}/'.format(
            date_input.replace('-', '/')
        )
        matching_path = 's3a://salusv/matching/payload/emr/allscripts/{}/'.format(
            date_input.replace('-', '/')
        )

    payload_loader.load(runner, matching_path, ['personid'])

    transaction_tables = [
        ('vitals', transaction_schemas.vitals),
        ('vaccines', transaction_schemas.vaccines),
        ('results', transaction_schemas.results),
        ('providers', transaction_schemas.providers),
        ('problems', transaction_schemas.problems),
        ('patients', transaction_schemas.patientdemographics),
        ('orders', transaction_schemas.orders),
        ('medications', transaction_schemas.medications),
        ('encounters', transaction_schemas.encounters),
        ('appointments', transaction_schemas.appointments),
        ('allergies', transaction_schemas.allergies)
    ]

    for table_name, table_schema in transaction_tables.keys():
        postprocessor.compose(
            postprocessor.trimmify, postprocessor.nullify
        )(
            runner.sqlContext.read.csv('{}{}/'.format(input_path, table_name), schema=table_schema)
        ).createOrReplaceTempView(table_name)

    # normalize and unload
    runner.run_spark_script('normalize.sql', return_output=True).repartition(20).write.parquet(
        constants.hdfs_staging_dir
    )


def main(args):
    # init
    spark, sqlContext = init("Allscripts EMR")

    # initialize runner
    runner = Runner(sqlContext)

    run(spark, runner, args.date, airflow_test=args.airflow_test)

    spark.stop()

    if args.airflow_test:
        output_path = 's3://salusv/testing/dewey/airflow/e2e/allscripts/emr/spark-output/'
    else:
        output_path = 's3://salusv/warehouse/parquet/emr/allscripts/{}/'.format(args.date.replace('-','/'))

    normalized_records_unloader.distcp(output_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str)
    parser.add_argument('--airflow_test', default=False, action='store_true')
    args = parser.parse_args()
    main(args)
