"""
acxiom normalize
"""
import argparse
from spark.common.marketplace_driver import MarketplaceDriver
from spark.common.consumer.events_common_model_v10 import schema
import spark.common.utility.logger as logger
import spark.helpers.payload_loader as payload_loader
from pyspark.sql.types import StructType, StructField, StringType
import spark.helpers.file_utils as file_utils


def run(date_input, test=False, end_to_end_test=False, spark=None, runner=None):

    # ------------------------ Provider specific configuration -----------------------
    provider_name = 'acxiom'
    output_table_names_to_schemas = {
        'acxiom_norm_event': schema
    }
    provider_partition_name = provider_name

    source_table_schemas = None
    # ------------------------ Common for all providers -----------------------

    # Create and run driver
    driver = MarketplaceDriver(
        provider_name,
        provider_partition_name,
        source_table_schemas,
        output_table_names_to_schemas,
        date_input,
        end_to_end_test=end_to_end_test,
        test=test,
        use_ref_gen_values=True,
        vdr_feed_id=50,
        unload_partition_count=20,
        output_to_transform_path=False
    )

    script_path = __file__

    if test:
        driver.matching_path = file_utils.get_abs_path(
            script_path, '../../../test/providers/acxiom/events/resources/matching/'
        )
        ids_path = file_utils.get_abs_path(
            script_path, '../../../test/providers/acxiom/events/resources/ids/'
        )
    elif end_to_end_test:
        driver.matching_path = 's3://salusv/testing/dewey/airflow/e2e/acxiom/payload/{}/'.format(
            date_input.replace('-', '/')
        )
        ids_path = 's3://salusv/testing/dewey/airflow/e2e/acxiom/ids/'
    else:
        ids_path = 's3://salusv/reference/acxiom/ids/'

    conf_parameters = {
        'spark.default.parallelism': 2000,
        'spark.sql.shuffle.partitions': 2000,
        'spark.executor.memoryOverhead': 512,
        'spark.driver.memoryOverhead': 512
    }

    if not test:
        driver.init_spark_context(conf_parameters=conf_parameters)
        acxiom_ids = driver.runner.sqlContext.read.parquet(ids_path)
    else:
        driver.spark = spark
        driver.runner = runner
        acxiom_ids = driver.runner.sqlContext.read.csv(ids_path,
                                                       StructType([StructField('aid', StringType(), True)]), sep='|')

    # Remove any source ids that are not in the acxiom ids table
    acxiom_ids.cache().createOrReplaceTempView('exclude_acxiom_ids')

    payload_loader.load(driver.runner, driver.matching_path, ['claimId'], load_file_name=True)

    driver.transform()
    if not test:
        driver.save_to_disk()
        driver.stop_spark()
        driver.log_run()
        driver.copy_to_output_path()
    logger.log('Done')


def main(args):
    run(args.date, end_to_end_test=args.airflow_test)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str)
    parser.add_argument('--airflow_test', default=False, action='store_true')
    args = parser.parse_known_args()[0]
    main(args)
