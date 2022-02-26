"""
mckesson normalize
"""
import argparse
from pyspark.sql.functions import lit, col
from spark.common.utility import logger
from spark.common.pharmacyclaims import schemas
from spark.common.marketplace_driver import MarketplaceDriver
import spark.helpers.file_utils as file_utils
import spark.providers.mckesson.pharmacyclaims.transaction_schemas_v1 as old_schema
import spark.providers.mckesson.pharmacyclaims.transaction_schemas_v2 as new_schema


def run(date_input, end_to_end_test=False, test=False, spark=None, runner=None):
    logger.log("Mckesson Normalize ")
    # ------------------------ Provider specific configuration -----------------------
    provider_name = 'mckesson'
    output_table_names_to_schemas = {
        'mckesson_rx_norm_final': schemas['schema_v6']
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
        end_to_end_test,
        test=test,
        unload_partition_count=3,
        vdr_feed_id=33,
        load_date_explode=False,
        use_ref_gen_values=True,
        output_to_transform_path=True
    )

    conf_parameters = {
        'spark.executor.memoryOverhead': 1024,
        'spark.driver.memoryOverhead': 1024
    }

    if not test:
        driver.init_spark_context(conf_parameters=conf_parameters)
    else:
        driver.spark = spark
        driver.runner = runner

    script_path = __file__
    if test:
        driver.input_path = file_utils.get_abs_path(
            script_path,
            '../../../test/providers/mckesson/pharmacyclaims/resources/input/'
        ) + '/'
        driver.matching_path = file_utils.get_abs_path(
            script_path,
            '../../../test/providers/mckesson/pharmacyclaims/resources/matching/'
        ) + '/'

    column_length = len(driver.spark.read.csv(driver.input_path, sep='|').columns)
    if column_length == 118:
        source_table_schemas=new_schema
    elif column_length == 119:
        source_table_schemas=old_schema
    else:
        raise ValueError('Unexpected column length in transaction: {}'.format(str(column_length)))

    driver.source_table_schema = source_table_schemas
    driver.load()
    if column_length == 118:
        logger.log('Adding columns for new schema')
        """
        Mckesson has 2 schemas - the old schema had 119 columns, the
        new schema has 118. There is no date cutoff for when the new
        schema starts and the old ends, so the best way to choose the
        correct schema is to simply count the columns.
        """
        txn_df = driver.spark.table('txn')
        txn_df = txn_df.withColumn(
            'DispensingFeePaid', lit(None)
        ).withColumn(
            'FillerTransactionTime', col('ClaimTransactionTime')
        )
        txn_df.createOrReplaceTempView('txn')

    driver.transform()
    if not test:
        driver.save_to_disk()
        driver.stop_spark()
        driver.log_run()
        driver.copy_to_output_path()
    logger.log("Done")


if __name__ == "__main__":
    # Parse input arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str)
    parser.add_argument('--end_to_end_test', default=False, action='store_true')
    args = parser.parse_known_args()[0]

    run(args.date, args.end_to_end_test)
