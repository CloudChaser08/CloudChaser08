import argparse
import spark.providers.change_pdx_dhc.custom_mart.transactional_schemas as source_table_schemas
from spark.common.marketplace_driver import MarketplaceDriver
from spark.common.datamart.dhc.custom import schemas as dhc_custom_schemas
import spark.helpers.postprocessor as postprocessor
import spark.common.utility.logger as logger
import pyspark.sql.functions as F

hasDeliveryPath = True
alltime_pdx_txn_dw = 's3://salusv/reference/pdx_rx_txn_alltime_dw/'
alltime_pdx_txn_dw_filter = 's3://salusv/reference/pdx_rx_txn_alltime_dw_filter/'

if __name__ == "__main__":

    # ------------------------ Provider specific configuration -----------------------
    provider_name = 'change_pdx_dhc'
    output_table_names_to_schemas = {
        'pdx_dhc_crosswalk_matched_norm_final': dhc_custom_schemas['rx_token_bridge_v1']
    }
    provider_partition_name = 'pdx'

    # ------------------------ Common for all providers -----------------------

    # Parse input arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str)
    parser.add_argument('--end_to_end_test', default=False, action='store_true')
    args = parser.parse_known_args()[0]
    date_input = args.date
    end_to_end_test = args.end_to_end_test

    # Create and run driver
    driver = MarketplaceDriver(
        provider_name,
        provider_partition_name,
        source_table_schemas,
        output_table_names_to_schemas,
        date_input,
        end_to_end_test,
        vdr_feed_id=65,
        use_ref_gen_values=True,
        unload_partition_count=1,
        load_date_explode=False,
        output_to_delivery_path=hasDeliveryPath,
        output_to_transform_path=False
    )

    # enable this for - alltime data read from original file
    # conf_parameters = {
    #     'spark.default.parallelism': 4000,
    #     'spark.sql.shuffle.partitions': 4000,
    #     'spark.executor.memoryOverhead': 1024,
    #     'spark.driver.memoryOverhead': 1024,
    #     'spark.driver.extraJavaOptions': '-XX:+UseG1GC',
    #     'spark.executor.extraJavaOptions': '-XX:+UseG1GC',
    #     'spark.sql.autoBroadcastJoinThreshold': 524288000
    # }

    conf_parameters = {
        'spark.executor.memoryOverhead': 1024,
        'spark.driver.memoryOverhead': 1024
    }

    driver.init_spark_context(conf_parameters=conf_parameters)
    driver.load(payloads=False)

    # ------------------------ Load Reference Tables -----------------------
    logger.log('Loading external table: pdx_rx_txn_alltime')
    # driver.spark.sql("msck repair table raw.pdx_rx_txn_alltime")
    # driver.spark.sql('select * from raw.pdx_rx_txn_alltime').createOrReplaceTempView('pdx_rx_txn_alltime_dw')

    driver.spark.read.parquet(alltime_pdx_txn_dw_filter).createOrReplaceTempView('pdx_rx_txn_alltime_dw')
    # date_filled_distinct = driver.spark.table('crs').select('date_filled').distinct()
    # filter down the pdx_rx_txn_alltime table by removing rows that are definitely
    # irrelevant
    # driver.spark.table('pdx_rx_txn_alltime_dw_all') \
    #     .join(F.broadcast(date_filled_distinct), 'date_filled') \
    #     .createOrReplaceTempView("pdx_rx_txn_alltime_dw")

    logger.log('Start transform')
    driver.transform()
    driver.save_to_disk()
    driver.stop_spark()
    driver.log_run()
    driver.copy_to_output_path()
    logger.log('Done')
