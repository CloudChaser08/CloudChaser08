import argparse
import spark.providers.inovalon_dhc.custom_mart.transactional_schemas as source_table_schemas
from spark.common.marketplace_driver import MarketplaceDriver
from spark.common.datamart.dhc.custom import schemas as dhc_custom_schemas
import spark.helpers.postprocessor as postprocessor
import spark.common.utility.logger as logger
import pyspark.sql.functions as F

hasDeliveryPath = True

if __name__ == "__main__":

    # ------------------------ Provider specific configuration -----------------------
    provider_name = 'inovalon_dhc'
    output_table_names_to_schemas = {
        'inovalon_norm_final': dhc_custom_schemas['rx_token_bridge_v1']
    }
    provider_partition_name = 'inovalon'

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
        vdr_feed_id=177,
        use_ref_gen_values=False,
        unload_partition_count=80,
        load_date_explode=False,
        output_to_delivery_path=hasDeliveryPath,
        output_to_transform_path=True
    )

    conf_parameters = {
        'spark.executor.memoryOverhead': 1024,
        'spark.driver.memoryOverhead': 1024
    }

    driver.init_spark_context(conf_parameters=conf_parameters)
    driver.load()

    logger.log(' -filter down, distinct - trimmify-nullify matching_payload data')
    matching_payload_df = driver.spark.table('matching_payload').select('hvid', 'claimId').distinct()

    cleaned_matching_payload_df = (
        postprocessor.compose(postprocessor.trimmify, postprocessor.nullify)(matching_payload_df)) \
        .createOrReplaceTempView("matching_payload")

    logger.log('Start transform')
    driver.transform()
    driver.save_to_disk()
    driver.stop_spark()
    driver.log_run()
    driver.copy_to_output_path()
    logger.log('Done')
