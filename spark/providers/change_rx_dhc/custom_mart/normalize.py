import argparse
import spark.providers.change_rx_dhc.custom_mart.transactional_schemas as source_table_schemas_v1
import spark.providers.change_rx_dhc.custom_mart.transactional_schemas_v2 as source_table_schemas_v2
from spark.common.marketplace_driver import MarketplaceDriver
from spark.common.datamart.dhc.custom import schemas as dhc_custom_schemas
import spark.common.utility.logger as logger

hasDeliveryPath = True

if __name__ == "__main__":

    # ------------------------ Provider specific configuration -----------------------
    provider_name = 'change_rx_dhc'
    output_table_names_to_schemas = {
        'change_rx_norm_final': dhc_custom_schemas['rx_token_bridge_v1']
    }
    provider_partition_name = 'emdeon'

    # ------------------------ Common for all providers -----------------------

    # Parse input arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str)
    parser.add_argument('--end_to_end_test', default=False, action='store_true')
    args = parser.parse_known_args()[0]
    date_input = args.date
    end_to_end_test = args.end_to_end_test

    if date_input >= '2021-01-01':
        source_table_schemas = source_table_schemas_v2
        logger.log('Using 2021-present source schema...')
    else:
        source_table_schemas = source_table_schemas_v1
        logger.log('Using 2016-2020 source schema...')

    # Create and run driver
    driver = MarketplaceDriver(
        provider_name,
        provider_partition_name,
        source_table_schemas,
        output_table_names_to_schemas,
        date_input,
        end_to_end_test,
        vdr_feed_id=11,
        use_ref_gen_values=True,
        unload_partition_count=40,
        load_date_explode=False,
        output_to_delivery_path=hasDeliveryPath,
        output_to_transform_path=False
    )

    conf_parameters = {
        'spark.executor.memoryOverhead': 1024,
        'spark.driver.memoryOverhead': 1024
    }

    driver.init_spark_context(conf_parameters=conf_parameters)
    driver.load(payloads=False)

    logger.log('Start transform')
    driver.transform()
    driver.save_to_disk()
    driver.stop_spark()
    driver.log_run()
    driver.copy_to_output_path()
    logger.log('Done')
