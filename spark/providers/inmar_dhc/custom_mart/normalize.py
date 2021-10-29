"""
inmar dhc normalize
"""
import argparse
import spark.providers.inmar_dhc.custom_mart.transactional_schemas as source_table_schemas
from spark.common.marketplace_driver import MarketplaceDriver
from spark.common.datamart.dhc.custom import schemas as dhc_custom_schemas
import spark.common.utility.logger as logger
import spark.helpers.s3_utils as s3_utils
import os
from spark.providers.inmar_dhc.custom_mart.dhc_utils import dhc_hist_record_loader

hasDeliveryPath = True

if __name__ == "__main__":

    # ------------------------ Provider specific configuration -----------------------
    provider_name = 'inmar_dhc'
    dhc_schema = dhc_custom_schemas['rx_token_bridge_v1']
    output_table_names_to_schemas = {
        'inmar_norm_final': dhc_schema
    }
    provider_partition_name = 'inmar'

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
        vdr_feed_id=147,
        use_ref_gen_values=True,
        unload_partition_count=2,
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
    
    logger.log('Loading previous history for deduplication')

    hist_path = os.path.join(driver.output_path, dhc_schema.output_directory
                               , 'part_provider={}/part_file_date={}/'.format(provider_partition_name, date_input))
    dhc_hist_record_loader(driver, hist_path, date_input)

    logger.log('Start transform')
    driver.transform()
    driver.save_to_disk()
    driver.stop_spark()
    driver.log_run()
    driver.copy_to_output_path()
    logger.log('Done')
