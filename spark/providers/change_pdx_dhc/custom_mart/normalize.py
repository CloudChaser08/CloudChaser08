"""
change pdx dhc normalize
"""
import argparse
from spark.providers.change_pdx_dhc.custom_mart import \
    transactional_schemas, transactional_schemas_v1
from spark.common.marketplace_driver import MarketplaceDriver
from spark.common.datamart.dhc.custom import schemas as dhc_custom_schemas
import spark.common.utility.logger as logger
from datetime import datetime

hasDeliveryPath = True
v_cutoff_date = "2021-04-25"

if __name__ == "__main__":

    # ------------------------ Provider specific configuration -----------------------
    provider_name = 'change_pdx_dhc'
    output_table_names_to_schemas = {
        'pdx_dhc_crosswalk_norm_final': dhc_custom_schemas['rx_token_bridge_v1']
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

    if datetime.strptime(date_input, '%Y-%m-%d') < datetime.strptime(v_cutoff_date, '%Y-%m-%d'):
        logger.log('Historic Load schema')
        source_table_schemas = transactional_schemas
    else:
        logger.log('Load using new schema with claim_number column')
        source_table_schemas = transactional_schemas_v1

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
