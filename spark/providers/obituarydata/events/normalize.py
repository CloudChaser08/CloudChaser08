import argparse
import spark.providers.obituarydata.events.transactional_schemas_v1 as source_table_schemas
from spark.common.marketplace_driver import MarketplaceDriver
from spark.common.consumer.events_common_model_v10 import schema
import spark.helpers.external_table_loader as external_table_loader
import spark.common.utility.logger as logger

if __name__ == "__main__":

    # ------------------------ Provider specific configuration -----------------------
    provider_name = 'obituarydata'
    output_table_names_to_schemas = {
        'obituarydata_norm_event': schema
    }
    provider_partition_name = 'obituarydata'

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
        use_ref_gen_values=True,
        vdr_feed_id=27,
        unload_partition_count=1,
        output_to_transform_path=True
    )

    conf_parameters = {
        'spark.default.parallelism': 2000,
        'spark.sql.shuffle.partitions': 2000,
        'spark.executor.memoryOverhead': 512,
        'spark.driver.memoryOverhead': 512
    }

    driver.init_spark_context(conf_parameters=conf_parameters)
    driver.load(extra_payload_cols=['hvJoinKey', 'deathMonth'])

    driver.transform()
    driver.save_to_disk()
    driver.stop_spark()
    driver.log_run()
    driver.copy_to_output_path()
    logger.log('Done')
