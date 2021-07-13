import argparse
import spark.providers.practice_insight.era.transactional_schemas as source_table_schemas
from spark.common.marketplace_driver import MarketplaceDriver
from spark.common.era.detail import schemas as detail_schemas
from spark.common.era.summary import schemas as summary_schemas
import spark.common.utility.logger as logger

if __name__ == "__main__":
    # ------------------------ Provider specific configuration -----------------------
    provider_name = 'practice_insight'
    output_table_names_to_schemas = {
        'practice_insight_era_detail_final': detail_schemas['schema_v7'],
        'practice_insight_era_summary_final': summary_schemas['schema_v7']
    }
    provider_partition_name = '216'

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
        load_date_explode=False,
        unload_partition_count=2,
        vdr_feed_id=216,
        use_ref_gen_values=True,
        output_to_transform_path=True
    )

    conf_parameters = {
        'spark.default.parallelism': 4000,
        'spark.sql.shuffle.partitions': 4000,
        'spark.executor.memoryOverhead': 1024,
        'spark.driver.memoryOverhead': 1024,
        'spark.driver.extraJavaOptions': '-XX:+UseG1GC',
        'spark.executor.extraJavaOptions': '-XX:+UseG1GC',
        'spark.sql.autoBroadcastJoinThreshold': 52428800
    }

    driver.init_spark_context(conf_parameters=conf_parameters)
    driver.load(payloads=False)
    driver.transform()
    driver.save_to_disk()
    driver.stop_spark()
    driver.log_run()
    driver.copy_to_output_path()
    logger.log('Done')
