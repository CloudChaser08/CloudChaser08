"""
rx acentrus normalize
"""
import os
import argparse
import spark.common.utility.logger as logger
from spark.common.utility.output_type import DataType, RunType
import spark.providers.acentrus.pharmacyclaims.transactional_schemas_v1 as transactional_schemas_v1
import spark.providers.acentrus.pharmacyclaims.transactional_schemas_v2 as transactional_schemas_v2
from spark.common.marketplace_driver import MarketplaceDriver
from spark.common.pharmacyclaims import schemas

if __name__ == "__main__":

    # ------------------------ Provider specific configuration -----------------------
    provider_name = 'acentrus'
    schema = schemas['schema_v11']
    output_table_names_to_schemas = {
        'acentrus_norm_40_final': schema
    }
    provider_partition_name = provider_name

    # ------------------------ Common for all providers -----------------------

    # Parse input arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str)
    parser.add_argument('--end_to_end_test', default=False, action='store_true')
    args = parser.parse_known_args()[0]
    date_input = args.date
    end_to_end_test = args.end_to_end_test

    v2_schema_dates = ['2021-11-09', '2021-11-16', '2021-11-23', '2021-12-07']
    source_table_schemas = transactional_schemas_v2 if date_input in v2_schema_dates else transactional_schemas_v1

    # Create and run driver
    driver = MarketplaceDriver(
        provider_name,
        provider_partition_name,
        source_table_schemas,
        output_table_names_to_schemas,
        date_input,
        end_to_end_test,
        unload_partition_count=1,
        vdr_feed_id=259,
        use_ref_gen_values=True,
        output_to_transform_path=False
    )

    # # init
    conf_parameters = {
        'spark.executor.memoryOverhead': 2048,
        'spark.driver.memoryOverhead': 2048,
        'spark.sql.autoBroadcastJoinThreshold': 104857600
    }

    driver.run(conf_parameters=conf_parameters)
    logger.log('Done')
