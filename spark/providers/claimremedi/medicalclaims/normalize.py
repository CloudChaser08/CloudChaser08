"""
claimremedi normalize
"""
import os
import argparse
import subprocess
from datetime import datetime
import spark.providers.claimremedi.medicalclaims.transactional_schemas as source_table_schemas
from spark.common.marketplace_driver import MarketplaceDriver
from spark.common.medicalclaims import schemas as medicalclaims_schemas
import spark.common.utility.logger as logger
import spark.helpers.postprocessor as postprocessor
from dateutil.relativedelta import relativedelta
import spark.helpers.file_utils as file_utils

if __name__ == "__main__":

    # ------------------------ Provider specific configuration -----------------------
    provider_name = 'claimremedi'
    output_table_names_to_schemas = {
        'claimremedi_13_norm_final': medicalclaims_schemas['schema_v1'],
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

    # Create and run driver
    # Create and run driver
    driver = MarketplaceDriver(
        provider_name,
        provider_partition_name,
        source_table_schemas,
        output_table_names_to_schemas,
        date_input,
        end_to_end_test,
        vdr_feed_id=264,
        use_ref_gen_values=True,
        unload_partition_count=3,
        load_date_explode=True,
        output_to_transform_path=True
    )

    conf_parameters = {
        'spark.default.parallelism': 600,
        'spark.sql.shuffle.partitions': 600,
        'spark.executor.memoryOverhead': 1024,
        'spark.driver.memoryOverhead': 1024,
        'spark.sql.autoBroadcastJoinThreshold': 10485760
    }

    driver.run(conf_parameters=conf_parameters)
    logger.log('Done')
