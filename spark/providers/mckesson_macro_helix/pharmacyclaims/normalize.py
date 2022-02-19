"""
mckesson macro helix normalize
"""
import argparse
from spark.common.utility import logger
from spark.common.pharmacyclaims import schemas
from spark.common.marketplace_driver import MarketplaceDriver
import spark.helpers.file_utils as file_utils
import spark.providers.mckesson_macro_helix.pharmacyclaims.transaction_schemas_v1 as old_schema
import spark.providers.mckesson_macro_helix.pharmacyclaims.transaction_schemas_v2 as new_schema

cutoff_date = datetime(2018, 9, 30)


def run(date_input, end_to_end_test=False, test=False, spark=None, runner=None):
    logger.log("Mckesson Normalize ")
    # ------------------------ Provider specific configuration -----------------------
    provider_name = 'mckesson_macro_helix'
    output_table_names_to_schemas = {
        'mckesson_macro_helix_rx_norm_final': schemas['schema_v6']
    }
    provider_partition_name = provider_name

    # ------------------------ Common for all providers -----------------------

    if datetime.strptime(date_input, '%Y-%m-%d') < cutoff_date:
        source_table_schemas = old_schema
    else:
        source_table_schemas = new_schema

    # Create and run driver
    driver = MarketplaceDriver(
        provider_name,
        provider_partition_name,
        source_table_schemas,
        output_table_names_to_schemas,
        date_input,
        end_to_end_test,
        test=test,
        unload_partition_count=2,
        vdr_feed_id=51,
        load_date_explode=False,
        use_ref_gen_values=True,
        output_to_transform_path=False
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
            '../../../test/providers/mckesson_macro_helix/pharmacyclaims/resources/input/'
        ) + '/'
        driver.matching_path = file_utils.get_abs_path(
            script_path,
            '../../../test/providers/mckesson_macro_helix/pharmacyclaims/resources/matching/'
        ) + '/'

    driver.load()
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
