import argparse
import spark.providers.bioreference_c19.labtests.transactional_schemas as source_table_schemas
from spark.common.marketplace_driver import MarketplaceDriver
from spark.common.lab_common_model import schemas as labtest_schemas
import spark.helpers.external_table_loader as external_table_loader
import spark.common.utility.logger as logger

if __name__ == "__main__":

    # ------------------------ Provider specific configuration -----------------------
    provider_name = 'bioreference_c19'
    output_table_names_to_schemas = {
        'bioreference_labtest': labtest_schemas['schema_v9'],
    }
    provider_partition_name = 'bioreference'

    # ------------------------ Common for all providers -----------------------

    # Parse input arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str)
    parser.add_argument('--end_to_end_test', default=False, action='store_true')
    args = parser.parse_args()
    date_input = args.date
    end_to_end_test = args.end_to_end_test

    # Create and run driver
    driver = MarketplaceDriver(
        provider_name,
        provider_partition_name,
        source_table_schemas,
        output_table_names_to_schemas,
        date_input,
        end_to_end_test
    )
    driver.init_spark_context()

    logger.log('Loading external table: ref_geo_state')
    external_table_loader.load_analytics_db_table(
        driver.sql_context, 'dw', 'ref_geo_state', 'ref_geo_state'
    )

    driver.run()
