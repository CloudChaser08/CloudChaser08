import argparse
import spark.providers.waystar.era.transactional_schemas as source_table_schemas
from spark.common.marketplace_driver import MarketplaceDriver
from spark.common.era.detail import schemas as detail_schemas
from spark.common.era.summary import schemas as summary_schemas


if __name__ == "__main__":

    # ------------------------ Provider specific configuration -----------------------
    provider_name = 'waystar'
    output_table_names_to_schemas = {
        'waystar_835_normalized_detail': detail_schemas['schema_v5'],
        'waystar_835_normalized_summary': summary_schemas['schema_v5']
    }
    provider_partition_name = '145'

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
        end_to_end_test
    )
    driver.run()
