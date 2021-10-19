"""
Scipher labtests normalize
"""
import argparse
import spark.providers.scipher.labtests.transactional_schemas as source_table_schemas
from spark.common.marketplace_driver import MarketplaceDriver
from spark.common.lab_common_model import schemas as labtest_schemas

HAS_DELIVERY_PATH = True

if __name__ == "__main__":
    # ------------------------ Provider specific configuration -----------------------
    provider_name = 'scipher'
    output_table_names_to_schemas = {
        'scipher_labtests_norm': labtest_schemas['schema_v9'],
    }
    provider_partition_name = 'scipher'

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
        vdr_feed_id=239,
        use_ref_gen_values=True,
        output_to_delivery_path=HAS_DELIVERY_PATH,
        output_to_transform_path=True
    )
    driver.run()
