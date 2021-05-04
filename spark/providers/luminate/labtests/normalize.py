import argparse
import spark.providers.luminate.labtests.transactional_schemas as source_table_schemas
from spark.common.marketplace_driver import MarketplaceDriver
from spark.common.lab_common_model import schemas as labtest_schemas
# import spark.helpers.external_table_loader as external_table_loader

if __name__ == "__main__":

    # ------------------------ Provider specific configuration -----------------------
    provider_name = 'luminate'
    output_table_names_to_schemas = {
        'luminatehealth_labtest': labtest_schemas['schema_v9'],
    }
    provider_partition_name = 'luminate'

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
        vdr_feed_id=195
    )

    driver.run()
