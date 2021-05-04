import argparse
import spark.providers.ovation.labtests.transactional_schemas as source_schema
import spark.providers.ovation.labtests.transactional_schemas_v2 as source_schema_v2
import spark.providers.ovation.labtests.transactional_schemas_v3 as source_schema_v3
from spark.common.marketplace_driver import MarketplaceDriver
from spark.common.lab_common_model import schemas as labtest_schemas


if __name__ == "__main__":

    # ------------------------ Provider specific configuration -----------------------
    provider_name = 'ovation'
    output_table_names_to_schemas = {
        'labtest_ovation_covid19': labtest_schemas['schema_v9'],
    }
    provider_partition_name = 'ovation'

    # ------------------------ Common for all providers -----------------------

    # Parse input arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str)
    parser.add_argument('--end_to_end_test', default=False, action='store_true')
    args = parser.parse_known_args()[0]
    date_input = args.date
    end_to_end_test = args.end_to_end_test

    if date_input >= '2021-05-03':  # changed schema to support 2 new client columns
        source_table_schemas = source_schema_v3
    elif date_input >= '2020-09-21':  # changed schema to support 4 new client columns
        source_table_schemas = source_schema_v2
    else:
        source_table_schemas = source_schema

    # Create and run driver
    driver = MarketplaceDriver(
        provider_name,
        provider_partition_name,
        source_table_schemas,
        output_table_names_to_schemas,
        date_input,
        end_to_end_test,
        vdr_feed_id=188,
        use_ref_gen_values=True
    )
    driver.run()
