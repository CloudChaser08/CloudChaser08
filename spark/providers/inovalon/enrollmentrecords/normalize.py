import argparse
import spark.providers.inovalon.enrollmentrecords.transactional_schemas as source_table_schemas
from spark.common.marketplace_driver import MarketplaceDriver
from spark.common.enrollment import schemas as enrollment_schemas

if __name__ == "__main__":

    # ------------------------ Provider specific configuration -----------------------
    provider_name = 'inovalon'
    output_table_names_to_schemas = {
        'inovalon_enr2_norm_final': enrollment_schemas['schema_v5'],
    }
    provider_partition_name = provider_name

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
        end_to_end_test,
        load_date_explode=False,
        unload_partition_count=40
    )

    driver.run()
