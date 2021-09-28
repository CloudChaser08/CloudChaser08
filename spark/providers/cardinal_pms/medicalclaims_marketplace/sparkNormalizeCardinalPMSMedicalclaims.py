"""
cardinal pms normalize
"""
import argparse


#     ------------------------ Provider specific configuration -----------------------
import spark.providers.cardinal_pms.medicalclaims_marketplace.transactional_schemas as source_schema
from spark.common.marketplace_driver import MarketplaceDriver
from spark.common.medicalclaims import schemas as claims_schemas

if __name__ == "__main__":
    # ------------------------ Provider specific configuration -----------------------
    provider_name = 'cardinal_pms'
    output_table_names_to_schemas = {
        'cardinal_pms_837_hvm_norm07_nrm_fnl': claims_schemas['schema_v8']
    }
    provider_partition_name = 'cardinal_pms'

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
        source_schema,
        output_table_names_to_schemas,
        date_input,
        end_to_end_test
    )
    driver.run()
