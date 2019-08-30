
import spark.providers.nthrive.cdm.transactional_schemas as source_table_schemas
from spark.common.marketplace_driver import MarketplaceDriver
from spark.common.cdm.diagnosis import schemas as diagnosis_schema
from spark.common.cdm.encounter import schemas as encounter_schema
from spark.common.cdm.encounter_detail import schemas as encounter_detail_schema
import argparse

if __name__ == "__main__":

    # ------------------------ Provider specific configuration -----------------------
    provider_name = 'nthrive'
    output_table_names_to_schemas = {
        'nthrive_norm02_diagnosis': diagnosis_schema['schema_v1'],
        'nthrive_norm01_encounter': encounter_schema['schema_v1'],
        'nthrive_norm06_encounter_detail': encounter_detail_schema['schema_v1']
    }
    provider_partition_name = '149'

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
    driver.run()
