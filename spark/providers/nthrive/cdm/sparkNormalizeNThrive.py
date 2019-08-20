import os
import inspect
import argparse
import spark.providers.nthrive.cdm.transactional_schemas as source_table_schemas
from spark.common.marketplace_driver import MarketplaceDriver
from spark.common.cdm.diagnosis import schemas as diagnosis_schema
from spark.common.cdm.encounter import schemas as encounter_schema
from spark.common.cdm.encounter_detail import schemas as encounter_detail_schema

if __name__ == "__main__":

    # ------------------------ Provider specific configuration -----------------------
    provider_name = 'nthrive'
    data_type = 'cdm'
    output_table_names_to_schemas = {
        'nthrive_norm02_diagnosis': diagnosis_schema['schema_v1'],
        'nthrive_norm01_encounter': encounter_schema['schema_v1'],
        'nthrive_norm06_encounter_detail': encounter_detail_schema['schema_v1']
    }
    provider_partition_column = 'part_hvm_vdr_feed_id'
    date_partition_column = 'part_mth'
    distribution_key = 'row_id'

    # --------------------------- Common for all providers ---------------------------
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str)
    parser.add_argument('--end_to_end_test', default=False, action='store_true')
    args = parser.parse_args()
    input_date = args.date
    end_to_end_test = args.end_to_end_test
    script_path = __file__
    provider_directory_path = os.path.dirname(inspect.getframeinfo(inspect.stack()[0][0]).filename)
    provider_directory_path = provider_directory_path.replace('spark/target/dewey.zip/', "") + '/'

    driver = MarketplaceDriver(
        provider_name,
        data_type,
        input_date,
        script_path,
        provider_directory_path,
        source_table_schemas,
        output_table_names_to_schemas,
        provider_partition_column,
        date_partition_column,
        end_to_end_test=end_to_end_test,
        distribution_key=distribution_key
    )
    driver.run()
