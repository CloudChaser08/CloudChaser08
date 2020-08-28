
import spark.providers.nthrive.cdm.transactional_schemas as source_table_schemas
from spark.common.marketplace_driver import MarketplaceDriver
from spark.common.cdm.diagnosis import schemas as diagnosis_schema
from spark.common.cdm.encounter import schemas as encounter_schema
from spark.common.cdm.encounter_detail import schemas as encounter_detail_schema
from spark.common.cdm.encounter_provider import schemas as encounter_provider_schema
import argparse

if __name__ == "__main__":

    # ------------------------ Provider specific configuration -----------------------
    provider_name = 'nthrive'
    output_table_names_to_schemas = {
        'nthrive_norm01_encounter': encounter_schema['schema_v1'],
        'nthrive_norm02_diagnosis': diagnosis_schema['schema_v1'],
        'nthrive_norm06_encounter_detail': encounter_detail_schema['schema_v1'],
        'nthrive_norm07_encounter_provider': encounter_provider_schema['schema_v1']
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
        end_to_end_test,
        use_ref_gen_values=True,
        vdr_feed_id=provider_partition_name,
        output_to_transform_path=True
    )

    conf_parameters = {
        'spark.default.parallelism': 600,
        'spark.sql.shuffle.partitions': 600,
        'spark.sql.autoBroadcastJoinThreshold': 10485760,
        'spark.executor.cores': 5,
        'spark.buffer.pageSize': '2m',
        'spark.network.timeout': '600s'
    }

    driver.run(conf_parameters=conf_parameters)