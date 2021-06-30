
from spark.providers.nthrive.cdm import transactional_schemas, transactional_schemas_v1
from spark.common.marketplace_driver import MarketplaceDriver
from spark.common.cdm.diagnosis import schemas as diagnosis_schema
from spark.common.cdm.encounter import schemas as encounter_schema
from spark.common.cdm.encounter_detail import schemas as encounter_detail_schema
from spark.common.cdm.encounter_provider import schemas as encounter_provider_schema
import spark.common.utility.logger as logger
from datetime import datetime
import argparse

if __name__ == "__main__":

    # ------------------------ Provider specific configuration -----------------------
    provider_name = 'nthrive'
    v_cutoff_date = "2021-03-10"
    output_table_names_to_schemas = {
        'nthrive_norm01_encounter': encounter_schema['schema_v2'],
        'nthrive_norm02_diagnosis': diagnosis_schema['schema_v2'],
        'nthrive_norm06_encounter_detail': encounter_detail_schema['schema_v1'],
        'nthrive_norm07_encounter_provider': encounter_provider_schema['schema_v1']
    }
    provider_partition_name = '149'

    # ------------------------ Common for all providers -----------------------

    # Parse input arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str)
    parser.add_argument('--end_to_end_test', default=False, action='store_true')
    args = parser.parse_known_args()[0]
    date_input = args.date
    end_to_end_test = args.end_to_end_test

    if datetime.strptime(date_input, '%Y-%m-%d') < datetime.strptime(v_cutoff_date, '%Y-%m-%d'):
        logger.log('Historic Load schema')
        source_table_schemas = transactional_schemas
    else:
        logger.log('Future Load using new schema with icu_indicator column')
        source_table_schemas = transactional_schemas_v1
    
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
        'spark.sql.autoBroadcastJoinThreshold': -1,
        'spark.executor.cores': 5,
        'spark.buffer.pageSize': '2m',
        'spark.network.timeout': '600s',
        'spark.sql.crossJoin.enabled': 'true',
        'spark.sql.broadcastTimeout' : 9000
    }

    driver.run(conf_parameters=conf_parameters)
