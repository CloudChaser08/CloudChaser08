"""
vigilanz labtests normalize
"""
import argparse
from spark.common.emr.clinical_observation import schemas as clinical_observation_schemas
from spark.common.emr.diagnosis import schemas as diagnosis_schemas
from spark.common.emr.encounter import schemas as encounter_schemas
from spark.common.emr.lab_test import schemas as lab_test_schema
from spark.common.emr.medication import schemas as medication_schemas
import spark.providers.vigilanz.emr.transactional_schemas_v1 as transactional_schemas_v1
from spark.common.marketplace_driver import MarketplaceDriver

HAS_DELIVERY_PATH = False

if __name__ == "__main__":

    # ------------------------ Provider specific configuration -----------------------
    provider_name = 'vigilanz'
    output_table_names_to_schemas = {
        'vigilanz_emr_norm_diag_final': diagnosis_schemas['schema_v10'],
        'vigilanz_emr_norm_clin_obsn_final': clinical_observation_schemas['schema_v11'],
        'vigilanz_emr_norm_enc_final': encounter_schemas['schema_v10'],
        'vigilanz_emr_norm_medctn_final': medication_schemas['schema_v11'],
        'vigilanz_emr_norm_lab_test_final': lab_test_schema['schema_v3']
    }
    provider_partition_name = '250'

    # ------------------------ Common for all providers -----------------------

    # Parse input arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str)
    parser.add_argument('--end_to_end_test', default=False, action='store_true')
    args = parser.parse_known_args()[0]
    date_input = args.date
    end_to_end_test = args.end_to_end_test

    # v1 Transactional Schemas
    source_table_schemas = transactional_schemas_v1

    # Create and run driver
    driver = MarketplaceDriver(
        provider_name,
        provider_partition_name,
        source_table_schemas,
        output_table_names_to_schemas,
        date_input,
        end_to_end_test,
        vdr_feed_id=250,
        use_ref_gen_values=True,
        output_to_delivery_path=HAS_DELIVERY_PATH,
        output_to_transform_path=False
    )

    # init
    conf_parameters = {
        'spark.executor.memoryOverhead': 2048,
        'spark.driver.memoryOverhead': 2048,
        'spark.driver.extraJavaOptions': '-XX:+UseG1GC',
        'spark.executor.extraJavaOptions': '-XX:+UseG1GC',
        'spark.sql.autoBroadcastJoinThreshold': 104857600
    }

    driver.run(conf_parameters=conf_parameters)
