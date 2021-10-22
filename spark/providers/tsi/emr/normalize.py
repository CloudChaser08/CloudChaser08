"""
tsi emr normalize
"""
import os
import argparse
import spark.providers.tsi.emr.transactional_schemas as source_table_schemas
from spark.common.marketplace_driver import MarketplaceDriver
from spark.common.utility import logger
from spark.common.emr.encounter import schemas as encounter_schemas
from spark.common.emr.diagnosis import schemas as diagnosis_schemas
from spark.common.emr.procedure import schemas as procedure_schemas
from spark.common.emr.provider_order import schemas as provider_order_schema
from spark.common.emr.medication import schemas as medication_schemas
from spark.common.emr.lab_test import schemas as lab_test_schemas
from spark.common.emr.clinical_observation import schemas as clinical_observation_schemas
import spark.helpers.external_table_loader as external_table_loader

HAS_DELIVERY_PATH = True

if __name__ == "__main__":
    # ------------------------ Provider specific configuration -----------------------

    provider_name = 'tsi'
    output_table_names_to_schemas = {
        'tsi_emr_norm_emr_enc_final': encounter_schemas['schema_v10'],
        'tsi_emr_norm_emr_diag_final': diagnosis_schemas['schema_v10'],
        'tsi_emr_norm_emr_proc_final': procedure_schemas['schema_v12'],
        'tsi_emr_norm_emr_prov_ord_final': provider_order_schema['schema_v9'],
        'tsi_emr_norm_emr_med_ord_final': medication_schemas['schema_v11'],
        'tsi_emr_norm_emr_lab_test_final': lab_test_schemas['schema_v3'],
        'tsi_emr_norm_emr_clin_obsn_final': clinical_observation_schemas['schema_v11']
    }

    provider_partition_name = '241'

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
        vdr_feed_id=241,
        use_ref_gen_values=True,
        output_to_delivery_path=HAS_DELIVERY_PATH,
        output_to_transform_path=True
    )

    driver.output_path = os.path.join(driver.output_path, 'hv002854/')

    # init
    conf_parameters = {
        'spark.executor.memoryOverhead': 1024,
        'spark.driver.memoryOverhead': 1024
    }
    logger.log('Loading external table: gen_ref_whtlst')
    external_table_loader.load_analytics_db_table(
        driver.runner.sqlContext, 'dw', 'gen_ref_whtlst', 'gen_ref_whtlst')
    driver.run(conf_parameters=conf_parameters)
