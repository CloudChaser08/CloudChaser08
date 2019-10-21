import argparse
import spark.providers.sentry.cdm.transactional_schemas as source_table_schemas
from spark.common.marketplace_driver import MarketplaceDriver
from spark.common.cdm.encounter import schemas as encounter_schema
from spark.common.cdm.diagnosis import schemas as diagnosis_schema
from spark.common.cdm.encounter_detail import schemas as encounter_detail_schema
from spark.common.cdm.encounter_provider import schemas as encounter_provider_schema
import spark.common.utility.logger as logger
import spark.helpers.records_loader as records_loader
import spark.helpers.payload_loader as payload_loader
import spark.helpers.external_table_loader as external_table_loader

if __name__ == "__main__":

    # ------------------------ Provider specific configuration -----------------------
    provider_name = 'sentry'
    output_table_names_to_schemas = {
        'sentry_norm01_enc': encounter_schema['schema_v1'],
        'sentry_norm02_enc_prov': encounter_provider_schema['schema_v1'],
        'sentry_norm05_enc_dtl': encounter_detail_schema['schema_v2'],
        'sentry_norm06_diag': diagnosis_schema['schema_v1']
    }
    provider_partition_name = '150'

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
    driver.init_spark_context()
    # custom load function
    logger.log('Loading the source data')
    logger.log(' -loading: transactions')
    records_loader.load_and_clean_all_v2(driver.runner, driver.input_path,
                                         driver.source_table_schema, load_file_name=True)

    logger.log(' -loading: payloads')
    payload_names = ['clm_pay', 'dgn_pay', 'dsp_pay', 'lin_pay', 'loc_pay', 'pat_pay', 'prv_pay']
    for name in payload_names:
        payload_path = driver.matching_path + name + '/'
        payload_loader.load(driver.runner, payload_path, table_name=name, load_file_name=True)

    if not driver.test:
        logger.log(' -loading: ref_gen_ref')
        external_table_loader.load_ref_gen_ref(driver.runner.sqlContext)

    driver.transform()
    driver.save_to_disk()
    driver.stop_spark()
    driver.copy_to_output_path()
