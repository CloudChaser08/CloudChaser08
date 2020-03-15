import argparse
import spark.helpers.external_table_loader as external_table_loader
import spark.providers.pointclickcare.emr.transactional_schemas as source_table_schemas
from spark.common.marketplace_driver import MarketplaceDriver
from spark.common.emr.clinical_observation import schemas as clinical_observation_schemas
from spark.common.emr.diagnosis import schemas as diagnosis_schemas
from spark.common.emr.encounter import schemas as encounter_schemas
from spark.common.emr.medication import schemas as medication_schemas
from spark.common.emr.procedure import schemas as procedure_schemas
from spark.common.utility.output_type import RunType
import spark.common.utility.logger as logger

if __name__ == "__main__":

    # ------------------------ Provider specific configuration -----------------------
    provider_name = 'pointclickcare'
    output_table_names_to_schemas = {
        'pcc_emr_norm_emr_clin_obsn': clinical_observation_schemas['schema_v11'],
        'pcc_emr_norm_emr_diag': diagnosis_schemas['schema_v10'],
        'pcc_emr_norm_emr_enc': encounter_schemas['schema_v10'],
        'pcc_emr_norm_emr_medctn': medication_schemas['schema_v11'],
        'pcc_emr_norm_emr_proc': procedure_schemas['schema_v12']
    }
    provider_partition_name = '156'

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
    driver.load()

    logger.log('Loading external table: ref_geo_state')
    external_table_loader.load_analytics_db_table(
        driver.sql_context, 'dw', 'ref_geo_state', 'ref_geo_state'
    )
    driver.spark.table('ref_geo_state').cache().createOrReplaceTempView('ref_geo_state')

    logger.log('Caching tables to improve performance of clinical observation tables')
    logger.log('1 of 3 - Caching obs table')
    driver.spark.table('obs').cache().createOrReplaceTempView('obs')

    logger.log('2 of 3  - Caching alg table')
    driver.spark.table('alg').cache().createOrReplaceTempView('alg')

    logger.log('3 of 3 - Caching dclt table')
    driver.spark.table('dclt').cache().createOrReplaceTempView('dclt')

    driver.transform()
    driver.save_to_disk()

    if not driver.test and not driver.end_to_end_test:
        logger.log_run_details(
            driver.provider_name,
            driver.data_type,
            driver.input_path,
            driver.matching_path,
            driver.output_path,
            RunType.MARKETPLACE,
            driver.date_input
        )

    driver.stop_spark()
    driver.copy_to_output_path()
