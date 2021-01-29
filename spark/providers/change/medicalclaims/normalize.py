import argparse
import spark.providers.change.medicalclaims.transactional_schemas as source_table_schemas
from spark.common.marketplace_driver import MarketplaceDriver
from spark.common.medicalclaims_common_model import schemas as medicalclaims_schemas
import spark.common.utility.logger as logger

if __name__ == "__main__":

    # ------------------------ Provider specific configuration -----------------------
    provider_name = 'change_relay'
    output_table_names_to_schemas = {
        'change_relay_05_norm_final': medicalclaims_schemas['schema_v10'],
    }
    provider_partition_name = 'emdeon'

    # ------------------------ Common for all providers -----------------------

    # Parse input arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str)
    parser.add_argument('--end_to_end_test', default=False, action='store_true')
    args = parser.parse_args()
    date_input = args.date
    end_to_end_test = args.end_to_end_test

    logger.log('Future Load using matching_payload table to get PCN')

    # Create and run driver
    driver = MarketplaceDriver(
        provider_name,
        provider_partition_name,
        source_table_schemas,
        output_table_names_to_schemas,
        date_input,
        end_to_end_test,
        output_to_transform_path=False,
        vdr_feed_id=10,
        use_ref_gen_values=True,
        unload_partition_count=20,
        load_date_explode=False,
        restricted_private_source=True
    )

    conf_parameters = {
        'spark.default.parallelism': 4000,
        'spark.sql.shuffle.partitions': 4000,
        'spark.executor.memoryOverhead': 1024,
        'spark.driver.memoryOverhead': 1024,
        'spark.driver.extraJavaOptions': '-XX:+UseG1GC',
        'spark.executor.extraJavaOptions': '-XX:+UseG1GC',
        'spark.sql.autoBroadcastJoinThreshold': 10485760,
        'spark.shuffle.sasl.timeout': 60000
    }

    driver.init_spark_context(conf_parameters=conf_parameters)
    driver.load(extra_payload_cols=['PCN', 'claimId'])

    # only 2 columns are needed from the following 2 tables.
    # Select only the columns we need, then broadcast in the sql
    logger.log('Build passthrough and plainout refernce tables')
    driver.spark.sql('select PCN as pcn, UPPER(claimId) as claimid from matching_payload group by 1, 2')\
        .createOrReplaceTempView('pas_tiny')
    driver.spark.sql('select patient_gender, UPPER(claim_number) as claim_number from plainout group by 1, 2')\
        .createOrReplaceTempView('pln_tiny')

    logger.log('Start transform')
    driver.transform()
    driver.save_to_disk()
    driver.log_run()
    driver.stop_spark()
    driver.copy_to_output_path()
