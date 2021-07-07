import argparse
import spark.providers.change_relay_dhc.medicalclaims.transactional_schemas as source_table_schemas
from spark.common.marketplace_driver import MarketplaceDriver
from spark.common.datamart.dhc.medicalclaims import schemas as dhc_medicalclaims_schemas
import spark.helpers.postprocessor as postprocessor
import spark.common.utility.logger as logger

hasDeliveryPath = True
if __name__ == "__main__":

    # ------------------------ Provider specific configuration -----------------------
    provider_name = 'change_relay_dhc'
    output_table_names_to_schemas = {
        'change_dxrelay_claims_normalized': dhc_medicalclaims_schemas['claims_schema_v1'],
        'change_dxrelay_service_lines_normalized': dhc_medicalclaims_schemas['service_lines_schema_v1']
    }
    provider_partition_name = 'change_relay'

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
        vdr_feed_id=220,
        use_ref_gen_values=True,
        unload_partition_count=1,
        load_date_explode=False,
        output_to_delivery_path=hasDeliveryPath,
        output_to_transform_path=False
    )

    conf_parameters = {
        'spark.default.parallelism': 4000,
        'spark.sql.shuffle.partitions': 4000,
        'spark.executor.memoryOverhead': 1024,
        'spark.driver.memoryOverhead': 1024,
        'spark.driver.extraJavaOptions': '-XX:+UseG1GC',
        'spark.executor.extraJavaOptions': '-XX:+UseG1GC',
        'spark.sql.autoBroadcastJoinThreshold': 52428800
    }

    driver.init_spark_context(conf_parameters=conf_parameters)
    driver.load()
    logger.log(' -trimmify-nullify matching_payload data')
    matching_payload_df = driver.spark.table('matching_payload')
    cleaned_matching_payload_df = (
        postprocessor.compose(postprocessor.trimmify, postprocessor.nullify)(matching_payload_df))
    cleaned_matching_payload_df.createOrReplaceTempView("matching_payload")

    # only 2 columns are needed from the following 2 tables.
    # Select only the columns we need, then broadcast in the sql
    logger.log('Build plainout refernce tables')
    driver.spark.sql('select patient_gender, UPPER(claim_number) as claim_number from plainout group by 1, 2')\
        .createOrReplaceTempView('change_dxrelay_plainout')

    logger.log('Start transform')
    driver.transform()
    driver.save_to_disk()
    driver.stop_spark()
    driver.log_run()
    driver.copy_to_output_path()
    logger.log('Done')
