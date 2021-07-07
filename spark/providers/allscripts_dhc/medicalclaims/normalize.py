import argparse
import spark.providers.allscripts_dhc.medicalclaims.transactional_schemas_v1 as transactions_v1
import spark.providers.allscripts_dhc.medicalclaims.transactional_schemas_v2 as transactions_v2
from spark.common.marketplace_driver import MarketplaceDriver
from spark.common.datamart.dhc.medicalclaims import schemas as dhc_medicalclaims_schemas
import spark.helpers.postprocessor as postprocessor
import spark.common.utility.logger as logger

hasDeliveryPath = True
if __name__ == "__main__":
    # ------------------------ Provider specific configuration -----------------------
    provider_name = 'allscripts_dhc'
    output_table_names_to_schemas = {
        'allscripts_veradigm_claim_normalized': dhc_medicalclaims_schemas['claims_schema_v1'],
        'allscripts_veradigm_serviceline_normalized': dhc_medicalclaims_schemas['service_lines_schema_v1']
    }
    provider_partition_name = 'allscripts'

    # ------------------------ Common for all providers -----------------------

    # Parse input arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str)
    parser.add_argument('--end_to_end_test', default=False, action='store_true')
    args = parser.parse_known_args()[0]
    date_input = args.date
    end_to_end_test = args.end_to_end_test

    # # ------------------------ Common for all providers -----------------------
    # # New layout after 2018-07-25, but we already got it once on 2018-07-24
    has_template_v2 = date_input > '2018-07-25' or date_input == '2018-07-24'

    # New layout after 2018-07-25, but we already got it once on 2018-07-24
    if has_template_v2:
        logger.log('using schema model transactions_v2')
        source_table_schemas = transactions_v2
    else:
        logger.log('using schema model with transactions_v1')
        source_table_schemas = transactions_v1

    # Create and run driver
    driver = MarketplaceDriver(
        provider_name,
        provider_partition_name,
        source_table_schemas,
        output_table_names_to_schemas,
        date_input,
        end_to_end_test,
        vdr_feed_id=26,
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
        'spark.sql.autoBroadcastJoinThreshold': 10485760
    }

    driver.init_spark_context(conf_parameters=conf_parameters)
    driver.load()

    logger.log(' -trimmify-nullify matching_payload data')
    matching_payload_df = driver.spark.table('matching_payload')
    cleaned_matching_payload_df = (
        postprocessor.compose(postprocessor.trimmify, postprocessor.nullify)(matching_payload_df))
    cleaned_matching_payload_df.createOrReplaceTempView("matching_payload")

    logger.log(' -trimmify-nullify transactions data')
    cleaned_header_df = postprocessor.nullify(postprocessor.trimmify(driver.spark.table('header')), ['NULL', 'Null', 'null', 'N/A', ''])
    cleaned_header_df.cache().createOrReplaceTempView("claim")

    cleaned_serviceline_df = postprocessor.nullify(postprocessor.trimmify(driver.spark.table('serviceline')), ['NULL', 'Null', 'null', 'N/A', ''])
    cleaned_serviceline_df.cache().createOrReplaceTempView("serviceline")

    logger.log('Start transform')
    driver.transform()
    driver.save_to_disk()
    driver.stop_spark()
    driver.log_run()
    driver.copy_to_output_path()
    logger.log('Done')
