import argparse
import spark.providers.waystar_dhc.medicalclaims.transactional_schemas as source_table_schemas
from spark.common.marketplace_driver import MarketplaceDriver
import spark.helpers.records_loader as records_loader
from spark.common.datamart.dhc.medicalclaims import schemas as dhc_medicalclaims_schemas
import spark.helpers.postprocessor as postprocessor
import spark.common.utility.logger as logger

augment_path = 's3://salusv/incoming/medicalclaims/waystar/2018/08/31/augment/'
hasDeliveryPath = True
if __name__ == "__main__":
    # ------------------------ Provider specific configuration -----------------------
    provider_name = 'waystar_dhc'
    output_table_names_to_schemas = {
        'waystar_claim_normalized': dhc_medicalclaims_schemas['claims_schema_v1'],
        'waystar_serviceline_normalized': dhc_medicalclaims_schemas['service_lines_schema_v1']
    }
    provider_partition_name = 'navicure'

    # ------------------------ Common for all providers -----------------------

    # Parse input arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str)
    parser.add_argument('--end_to_end_test', default=False, action='store_true')
    args = parser.parse_known_args()[0]
    date_input = args.date
    end_to_end_test = args.end_to_end_test

    # # ------------------------ Common for all providers -----------------------

    # Create and run driver
    driver = MarketplaceDriver(
        provider_name,
        provider_partition_name,
        source_table_schemas,
        output_table_names_to_schemas,
        date_input,
        end_to_end_test,
        vdr_feed_id=24,
        use_ref_gen_values=True,
        unload_partition_count=20,
        load_date_explode=False,
        output_to_delivery_path=hasDeliveryPath,
        output_to_transform_path=True
    )

    conf_parameters = {
        'spark.default.parallelism': 200,
        'spark.sql.shuffle.partitions': 200,
        'spark.executor.memoryOverhead': 1024,
        'spark.driver.memoryOverhead': 1024,
        'spark.sql.autoBroadcastJoinThreshold': 5242880
    }

    driver.init_spark_context(conf_parameters=conf_parameters)
    driver.load(extra_payload_cols=['pcn'])

    logger.log(' -load augment data')
    augment_df = records_loader.load(
        driver.runner, augment_path, columns=['instanceid', 'accounttype'],
        file_type='csv', header=True
    )
    augment_df.createOrReplaceTempView('waystar_medicalclaims_augment_comb')

    logger.log(' -trimmify-nullify matching_payload data')
    matching_payload_df = driver.spark.table('matching_payload')
    cleaned_matching_payload_df = (
        postprocessor.compose(postprocessor.trimmify, postprocessor.nullify)(matching_payload_df))
    cleaned_matching_payload_df.createOrReplaceTempView("matching_payload")

    logger.log('Start transform')
    driver.transform()
    driver.save_to_disk()
    driver.stop_spark()
    driver.log_run()
    driver.copy_to_output_path()
    logger.log('Done')
