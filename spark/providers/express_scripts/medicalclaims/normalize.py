import argparse
import subprocess
from spark.common.utility import logger
from spark.common.marketplace_driver import MarketplaceDriver
from spark.common.medicalclaims import schemas as medicalclaims_schemas
import spark.providers.express_scripts.medicalclaims.transactional_schemas as source_table_schemas
import spark.helpers.postprocessor as postprocessor
import spark.helpers.file_utils as file_utils
from spark.helpers import normalized_records_unloader

S3A_REF_PHI = 's3a://salusv/reference/express_scripts_phi/'
S3_UNMATCHED_REFERENCE = 's3://salusv/reference/express_scripts_unmatched/'
LOCAL_UNMATCHED = '/unmatched/'

if __name__ == "__main__":

    # ------------------------ Provider specific configuration -----------------------
    provider_name = 'express_scripts'
    output_table_names_to_schemas = {
        'esi_norm_final_matched': medicalclaims_schemas['schema_v9']
    }
    provider_partition_name = 'express_scripts'

    # ------------------------ Common for all providers -----------------------

    # Parse input arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str)
    # RX and DX data does not come in at the same time.
    # This argument allows you to point to the correct Rx PHI date
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
        unload_partition_count=10,
        vdr_feed_id=155,
        load_date_explode=False,
        use_ref_gen_values=True,
        output_to_transform_path=False
    )

    conf_parameters = {
        'spark.executor.memoryOverhead': 1024,
        'spark.driver.memoryOverhead': 1024
    }

    logger.log(' -Setting up input/output paths')
    date_path = date_input.replace('-', '/')
    file_utils.clean_up_output_hdfs(LOCAL_UNMATCHED)
    subprocess.check_call(['hadoop', 'fs', '-mkdir', LOCAL_UNMATCHED])

    driver.init_spark_context(conf_parameters=conf_parameters)

    driver.load()
    logger.log(' -trimmify-nullify matching_payload data')
    matching_payload_df = driver.spark.table('matching_payload')
    cleaned_matching_payload_df = (
        postprocessor.compose(postprocessor.trimmify, postprocessor.nullify)(matching_payload_df))
    cleaned_matching_payload_df.createOrReplaceTempView("matching_payload")

    logger.log('Load Rx payload reference location')
    driver.spark.read.parquet(S3A_REF_PHI).createOrReplaceTempView('rx_payloads')

    driver.transform()
    driver.save_to_disk()
    logger.log('Save unmatched reference records to: ' + LOCAL_UNMATCHED)
    driver.spark.table('esi_norm_final_unmatched').repartition(2).write.parquet(
        LOCAL_UNMATCHED, partitionBy='part_best_date', compression='gzip', mode='overwrite')
    driver.log_run()
    driver.stop_spark()
    driver.copy_to_output_path()
    if not end_to_end_test:
        logger.log('Write the unmatched reference data to s3: ' + S3_UNMATCHED_REFERENCE)
        normalized_records_unloader.distcp(S3_UNMATCHED_REFERENCE, LOCAL_UNMATCHED)
    logger.log("Done")
