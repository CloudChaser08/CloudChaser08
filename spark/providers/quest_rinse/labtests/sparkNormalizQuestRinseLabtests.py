import argparse
import spark.providers.quest_rinse.labtests.transactional_schemas as source_table_schemas
from spark.common.marketplace_driver import MarketplaceDriver
from spark.common.lab_common_model import schemas as labtests_schemas
import spark.helpers.hdfs_tools as hdfs_utils
import spark.helpers.file_utils as file_utils
import re
import spark.helpers.constants as constants
import spark.helpers.external_table_loader as external_table_loader
import spark.helpers.normalized_records_unloader as normalized_records_unloader
import spark.common.utility.logger as logger
from spark.common.utility.run_recorder import RunRecorder
from pyspark.sql.functions import col

_parquet_file_split_by_size = True
_parquet_file_split_size = 1024 * 1024 * 1024
_ref_loinc_schema = 'darch'
_ref_loinc_table = 'hv_loinc_20191204_hardcoded_final'

if __name__ == "__main__":

    # ------------------------ Provider specific configuration -----------------------
    provider_name = 'quest_rinse'
    output_table_names_to_schemas = {
        'labtest_quest_rinse_census_final': labtests_schemas['schema_v9']
    }
    provider_partition_name = 'quest_rinse'

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
        end_to_end_test,
        vdr_feed_id=187,
        use_ref_gen_values=True
    )

    # init
    conf_parameters = {
        'spark.executor.memoryOverhead': 4096,
        'spark.driver.memoryOverhead': 4096,
        'spark.driver.extraJavaOptions': '-XX:+UseG1GC',
        'spark.executor.extraJavaOptions': '-XX:+UseG1GC',
        'spark.files.maxPartitionBytes': 3221225472,
        'spark.reducer.maxBlocksInFlightPerAddress': 64,
        'spark.reducer.maxReqsInFlight': 192,
        'spark.shuffle.service.enabled': 'true',
        'spark.shuffle.sasl.timeout': 60000,
        'spark.network.timeout': '480s',
        'spark.port.maxRetries': 64,
        'spark.yarn.maxAppAttempts': 1,
        'spark.task.maxFailures': 8,
        'spark.max.executor.failures': 800
    }

    driver.init_spark_context(conf_parameters=conf_parameters)

    # ------------------------ Load Reference Tables -----------------------
    logger.log('Loading external table: ref_geo_state')
    external_table_loader.load_analytics_db_table(
        driver.sql_context, 'dw', 'ref_geo_state', 'ref_geo_state'
    )
    driver.spark.table('ref_geo_state').cache().createOrReplaceTempView('ref_geo_state')

    logger.log('Loading external table: {}.{}'.format(_ref_loinc_schema, _ref_loinc_table))
    external_table_loader.load_analytics_db_table(
        driver.sql_context, _ref_loinc_schema, _ref_loinc_table, 'loinc'
    )
    driver.spark.table('loinc').cache().createOrReplaceTempView('loinc')
    # ------------------------ End Reference Tables Load -----------------------

    driver.load()
    driver.transform()

    # This data goes right to the provider. They want the data in parquet without
    # column partitions.
    logger.log('Saving data to the local file system')
    output_table = driver.spark.table("labtest_quest_rinse_census_final")
    delivery_date = date_input.replace('-', '')
    delivery_path='s3://salusv/deliverable/questrinse/{}/'.format(delivery_date)
    hdfs_output_path = 'hdfs:///staging/'
    file_utils.clean_up_output_hdfs(hdfs_output_path)

    """
    requirement to delivery with 1 GB (1024 * 1024 * 1024) per file
    (split into 1GB files)
    """
    if _parquet_file_split_by_size:
        hdfs_output_temp_path = 'hdfs:///staging_temp/'
        file_utils.clean_up_output_hdfs(hdfs_output_temp_path)
        output_table.repartition(20).write.parquet(hdfs_output_temp_path, compression='gzip', mode='append')

        # Collect total files size
        repartition_cnt = int(round(hdfs_utils.get_hdfs_file_path_size(hdfs_output_temp_path) / _parquet_file_split_size))

        logger.log('Repartition with given specific file size.')
        driver.spark.read.parquet(hdfs_output_temp_path).repartition(repartition_cnt).write.parquet(
            hdfs_output_path, compression='gzip', mode='append')

        file_utils.clean_up_output_hdfs(hdfs_output_temp_path)
    else:
        output_table.repartition(20).write.parquet(hdfs_output_path, compression='gzip', mode='append')

    driver.log_run()

    logger.log("Renaming files")
    output_file_name_template = '{}_response_{{}}'.format(delivery_date)

    for filename in [f for f in hdfs_utils.get_files_from_hdfs_path(hdfs_output_path)
                     if f[0] != '.' and f != "_SUCCESS"]:
        part_number = re.match('''part-([0-9]+)[.-].*''', filename).group(1)
        new_name = output_file_name_template.format(str(part_number).zfill(5)) + '.gz.parquet'
        file_utils.rename_file_hdfs(hdfs_output_path + filename, hdfs_output_path + new_name)

    # Re-initialize spark in order to provide parquet row counts in manifest file
    logger.log('Creating manifest file with counts')
    manifest_file_name = '{delivery_date}_manifest.tsv'.format(delivery_date=delivery_date)
    file_utils.create_parquet_row_count_file(driver.spark, '/staging/', delivery_path, manifest_file_name, True)
    driver.stop_spark()

    driver.copy_to_output_path(output_location=delivery_path)

    # Quest doesn't want to see the _SUCCESS file that spark prints out
    logger.log('Deleting _SUCCESS file')
    file_utils.delete_success_file(delivery_path)

    logger.log('Done')
