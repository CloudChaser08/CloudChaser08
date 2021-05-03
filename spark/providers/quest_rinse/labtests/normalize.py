import argparse
import re
import spark.providers.quest_rinse.labtests.transactional_schemas as source_table_schemas
import spark.providers.quest_rinse.labtests.refbuild_loinc_delta_sql as refbuild_loinc_delta
from spark.common.marketplace_driver import MarketplaceDriver
from spark.common.lab_common_model import schemas as labtests_schemas
import spark.common.utility.logger as logger
import spark.helpers.hdfs_utils as hdfs_utils
import spark.helpers.file_utils as file_utils
import spark.helpers.external_table_loader as external_table_loader
import spark.helpers.normalized_records_unloader as normalized_records_unloader

PARQUET_FILE_SIZE = 1024 * 1024 * 1024
REFERENCE_OUTPUT_PATH = 's3://salusv/reference/questrinse/loinc_ref/'
REFERENCE_HDFS_OUTPUT_PATH = '/reference/'
REFERENCE_LOINC_DELTA = 'loinc_delta'
OUTPUT_PATH_PRODUCTION = 's3://salusv/warehouse/parquet/custom/quest_rinse/HV000838/'

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
    args = parser.parse_known_args()
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
        use_ref_gen_values=True,
        unload_partition_count=20,
        output_to_transform_path=False
    )

    # init
    conf_parameters = {
        'spark.default.parallelism': 2000,
        'spark.sql.shuffle.partitions': 2000,
        'spark.executor.memoryOverhead': 1024,
        'spark.driver.memoryOverhead': 1024,
        'spark.driver.extraJavaOptions': '-XX:+UseG1GC',
        'spark.executor.extraJavaOptions': '-XX:+UseG1GC',
        'spark.sql.autoBroadcastJoinThreshold': 52428800
    }
    delivery_date = date_input.replace('-', '')
    _batch_id_path = 'batch_id={}'.format(delivery_date)
    local_output_path = '/staging/{batch_id_path}/'.format(batch_id_path=_batch_id_path)
    delivery_path = OUTPUT_PATH_PRODUCTION
    if driver.output_to_transform_path:
        delivery_path = delivery_path.replace('parquet', 'transformed')

    driver.init_spark_context(conf_parameters=conf_parameters)

    # ------------------------ Load Reference Tables -----------------------
    logger.log('Loading external table: ref_geo_state')
    external_table_loader.load_analytics_db_table(
        driver.sql_context, 'dw', 'ref_geo_state', 'ref_geo_state'
    )
    driver.spark.table('ref_geo_state').createOrReplaceTempView('ref_geo_state')

    logger.log('Loading LOINC reference data from S3')
    driver.spark.read.parquet(REFERENCE_OUTPUT_PATH).createOrReplaceTempView('loinc')

    driver.load()
    df = driver.spark.table('order_result')
    df = df.repartition(int(
        driver.spark.sparkContext.getConf().get('spark.sql.shuffle.partitions')), 'unique_accession_id')
    df = df.cache_and_track('order_result')
    df.createOrReplaceTempView('order_result')

    logger.log('Building LOINC delta reference data')
    file_utils.clean_up_output_hdfs(REFERENCE_HDFS_OUTPUT_PATH)
    for table_conf in refbuild_loinc_delta.TABLE_CONF:
        table_name = str(table_conf['table_name'])
        logger.log("        -loading: writing {}".format(table_name))
        repart_num = 20 if table_name == 'lqrc_order_result_trans' else 1
        sql_stmnt = str(table_conf['sql_stmnt']).strip()
        if not sql_stmnt:
            continue
        driver.runner.run_spark_query(sql_stmnt, return_output=True).createOrReplaceTempView(table_name)
        if table_name == REFERENCE_LOINC_DELTA:
            driver.spark.table(table_name).repartition(repart_num).write.parquet(
                REFERENCE_HDFS_OUTPUT_PATH + table_name, compression='gzip', mode='append', partitionBy=['year'])
        else:
            driver.spark.table(table_name).repartition(repart_num).write.parquet(
                REFERENCE_HDFS_OUTPUT_PATH + table_name, compression='gzip', mode='append')
            driver.spark.read.parquet(
                REFERENCE_HDFS_OUTPUT_PATH + table_name).createOrReplaceTempView(table_name)

    if hdfs_utils.list_parquet_files(REFERENCE_HDFS_OUTPUT_PATH + REFERENCE_LOINC_DELTA)[0].strip():
        logger.log('Loading Delta LOINC reference data from HDFS')
        driver.spark.read.parquet(
            REFERENCE_HDFS_OUTPUT_PATH + REFERENCE_LOINC_DELTA).createOrReplaceTempView(REFERENCE_LOINC_DELTA)
    else:
        logger.log('No Delta LOINC reference data for this cycle')
        driver.spark.table("loinc").createOrReplaceTempView(REFERENCE_LOINC_DELTA)

    driver.transform()

    # Save to disk
    # This data goes right to the provider. They want the data in parquet without
    # column partitions.
    logger.log('Saving data to the local file system')
    output_table = driver.spark.table("labtest_quest_rinse_census_final")
    file_utils.clean_up_output_hdfs(local_output_path)
    output_table.repartition(
        driver.unload_partition_count).write.parquet(local_output_path, compression='gzip', mode='overwrite')

    # file renaming
    logger.log("Renaming files")
    output_file_name_template = '{}_response_{{}}'.format(delivery_date)
    for filename in [f for f in hdfs_utils.get_files_from_hdfs_path(local_output_path)
                     if f[0] != '.' and f != "_SUCCESS"]:
        part_number = re.match('''part-([0-9]+)[.-].*''', filename).group(1)
        new_name = output_file_name_template.format(str(part_number).zfill(5)) + '.gz.parquet'
        file_utils.rename_file_hdfs(local_output_path + filename, local_output_path + new_name)

    driver.log_run()
    driver.stop_spark()
    driver.copy_to_output_path(output_location=delivery_path)
    if hdfs_utils.list_parquet_files(REFERENCE_HDFS_OUTPUT_PATH + REFERENCE_LOINC_DELTA)[0].strip():
        logger.log("Copying reference files to: " + REFERENCE_OUTPUT_PATH)
        normalized_records_unloader.distcp(REFERENCE_OUTPUT_PATH, REFERENCE_HDFS_OUTPUT_PATH + REFERENCE_LOINC_DELTA)
    logger.log('Deleting ' + REFERENCE_HDFS_OUTPUT_PATH)
    file_utils.clean_up_output_hdfs(REFERENCE_HDFS_OUTPUT_PATH)
    logger.log('Done')
