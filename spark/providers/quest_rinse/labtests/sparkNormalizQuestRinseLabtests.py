import argparse
import spark.providers.quest_rinse.labtests.transactional_schemas as source_table_schemas
from spark.common.marketplace_driver import MarketplaceDriver
from spark.common.lab_common_model import schemas as labtests_schemas
import spark.helpers.file_utils as file_utils
import re
import spark.helpers.constants as constants
import spark.helpers.external_table_loader as external_table_loader
import spark.helpers.normalized_records_unloader as normalized_records_unloader
import spark.common.utility.logger as logger
from spark.common.utility.run_recorder import RunRecorder

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

    driver.init_spark_context()
    logger.log('Loading external table: ref_geo_state')
    external_table_loader.load_analytics_db_table(
        driver.sql_context, 'dw', 'ref_geo_state', 'ref_geo_state'
    )
    driver.spark.table('ref_geo_state').cache().createOrReplaceTempView('ref_geo_state')
    driver.load()
    driver.transform()

    logger.log('Saving data to the local file system')
    # This data goes right to the provider. They want the data in parquet without
    # column partitions.
    delivery_date = date_input.replace('-', '')
    delivery_path='s3://salusv/tmp/questrinse/{}/'.format(delivery_date)
    hdfs_output_path = constants.hdfs_staging_dir
    df = driver.spark.table('labtest_quest_rinse_census_final')
    file_utils.clean_up_output_hdfs(hdfs_output_path)
    df.repartition(100).write.parquet(hdfs_output_path, compression='gzip', mode='append')

    driver.log_run()
    driver.stop_spark()

    logger.log("Renaming files")
    output_file_name_template = '{}_response_{{}}'.format(delivery_date)
    for filename in [f for f in file_utils.list_dir_hdfs(hdfs_output_path) if f[0] != '.' and f != "_SUCCESS"]:
        part_number = re.match('''part-([0-9]+)[.-].*''', filename).group(1)
        new_name = output_file_name_template.format(str(part_number).zfill(5)) + '.gz.parquet'
        file_utils.rename_file_hdfs(hdfs_output_path + filename, hdfs_output_path + new_name)

    driver.copy_to_output_path(delivery_path)

    manifest_file_name = '{delivery_date}_manifest.tsv'.format(delivery_date=delivery_date)
    file_utils.create_manifest_file(delivery_path, manifest_file_name)
