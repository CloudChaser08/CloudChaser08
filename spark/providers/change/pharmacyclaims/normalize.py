"""
change  normalize
"""
import os
import subprocess
import argparse
from datetime import datetime
import spark.providers.change.pharmacyclaims.transactional_schemas as source_table_schemas
import spark.common.utility.logger as logger
from spark.common.marketplace_driver import MarketplaceDriver
from spark.common.pharmacyclaims import schemas as pharma_schemas
from dateutil.relativedelta import relativedelta
import spark.helpers.constants as constants
import spark.helpers.normalized_records_unloader as normalized_records_unloader
import spark.helpers.hdfs_utils as hdfs_utils


def run(date_input, end_to_end_test=False, no_daily_load=False, test=False, spark=None, runner=None):
    # ------------------------ Provider specific configuration -----------------------
    provider_name = 'change'
    provider_partition_name = 'emdeon'
    opportunity_id = 'definitive_hv002886'
    CAP_NBR_OF_DAYS = 1
    daily_date_partition_column = 'created'

    schema = pharma_schemas['schema_v11']
    output_table_names_to_schemas = {
        'change_rx_05_norm_final': schema
    }

    # ------------------------ Common for all providers -----------------------

    # Create and run driver
    driver = MarketplaceDriver(
        provider_name,
        provider_partition_name,
        source_table_schemas,
        output_table_names_to_schemas,
        date_input,
        end_to_end_test,
        vdr_feed_id=11,
        load_date_explode=False,
        use_ref_gen_values=True,
        output_to_transform_path=False,
        unload_partition_count=30,
        restricted_private_source=True
    )

    daily_staging_dir = constants.hdfs_staging_dir + 'daily/'
    provider_output_directory = '{}/part_provider={}/'.format(schema.output_directory, provider_partition_name)
    daily_output_path = os.path.join(driver.output_path.replace('restricted', 'datamart'), opportunity_id + '/daily/')

    # ------------------------ Provider specific run sequence -----------------------
    # # init
    conf_parameters = {
        'spark.default.parallelism': 2000,
        'spark.sql.shuffle.partitions': 2000,
        'spark.executor.memoryOverhead': 2048,
        'spark.driver.memoryOverhead': 2048,
        'spark.driver.extraJavaOptions': '-XX:+UseG1GC',
        'spark.executor.extraJavaOptions': '-XX:+UseG1GC',
        'spark.sql.autoBroadcastJoinThreshold': 104857600,
    }

    driver.init_spark_context(conf_parameters=conf_parameters)

    logger.log('Loading previous history')
    driver.spark.read.parquet(
        os.path.join(driver.output_path, provider_output_directory)).createOrReplaceTempView('_temp_pharmacyclaims_nb')

    driver.load()
    driver.transform(additional_variables=[['CAP_NBR_OF_DAYS', CAP_NBR_OF_DAYS, False]])
    driver.save_to_disk()

    if no_daily_load:
        logger.log(' -daily load skipped')
    else:
        logger.log(' -build daily load')
        df = driver.spark.read.parquet(constants.hdfs_staging_dir + provider_output_directory)
        daily_load_columns = \
            [clmn for clmn in df.columns if clmn != daily_date_partition_column] + [daily_date_partition_column]

        hdfs_utils.clean_up_output_hdfs(daily_staging_dir + provider_output_directory)
        df.select(daily_load_columns).repartition(driver.unload_partition_count).write.parquet(
            daily_staging_dir + provider_output_directory, mode='overwrite', compression='gzip',
            partitionBy=[daily_date_partition_column])

        # add a prefix to part file names
        normalized_records_unloader.custom_rename(
            driver.spark, daily_staging_dir + provider_output_directory, str(date_input))
        logger.log(' -daily load has been created')

    driver.stop_spark()
    driver.log_run()

    logger.log('Backup historical data')
    if end_to_end_test:
        tmp_path = 's3://salusv/testing/dewey/airflow/e2e/emdeon/pharmacyclaims/backup/'
    else:
        tmp_path = 's3://salusv/backup/emdeon/pharmacyclaims/{}/'.format(args.date)
    provider_date_part = provider_output_directory + 'part_best_date={}/'

    current_year_month = args.date[:7] + '-01'
    one_month_prior = \
        (datetime.strptime(args.date, '%Y-%m-%d') - relativedelta(months=1)).strftime('%Y-%m-01')
    two_months_prior = \
        (datetime.strptime(args.date, '%Y-%m-%d') - relativedelta(months=2)).strftime('%Y-%m-01')

    for month in [current_year_month, one_month_prior, two_months_prior]:
        normalized_records_unloader.s3distcp(src=driver.output_path + provider_date_part.format(month),
                                             dest=tmp_path + provider_date_part.format(month))
    if not no_daily_load:
        logger.log('Backup historical daily data')
        backup_path = '{}{}part_file_date={}/'.format(daily_output_path.replace('salusv', 'salusv/backup'),
                                                      provider_output_directory, date_input)
        normalized_records_unloader.s3distcp(src=daily_output_path + provider_output_directory, dest=backup_path)

        # daily delivery location should only have data from most recent run
        normalized_records_unloader.distcp(daily_output_path, src=daily_staging_dir)

    driver.copy_to_output_path()
    logger.log('Done')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str)
    parser.add_argument('--no_daily_load', default=False, action='store_true')
    parser.add_argument('--end_to_end_test', default=False, action='store_true')
    args = parser.parse_known_args()[0]
    run(args.date, args.end_to_end_test, args.no_daily_load)
