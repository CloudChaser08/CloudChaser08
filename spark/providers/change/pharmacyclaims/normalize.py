import os
import spark.providers.change.pharmacyclaims.transactional_schemas as source_table_schemas
import subprocess
import argparse
import spark.common.utility.logger as logger
from spark.common.marketplace_driver import MarketplaceDriver
from spark.common.pharmacyclaims import schemas as pharma_schemas
from datetime import datetime
from dateutil.relativedelta import relativedelta

if __name__ == "__main__":

    # ------------------------ Provider specific configuration -----------------------
    provider_name = 'change'
    existing_output = 's3://salusv/warehouse/restricted/'
    schema = pharma_schemas['schema_v11']
    additional_schema = pharma_schemas['schema_v11_daily']
    output_table_names_to_schemas = {
        'change_rx_05_norm_final': schema
    }
    additional_output_schemas = {
        'change_rx_05_norm_final': additional_schema
    }
    provider_partition_name = 'emdeon'

    # ------------------------ Common for all providers -----------------------

    # Parse input arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str)
    parser.add_argument('--end_to_end_test', default=False, action='store_true')
    parser.add_argument('--additional_output_path', default=None, type=str)
    args = parser.parse_args()
    date_input = args.date
    end_to_end_test = args.end_to_end_test
    additional_output_path = args.additional_output_path
    if not additional_output_path: additional_output_schemas = None

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
        unload_partition_count=20,
        restricted_private_source=True,
        additional_output_path=additional_output_path,
        additional_output_schemas=additional_output_schemas
    )

    # ------------------------ Provider specific run sequence -----------------------
    conf_parameters = {
        'spark.executor.memoryOverhead': 1024,
        'spark.driver.memoryOverhead': 1024,
        'spark.sql.crossJoin.enabled': 'true'
    }

    driver.init_spark_context(conf_parameters=conf_parameters)

    logger.log('Loading previous history')
    driver.spark.read.parquet(os.path.join(existing_output, schema.output_directory, 'part_provider=emdeon/'))\
        .createOrReplaceTempView('_temp_pharmacyclaims_nb')

    driver.load()
    driver.transform()
    driver.save_to_disk()
    driver.log_run()

    logger.log('Backup historical data')
    if end_to_end_test:
        tmp_path = 's3://salusv/testing/dewey/airflow/e2e/emdeon/pharmacyclaims/backup/'
    else:
        tmp_path = 's3://salusv/backup/emdeon/pharmacyclaims/{}/'.format(args.date)
    date_part = 'part_provider=emdeon/part_best_date={}/'

    current_year_month = args.date[:7] + '-01'
    one_month_prior = (datetime.strptime(args.date, '%Y-%m-%d') - relativedelta(months=1)).strftime('%Y-%m-01')
    two_months_prior = (datetime.strptime(args.date, '%Y-%m-%d') - relativedelta(months=2)).strftime('%Y-%m-01')

    for month in [current_year_month, one_month_prior, two_months_prior]:
        subprocess.check_call(
            ['aws', 's3', 'mv', '--recursive',
             driver.output_path + 'pharmacyclaims/2018-11-26/' + date_part.format(month),
             tmp_path + date_part.format(month)]
        )

    driver.stop_spark()
    driver.copy_to_output_path()
    logger.log('Done')
