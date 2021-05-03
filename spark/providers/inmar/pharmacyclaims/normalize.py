import os
import spark.providers.inmar.pharmacyclaims.transactional_schemas as source_table_schemas
import subprocess
import argparse
from spark.common.utility.output_type import DataType, RunType
import spark.common.utility.logger as logger
from spark.common.marketplace_driver import MarketplaceDriver
from spark.common.pharmacyclaims import schemas as pharma_schemas
from datetime import datetime
from dateutil.relativedelta import relativedelta


if __name__ == "__main__":

    # ------------------------ Provider specific configuration -----------------------
    provider_name = 'inmar'
    schema = pharma_schemas['schema_v11']
    output_table_names_to_schemas = {
        'inmar_05_norm_final': schema
    }
    provider_partition_name = 'inmar'

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
        vdr_feed_id=147,
        use_ref_gen_values=True
    )

    # ------------------------ Provider specific run sequence -----------------------

    # manually step through the driver to allow for custom backup process
    # 'spark.driver.maxResultSize': '10G',

    conf_parameters = {
        'spark.max.executor.failures': 800,
        'spark.task.maxFailures': 8,
        'spark.driver.extraJavaOptions': '-XX:+UseG1GC',
        'spark.executor.extraJavaOptions': '-XX:+UseG1GC',
        'spark.executor.memoryOverhead': 4096,
        'spark.driver.memoryOverhead': 4096
    }

    driver.init_spark_context(conf_parameters=conf_parameters)
    logger.log('Loading external tables')
    driver.spark.read.parquet(os.path.join(driver.output_path, schema.output_directory, 'part_provider=inmar/'))\
        .createOrReplaceTempView('_temp_pharmacyclaims_nb')
    driver.load()
    driver.transform()
    driver.save_to_disk()

    if not end_to_end_test:
        logger.log_run_details(
            provider_name='Inmar',
            data_type=DataType.PHARMACY_CLAIMS,
            data_source_transaction_path=driver.input_path,
            data_source_matching_path=driver.matching_path,
            output_path=driver.output_path,
            run_type=RunType.MARKETPLACE,
            input_date=date_input
        )
    driver.stop_spark()

    logger.log('Backup historical data')
    if end_to_end_test:
        tmp_path = 's3://salusv/testing/dewey/airflow/e2e/inmar/pharmacyclaims/backup/'
    else:
        tmp_path = 's3://salusv/backup/inmar/pharmacyclaims/{}/'.format(args.date)
    date_part = 'part_provider=inmar/part_best_date={}/'

    current_year_month = args.date[:7] + '-01'
    one_month_prior = (datetime.strptime(args.date, '%Y-%m-%d') - relativedelta(months=1)).strftime('%Y-%m-01')
    two_months_prior = (datetime.strptime(args.date, '%Y-%m-%d') - relativedelta(months=2)).strftime('%Y-%m-01')

    for month in [current_year_month, one_month_prior, two_months_prior]:
        subprocess.check_call(
            ['aws', 's3', 'mv', '--recursive',
             driver.output_path + 'pharmacyclaims/2018-11-26/' + date_part.format(month),
             tmp_path + date_part.format(month)]
        )

    driver.copy_to_output_path()
