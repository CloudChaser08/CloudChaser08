import argparse
import spark.common.utility.logger as logger
from spark.common.utility.output_type import DataType, RunType
import spark.providers.erx.pharmacyclaims.transactional_schemas_erx as source_table_schemas
from spark.common.marketplace_driver import MarketplaceDriver
from spark.common.pharmacyclaims_common_model import schemas
import spark.helpers.reject_reversal as rr
import subprocess
from datetime import date, datetime
from dateutil.relativedelta import relativedelta

if __name__ == "__main__":

    # ------------------------ Provider specific configuration -----------------------
    provider_name = 'erx'
    output_table_names_to_schemas = {
        'erx_08_final': schemas['schema_v11']
    }
    provider_partition_name = provider_name

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
        vdr_feed_id=159,
        use_ref_gen_values=True
    )

    if not end_to_end_test:
        logger.log_run_details(
            provider_name='ERX',
            data_type=DataType.PHARMACY_CLAIMS,
            data_source_transaction_path=driver.input_path,
            data_source_matching_path=driver.matching_path,
            output_path=driver.output_path,
            run_type=RunType.MARKETPLACE,
            input_date=date_input
        )

    driver.init_spark_context()
    driver.load(extra_payload_cols=['RXNumber', 'privateIdOne'])
    logger.log('Loading external tables')

    output_path = driver.output_path + 'pharmacyclaims/2018-11-26/part_provider=erx/'
    driver.spark.read.parquet(output_path).createOrReplaceTempView('_temp_pharmacyclaims_nb')
    
    driver.transform()
    driver.save_to_disk()

    if not end_to_end_test:
        tmp_path = 's3://salusv/backup/erx/pharmacyclaims/{}/'.format(date_input)
        date_part = 'part_provider=erx/part_best_date={}/'

        current_year_month = date_input[:7] + '-01'

        one_month_prior = (datetime.strptime(args.date, '%Y-%m-%d') - relativedelta(months=1)).strftime(
            '%Y-%m-01')
        two_months_prior = (
                    datetime.strptime(args.date, '%Y-%m-%d') - relativedelta(months=2)).strftime(
            '%Y-%m-01')

        for month in [current_year_month, one_month_prior, two_months_prior]:
            subprocess.check_call(
                ['aws', 's3', 'mv', '--recursive', driver.output_path + date_part.format(month),
                 tmp_path + date_part.format(month)]
            )

    driver.stop_spark()
    driver.copy_to_output_path()
