"""
erx normalize
"""
import os
import argparse
import spark.common.utility.logger as logger
from spark.common.utility.output_type import DataType, RunType
import spark.providers.erx.pharmacyclaims.transactional_schemas_erx as source_table_schemas
from spark.common.marketplace_driver import MarketplaceDriver
from spark.common.pharmacyclaims import schemas
# import spark.helpers.reject_reversal as rr
# from datetime import date, datetime
# from dateutil.relativedelta import relativedelta

if __name__ == "__main__":

    # ------------------------ Provider specific configuration -----------------------
    provider_name = 'erx'
    schema = schemas['schema_v11']
    output_table_names_to_schemas = {
        'erx_08_final': schema
    }
    provider_partition_name = provider_name

    # ------------------------ Common for all providers -----------------------

    # Parse input arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str)
    parser.add_argument('--end_to_end_test', default=False, action='store_true')
    args = parser.parse_known_args()[0]
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
        unload_partition_count=1,
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

    output_path = os.path.join(driver.output_path, schema.output_directory, 'part_provider=erx/')
    driver.spark.read.parquet(output_path).createOrReplaceTempView('_temp_pharmacyclaims_nb')

    driver.transform()
    driver.save_to_disk()
    driver.stop_spark()
    driver.copy_to_output_path()
