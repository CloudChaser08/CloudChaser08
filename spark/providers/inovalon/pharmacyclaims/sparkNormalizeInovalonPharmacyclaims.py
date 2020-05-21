import argparse
import spark.providers.inovalon.pharmacyclaims.transactional_schemas as source_table_schemas
from spark.common.marketplace_driver import MarketplaceDriver
from spark.common.pharmacyclaims_common_model import schemas as pharmacyclaims_schema
import spark.common.utility.logger as logger


if __name__ == "__main__":
    OUTPUT_PATH_PRODUCTION = 's3://salusv/warehouse/transformed/pharmacyclaims/2018-11-26/'

    # ------------------------ Provider specific configuration -----------------------
    provider_name = 'inovalon'
    versioned_schema = pharmacyclaims_schema['schema_v11']
    output_table_names_to_schemas = {
        'inovalon_05_norm_final': versioned_schema
    }
    provider_partition_name = 'inovalon'

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
        load_date_explode=False,
        unload_partition_count=40
    )
    driver.init_spark_context()
    logger.log('Loading external tables')
    output_path = driver.output_path + 'pharmacyclaims/2018-11-26/part_provider=inovalon/'
    driver.spark.read.parquet(output_path).createOrReplaceTempView('_temp_pharmacyclaims_nb')

    driver.run()
