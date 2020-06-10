import argparse
import spark.providers.inovalon.pharmacyclaims.transactional_schemas_v1 as historic_schemas
import spark.providers.inovalon.pharmacyclaims.transactional_schemas_v2 as jan_feb_2020_schemas
import spark.providers.inovalon.pharmacyclaims.transactional_schemas_v3 as mar_2020_schemas

from pyspark.sql.types import StringType
from pyspark.sql.functions import lit
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

    # the vendor sent a different schema for the following dates
    is_schema_v2 = date_input in ['2020-03-03', '2020-03-04']
    is_schema_v3 = date_input == '2020-03-25'

    if is_schema_v2:
        logger.log('Using the Jan/Feb 2020 refresh schema (v2)')
        source_table_schema = jan_feb_2020_schemas
    elif is_schema_v3:
        logger.log('Using the Mar 2020 refresh schema (v3)')
        source_table_schema = mar_2020_schemas
    else:
        logger.log('Using the historic schema (v1)')
        source_table_schema = historic_schemas

    # Create and run driver
    driver = MarketplaceDriver(
        provider_name,
        provider_partition_name,
        source_table_schema,
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

    driver.init_spark_context()
    driver.load()

    if is_schema_v2:
        logger.log('Adding missing Jan/Feb 2020 columns')
        rxc = driver.spark.table('rxc')
        rxc = rxc.withColumn('billedamount', lit(None).cast(StringType())) \
            .withColumn('allowedamount', lit(None).cast(StringType())) \
            .withColumn('copayamount', lit(None).cast(StringType())) \
            .withColumn('costamount', lit(None).cast(StringType())) \
            .withColumn('paidamount', lit(None).cast(StringType()))
        rxc.createOrReplaceTempView('rxc')
    elif is_schema_v3:
        logger.log('Adding missing Mar 2020 columns')
        rxc = driver.spark.table('rxc')
        rxc = rxc.withColumn('billedamount', lit(None).cast(StringType())) \
            .withColumn('allowedamount', lit(None).cast(StringType())) \
            .withColumn('copayamount', lit(None).cast(StringType())) \
            .withColumn('costamount', lit(None).cast(StringType()))
        rxc.createOrReplaceTempView('rxc')

    driver.transform()
    driver.save_to_disk()
    driver.log_run()
    driver.stop_spark()
    driver.copy_to_output_path()
