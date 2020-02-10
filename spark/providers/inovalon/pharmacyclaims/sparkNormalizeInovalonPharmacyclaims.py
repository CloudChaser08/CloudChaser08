import argparse
import spark.providers.inovalon.pharmacyclaims.transactional_schemas as source_table_schemas
from spark.common.marketplace_driver import MarketplaceDriver
from spark.common.pharmacyclaims_common_model import schemas as medicalclaims_schemas
from pyspark.sql.functions import lit
from pyspark.sql.types import StructField, StringType, StructType

if __name__ == "__main__":

    # ------------------------ Provider specific configuration -----------------------
    provider_name = 'inovalon'
    versioned_schema = medicalclaims_schemas['schema_v11']
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
        output_to_transform_path=True
    )
    driver.init_spark_context()

    df = driver.spark.read.parquet(
        driver.output_path + 'pharmacyclaims/2018-11-26/part_provider=inovalon')
    df = df.withColumn("part_provider", lit("inovalon"))
    df.createOrReplaceTempView('_pharmacyclaims_nb')

    driver.run()
