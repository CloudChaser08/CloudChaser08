"""
allscripts normalize
"""
import argparse
import spark.providers.allscripts.medicalclaims.transactions_v1 as transactions_v1
import spark.providers.allscripts.medicalclaims.transactions_v2 as transactions_v2
import spark.providers.allscripts.medicalclaims.udf as allscripts_udf

from spark.common.marketplace_driver import MarketplaceDriver
from spark.common.medicalclaims import schemas as medicalclaims_schemas
from pyspark.sql.types import ArrayType, StringType
import spark.helpers.explode as explode
from spark.helpers.s3_constants import DATAMART_PATH, E2E_DATAMART_PATH


def run(date_input, end_to_end_test=False, test=False, spark=None, runner=None):
    # ------------------------ Provider specific configuration -----------------------
    provider_name = 'allscripts'
    output_table_names_to_schemas = {
        'normalize_final': medicalclaims_schemas['schema_v6']
    }
    provider_partition_name = provider_name

    # ------------------------ Common for all providers -----------------------
    # New layout after 2018-07-25, but we already got it once on 2018-07-24
    if date_input > '2018-07-25' or date_input == '2018-07-24':
        source_table_schemas = transactions_v2
    else:
        source_table_schemas = transactions_v1

    # Create and run driver
    driver = MarketplaceDriver(
        provider_name,
        provider_partition_name,
        source_table_schemas,
        output_table_names_to_schemas,
        date_input,
        end_to_end_test,
        test=test,
        load_date_explode=False,
        vdr_feed_id=26,
        use_ref_gen_values=True,
        output_to_transform_path=False
    )

    # Placeholder to Override Spark Conf. properties (after spark launch)
    conf_parameters = {
        'spark.default.parallelism': 2000,
        'spark.sql.shuffle.partitions': 2000,
        'spark.executor.memoryOverhead': 1024,
        'spark.driver.memoryOverhead': 1024,
        'spark.driver.extraJavaOptions': '-XX:+UseG1GC',
        'spark.executor.extraJavaOptions': '-XX:+UseG1GC',
        'spark.sql.autoBroadcastJoinThreshold': 52428800
    }

    if not test:
        driver.init_spark_context(conf_parameters=conf_parameters)
    else:
        driver.spark = spark
        driver.runner = runner

    explode.generate_exploder_table(driver.spark, 8, 'diag_exploder')
    driver.load(extra_payload_cols=['PCN'])
    driver.spark.udf.register(
        'linked_and_unlinked_diagnoses'
        , allscripts_udf.linked_and_unlinked_diagnoses, ArrayType(ArrayType(StringType()))
        )
    driver.transform()
    driver.save_to_disk()
    if not test:
        driver.stop_spark()
        driver.log_run()
        driver.copy_to_output_path()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str)
    parser.add_argument('--end_to_end_test', default=False, action='store_true')
    args = parser.parse_known_args()[0]
    run(args.date, args.end_to_end_test)
