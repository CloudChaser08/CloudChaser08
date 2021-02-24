import os
import argparse
import subprocess
import spark.providers.inovalon.pharmacyclaims.transactional_schemas_v1 as historic_schemas
import spark.providers.inovalon.pharmacyclaims.transactional_schemas_v2 as jan_feb_2020_schemas
import spark.providers.inovalon.pharmacyclaims.transactional_schemas_v3 as mar_2020_schemas
import spark.providers.inovalon.pharmacyclaims.transactional_schemas_v4 as full_hist_restate_schemas

from pyspark.sql.types import StringType
from pyspark.sql.functions import lit
from spark.common.marketplace_driver import MarketplaceDriver
from spark.common.pharmacyclaims import schemas as pharmacyclaims_schema
import spark.helpers.postprocessor as postprocessor
import spark.common.utility.logger as logger
from datetime import datetime
from dateutil.relativedelta import relativedelta

v_cutoff_date = '2021-02-01'

if __name__ == "__main__":
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
    elif datetime.strptime(date_input, '%Y-%m-%d').date() < datetime.strptime(v_cutoff_date, '%Y-%m-%d').date():
        logger.log('Using the historic schema (v1)')
        source_table_schema = historic_schemas
    else:
        logger.log('Using the future restate schema (v4)')
        source_table_schema = full_hist_restate_schemas

    # Create and run driver
    driver = MarketplaceDriver(
        provider_name,
        provider_partition_name,
        source_table_schema,
        output_table_names_to_schemas,
        date_input,
        end_to_end_test,
        load_date_explode=False,
        unload_partition_count=40,
        vdr_feed_id=177,
        use_ref_gen_values=True,
        output_to_transform_path=False
    )

    conf_parameters = {
        'spark.executor.memoryOverhead': 4096,
        'spark.driver.memoryOverhead': 4096,
        'spark.driver.extraJavaOptions': '-XX:+UseG1GC',
        'spark.executor.extraJavaOptions': '-XX:+UseG1GC',
        'spark.task.maxFailures': 8,
        'spark.max.executor.failures': 800,
        'spark.sql.autoBroadcastJoinThreshold': 10485760
    }

    driver.init_spark_context(conf_parameters=conf_parameters)
    logger.log('Loading external tables')
    output_path = os.path.join(driver.output_path, versioned_schema.output_directory, 'part_provider=inovalon/')
    driver.spark.read.parquet(output_path).createOrReplaceTempView('_temp_pharmacyclaims_nb')

    driver.load()

    matching_payload_df = driver.spark.table('matching_payload')
    cleaned_matching_payload_df = (
        postprocessor.compose(postprocessor.trimmify, postprocessor.nullify)(matching_payload_df))
    cleaned_matching_payload_df.createOrReplaceTempView("matching_payload")

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

    logger.log('Backup historical data')
    if end_to_end_test:
        tmp_path = 's3://salusv/testing/dewey/airflow/e2e/inovalon/pharmacyclaims/backup/'
    else:
        tmp_path = 's3://salusv/backup/inovalon/pharmacyclaims/{}/'.format(args.date)
    date_part = 'part_provider=inovalon/part_best_date={}/'

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
    logger.log('Done')