import os
import argparse
import subprocess
import spark.providers.practice_insight.medicalclaims.transactional_schemas as source_table_schemas
from spark.common.marketplace_driver import MarketplaceDriver
from spark.common.medicalclaims import schemas as medicalclaims_schemas
import spark.common.utility.logger as logger
import spark.helpers.postprocessor as postprocessor
from datetime import datetime
from dateutil.relativedelta import relativedelta

b_run_dedup = True

if __name__ == "__main__":

    # ------------------------ Provider specific configuration -----------------------
    provider_name = 'practice_insight'
    schema = medicalclaims_schemas['schema_v1']
    output_table_names_to_schemas = {
        'practice_insight_16_norm_final': schema,
    }
    provider_partition_name = 'practice_insight'

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
        output_to_transform_path=True,
        vdr_feed_id=22,
        use_ref_gen_values=True,
        unload_partition_count=10,
        load_date_explode=True
    )

    conf_parameters = {
        'spark.default.parallelism': 2000,
        'spark.sql.shuffle.partitions': 2000,
        'spark.executor.memoryOverhead': 1024,
        'spark.driver.memoryOverhead': 1024,
        'spark.driver.extraJavaOptions': '-XX:+UseG1GC',
        'spark.executor.extraJavaOptions': '-XX:+UseG1GC',
        'spark.sql.autoBroadcastJoinThreshold': 26214400
    }

    driver.init_spark_context(conf_parameters=conf_parameters)

    logger.log('Loading external tables')
    driver.spark.read.parquet(os.path.join(driver.output_path, schema.output_directory, 'part_provider=practice_insight/')) \
        .createOrReplaceTempView('_temp_medicalclaims_nb')

    driver.load(extra_payload_cols=['claimId'])

    matching_payload_df = driver.spark.table('matching_payload')
    cleaned_matching_payload_df = (
        postprocessor.compose(postprocessor.trimmify, postprocessor.nullify)(matching_payload_df))
    cleaned_matching_payload_df.createOrReplaceTempView("matching_payload")

    logger.log('Start transform')
    driver.transform()

    logger.log('Apply custom nullify trimmify for exploded')
    diag_exploded_df = driver.spark.table('practice_insight_03_clm_diag')
    cleaned_diag_exploded_df = (
        postprocessor.compose(postprocessor.trimmify, postprocessor.nullify)(diag_exploded_df))
    cleaned_diag_exploded_df.createOrReplaceTempView("practice_insight_03_clm_diag")

    proc_exploded_df = driver.spark.table('practice_insight_05_clm_proc')
    cleaned_proc_exploded_df = (
        postprocessor.compose(postprocessor.trimmify, postprocessor.nullify)(proc_exploded_df))
    cleaned_proc_exploded_df.createOrReplaceTempView("practice_insight_05_clm_proc")

    driver.save_to_disk()
    driver.stop_spark()
    driver.log_run()

    if b_run_dedup:
        logger.log('Backup historical data')
        if end_to_end_test:
            tmp_path = 's3://salusv/testing/dewey/airflow/e2e/practice_insight/medicalclaims/backup/'
        else:
            tmp_path = 's3://salusv/backup/practice_insight/medicalclaims/{}/'.format(args.date)
        date_part = 'part_provider=practice_insight/part_best_date={}/'

        current_year_month = args.date[:7]
        one_month_prior = (datetime.strptime(args.date, '%Y-%m-%d') - relativedelta(months=1)).strftime('%Y-%m')
        two_months_prior = (datetime.strptime(args.date, '%Y-%m-%d') - relativedelta(months=2)).strftime('%Y-%m')

        for month in [current_year_month, one_month_prior, two_months_prior]:
            subprocess.check_call(
                ['aws', 's3', 'mv', '--recursive',
                 driver.output_path + 'medicalclaims/2017-02-24/' + date_part.format(month),
                 tmp_path + date_part.format(month)]
            )
    else:
        logger.log('De-dupe process disabled')
    driver.copy_to_output_path()
    logger.log('Done')