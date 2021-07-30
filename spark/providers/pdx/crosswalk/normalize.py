import argparse
from spark.common.marketplace_driver import MarketplaceDriver
import spark.providers.pdx.crosswalk.transactional_schemas as source_table_schemas
from spark.common.pharmacyclaims.pharmacyclaims_common_model_v7 import schema as output_schema
import spark.helpers.normalized_records_unloader as normalized_records_unloader
import spark.common.utility.logger as logger
import spark.helpers.postprocessor as postprocessor

if __name__ == "__main__":

    # ------------------------ Provider specific configuration -----------------------
    provider_name = 'pdx'
    output_table_names_to_schemas = {'pdx_crosswalk_norm': output_schema}

    provider_partition_name = '65'
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
        unload_partition_count=2,
        use_ref_gen_values=True,
        vdr_feed_id=65,
        output_to_transform_path=True
    )

    driver.input_path = 's3://salusv/incoming/crosswalk/pdx/{}/'.format(
            date_input.replace('-', '/')
        )

    # init
    conf_parameters = {
        'spark.executor.memoryOverhead': 1024,
        'spark.driver.memoryOverhead': 1024
    }

    driver.init_spark_context(conf_parameters=conf_parameters)
    driver.load(payloads=False)

    logger.log('Apply custom nullify trimmify')
    for table in driver.source_table_schema.TABLE_CONF:
        cleaned_df = postprocessor.nullify(postprocessor.trimmify(driver.spark.table(table)), ['NULL', 'Null', 'null', ''])
        cleaned_df.createOrReplaceTempView(table)

    logger.log('Running the normalization SQL scripts')

    variables = [['VDR_FILE_DT', str(date_input), False],
                     ['AVAILABLE_START_DATE', driver.available_start_date, False],
                     ['EARLIEST_SERVICE_DATE', driver.earliest_service_date, False],
                     ['EARLIEST_DIAGNOSIS_DATE', driver.earliest_diagnosis_date, False]]

    output_df = driver.runner.run_all_spark_scripts(variables, directory_path=driver.provider_directory_path,
                                    count_transform_sql=driver.count_transform_sql)

    normalized_records_unloader.unload(
            driver.spark,
            driver.runner,
            output_df,
            'part_best_date',
            str(date_input),
            driver.provider_partition_name,
            substr_date_part=False,
            columns=output_df.columns,
            date_partition_name='pdx_crosswalk_norm.part_best_date',
            provider_partition_name='pdx_crosswalk_norm.part_provider',
            distribution_key='rx_number',
            staging_subdir='crosswalk/2021-07-01',
            partition_by_part_file_date=driver.output_to_transform_path or driver.output_to_delivery_path,
            unload_partition_count=driver.unload_partition_count,
            test_dir=(driver.output_path if driver.test else None)
        )

    driver.stop_spark()
    driver.log_run()
    driver.copy_to_output_path()
    logger.log('Done')



