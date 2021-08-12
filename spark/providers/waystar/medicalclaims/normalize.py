"""
Waystar / Navicure's normalization routine
"""
import argparse
import spark.helpers.file_utils as file_utils
import spark.helpers.records_loader as records_loader
from spark.common.marketplace_driver import MarketplaceDriver
from spark.common.medicalclaims import schemas as medicalclaims_schemas
import spark.providers.waystar.medicalclaims.transactional_schemas as transactional_schemas
from spark.helpers.s3_constants import DATAMART_PATH, E2E_DATAMART_PATH

FEED_ID = '24'

def run(date_input, end_to_end_test=False, test=False, spark=None, runner=None):
    """
    Run normalization for waystar
    """
    # ------------------------ Provider specific configuration -----------------------
    provider_name = 'waystar'
    provider_partition_name = 'navicure'
    schema = medicalclaims_schemas['schema_v8']

    output_table_names_to_schemas = {
        'waystar_norm03_norm_final': schema
    }

    # ------------------------ Common for all providers -----------------------

    # Create and run driver
    driver = MarketplaceDriver(
        provider_name,
        provider_partition_name,
        transactional_schemas,
        output_table_names_to_schemas,
        date_input,
        end_to_end_test,
        test=test,
        load_date_explode=False,
        vdr_feed_id=24,
        use_ref_gen_values=True,
        output_to_transform_path=False
    )

    conf_parameters = {
        'spark.default.parallelism': 200,
        'spark.sql.shuffle.partitions': 200,
        'spark.executor.memoryOverhead': 1024,
        'spark.driver.memoryOverhead': 1024,
        'spark.sql.autoBroadcastJoinThreshold': 5242880
    }

    if not test:
        driver.init_spark_context(conf_parameters=conf_parameters)
        augment_path = 's3://salusv/incoming/medicalclaims/waystar/2018/08/31/augment/'
    else:
        driver.spark = spark
        driver.runner = runner
        
        driver.input_path = file_utils.get_abs_path(
            __file__, '../../../test/providers/waystar/medicalclaims/resources/input/'	
        ) + '/'	
        driver.matching_path = file_utils.get_abs_path(	
            __file__, '../../../test/providers/waystar/medicalclaims/resources/matching/'	
        ) + '/'

        augment_path = file_utils.get_abs_path(
            __file__, '../../../test/providers/waystar/medicalclaims/resources/augment/'
        ) + '/'



    augment_df = records_loader.load(
        driver.runner, augment_path, columns=['instanceid', 'accounttype'],
        file_type='csv', header=True
    )

    augment_df.createOrReplaceTempView('waystar_medicalclaims_augment')

    driver.load(extra_payload_cols=['pcn'])
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
