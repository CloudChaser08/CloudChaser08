"""
labcorp normalize
"""
import argparse
from datetime import datetime
import spark.providers.labcorp.labtests.transactional_schemas as transactions_v1
from spark.common.marketplace_driver import MarketplaceDriver
from spark.common.lab_common_model import schemas as labtest_schemas
import spark.common.utility.logger as logger
import spark.helpers.hdfs_utils as hdfs_utils

xwalk_loc = 's3://salusv/reference/labcorp_abbr_xwalk/created_date=2022-03-09/'
tmp_loc = '/tmp/lab/'

if __name__ == "__main__":

    # ------------------------ Provider specific configuration -----------------------
    provider_name = 'labcorp'
    output_table_names_to_schemas = {
        'labtest_labcorp_269_final': labtest_schemas['schema_v9']
    }
    provider_partition_name = 'labcorp'

    # ------------------------ Common for all providers -----------------------

    # Parse input arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str)
    parser.add_argument('--end_to_end_test', default=False, action='store_true')
    args = parser.parse_known_args()[0]
    date_input = args.date
    end_to_end_test = args.end_to_end_test

    source_table_schemas = transactions_v1

    # Create and run driver
    driver = MarketplaceDriver(
        provider_name,
        provider_partition_name,
        source_table_schemas,
        output_table_names_to_schemas,
        date_input,
        end_to_end_test,
        use_ref_gen_values=True,
        vdr_feed_id=269,
        unload_partition_count=8,
        output_to_transform_path=False
    )

    conf_parameters = {
        'spark.default.parallelism': 2000,
        'spark.sql.shuffle.partitions': 2000,
        'spark.executor.memoryOverhead': 4096,
        'spark.driver.memoryOverhead': 4096,
        'spark.driver.extraJavaOptions': '-XX:+UseG1GC',
        'spark.executor.extraJavaOptions': '-XX:+UseG1GC',
        'spark.sql.autoBroadcastJoinThreshold': 26214400
    }
    hdfs_utils.clean_up_output_hdfs(tmp_loc)
    driver.init_spark_context(conf_parameters=conf_parameters)
    """
    labecorp is using labcorp_abbr_xwalk external table
    """
    ref_table = 'labcorp_abbr_xwalk'
    driver.spark.read.parquet(xwalk_loc) \
        .distinct().repartition(2) \
        .write.parquet(tmp_loc + ref_table + '/', compression='gzip', mode='overwrite')
    driver.spark.read.parquet(tmp_loc + ref_table + '/').createOrReplaceTempView(ref_table)

    driver.load()
    driver.transform()
    driver.save_to_disk()
    driver.stop_spark()
    driver.log_run()
    driver.copy_to_output_path()
    hdfs_utils.clean_up_output_hdfs(tmp_loc)
    logger.log('Done')
