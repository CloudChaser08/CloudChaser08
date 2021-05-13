import argparse
from datetime import datetime
import spark.helpers.hdfs_utils as hdfs_utils
import spark.helpers.external_table_loader as external_table_loader
import spark.providers.pointclickcare.emr.transactional_schemas as historic_source_table_schemas
import spark.providers.pointclickcare.emr.transactional_schemas_v1 as transactions_v1
from spark.common.marketplace_driver import MarketplaceDriver
from spark.common.emr.clinical_observation import schemas as clinical_observation_schemas
from spark.common.emr.diagnosis import schemas as diagnosis_schemas
from spark.common.emr.encounter import schemas as encounter_schemas
from spark.common.emr.medication import schemas as medication_schemas
from spark.common.emr.procedure import schemas as procedure_schemas
from spark.common.emr.lab_test import schemas as lab_test_schema
import spark.common.utility.logger as logger
import spark.helpers.postprocessor as postprocessor

v_cutoff_date = '2021-02-01'
v_ref_hdfs_output_path = '/reference/'

if __name__ == "__main__":

    # ------------------------ Provider specific configuration -----------------------
    provider_name = 'pointclickcare'
    output_table_names_to_schemas = {
        'pcc_emr_norm_emr_clin_obsn': clinical_observation_schemas['schema_v11'],
        'pcc_emr_norm_emr_diag': diagnosis_schemas['schema_v10'],
        'pcc_emr_norm_emr_enc': encounter_schemas['schema_v10'],
        'pcc_emr_norm_emr_medctn': medication_schemas['schema_v11'],
        'pcc_emr_norm_emr_proc': procedure_schemas['schema_v12'],
        'pcc_emr_norm_emr_labtest': lab_test_schema['schema_v3']
    }
    provider_partition_name = '156'

    # ------------------------ Common for all providers -----------------------

    # Parse input arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str)
    parser.add_argument('--end_to_end_test', default=False, action='store_true')
    args = parser.parse_known_args()[0]
    date_input = args.date
    end_to_end_test = args.end_to_end_test

    if datetime.strptime(date_input, '%Y-%m-%d').date() < datetime.strptime(v_cutoff_date, '%Y-%m-%d').date():
        logger.log('Historic Load schema with ddid column')
        source_table_schemas = historic_source_table_schemas
    else:
        logger.log('Future Load using new schema with drugid column, and new labtest schema')
        source_table_schemas = transactions_v1

    # Create and run driver
    driver = MarketplaceDriver(
        provider_name,
        provider_partition_name,
        source_table_schemas,
        output_table_names_to_schemas,
        date_input,
        end_to_end_test,
        unload_partition_count=20,
        use_ref_gen_values=True,
        vdr_feed_id=156,
        output_to_transform_path=True
    )

    # init
    conf_parameters = {
        'spark.executor.memoryOverhead': 1024,
        'spark.driver.memoryOverhead': 1024
    }

    driver.init_spark_context(conf_parameters=conf_parameters)
    driver.load()

    logger.log('Loading external table: ref_ndc_ddid')
    external_table_loader.load_analytics_db_table(
        driver.sql_context, 'dw', 'ref_ndc_ddid', 'ref_ndc_ddid'
    )

    logger.log('Building ref_ndc_ddid reference data')
    hdfs_utils.clean_up_output_hdfs(v_ref_hdfs_output_path)
    table_name = 'ref_ndc_ddid'
    driver.spark.table(table_name).repartition(1).write.parquet(
        v_ref_hdfs_output_path + table_name, compression='gzip', mode='append')
    driver.spark.read.parquet(v_ref_hdfs_output_path + table_name).cache().createOrReplaceTempView(table_name)

    logger.log('Apply custom nullify trimmify')
    for table in driver.source_table_schema.TABLE_CONF:
        postprocessor.nullify(
            postprocessor.trimmify(driver.spark.table(table))
            , ['NULL', 'Null', 'null', 'unknown', 'Unknown', 'UNKNOWN', '19000101', '']).createOrReplaceTempView(table)

    driver.transform()
    driver.save_to_disk()
    driver.stop_spark()
    driver.log_run()
    driver.copy_to_output_path()
    hdfs_utils.clean_up_output_hdfs(v_ref_hdfs_output_path)
    logger.log('Done')



