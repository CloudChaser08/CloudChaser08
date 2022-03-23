"""
tsi emr normalize
"""
import os
import argparse
import subprocess
import spark.helpers.normalized_records_unloader as normalized_records_unloader
from datetime import datetime
import spark.providers.tsi.emr.transactional_schemas as table_schemas
import spark.providers.tsi.emr.transactional_schemas_v1 as table_schemas_v1
from spark.common.marketplace_driver import MarketplaceDriver
from spark.common.utility import logger
from spark.common.emr.encounter import schemas as encounter_schemas
from spark.common.emr.diagnosis import schemas as diagnosis_schemas
from spark.common.emr.procedure import schemas as procedure_schemas
from spark.common.emr.provider_order import schemas as provider_order_schema
from spark.common.emr.medication import schemas as medication_schemas
from spark.common.emr.lab_test import schemas as lab_test_schemas
from spark.common.emr.clinical_observation import schemas as clinical_observation_schemas
import spark.helpers.external_table_loader as external_table_loader
import spark.helpers.file_utils as file_utils
import spark.helpers.hdfs_utils as hdfs_utils
import spark.helpers.s3_utils as s3_utils

CUTOFF_DATE_V1 = datetime.strptime('2022-02-14', '%Y-%m-%d')
S3_REF_TSI_CROSSWALK = 's3://salusv/reference/tsi/crosswalk_for_race/'
S3_TSI_PAYLOAD = 's3://salusv/matching/payload/emr/tsi/202*/*/*/'
S3_REF_TSI_CROSSWALK_BACKUP = 's3://salusv/backup/reference/tsi/crosswalk_for_race/date_input={' \
                              'date_input}/'
LOCAL_REF_TSI = '/local_phi/'
PARQUET_FILE_SIZE = 1024 * 1024 * 250

HAS_DELIVERY_PATH = True

if __name__ == "__main__":
    # ------------------------ Provider specific configuration -----------------------

    provider_name = 'tsi'
    output_table_names_to_schemas = {
        'tsi_emr_norm_emr_enc_final': encounter_schemas['schema_v10'],
        'tsi_emr_norm_emr_diag_final': diagnosis_schemas['schema_v10'],
        'tsi_emr_norm_emr_proc_final': procedure_schemas['schema_v12'],
        'tsi_emr_norm_emr_prov_ord_final': provider_order_schema['schema_v9'],
        'tsi_emr_norm_emr_med_ord_final': medication_schemas['schema_v11'],
        'tsi_emr_norm_emr_lab_test_final': lab_test_schemas['schema_v3'],
        'tsi_emr_norm_emr_clin_obsn_final': clinical_observation_schemas['schema_v11']
    }

    provider_partition_name = '241'

    # ------------------------ Common for all providers -----------------------

    # Parse input arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str)
    parser.add_argument('--end_to_end_test', default=False, action='store_true')
    args = parser.parse_known_args()[0]
    date_input = args.date
    end_to_end_test = args.end_to_end_test

    is_schema_v1 = datetime.strptime(date_input, '%Y-%m-%d') >= CUTOFF_DATE_V1
    if is_schema_v1:
        logger.log('Current Load schema')
        source_table_schemas = table_schemas_v1
    else:
        logger.log('Historic Load schema')
        source_table_schemas = table_schemas

    # Create and run driver
    driver = MarketplaceDriver(
        provider_name,
        provider_partition_name,
        source_table_schemas,
        output_table_names_to_schemas,
        date_input,
        end_to_end_test,
        vdr_feed_id=241,
        use_ref_gen_values=True,
        unload_partition_count=10,
        output_to_delivery_path=HAS_DELIVERY_PATH,
        output_to_transform_path=False
    )

    driver.output_path = os.path.join(driver.output_path, 'hv002854/')

    conf_parameters = {
        'spark.executor.memoryOverhead': 4096,
        'spark.driver.memoryOverhead': 4096
    }
    logger.log('Loading external table: gen_ref_whtlst')
    # init
    driver.init_spark_context(conf_parameters=conf_parameters)
    driver.load()
    external_table_loader.load_analytics_db_table(
        driver.runner.sqlContext, 'dw', 'gen_ref_whtlst', 'gen_ref_whtlst')
    driver.spark.read.json(S3_TSI_PAYLOAD).createOrReplaceTempView("tsi_payload")
    driver.transform()
    driver.save_to_disk()
    driver.stop_spark()
    driver.log_run()
    driver.copy_to_output_path()
    logger.log('Saving Crosswalk for race')
    repartition_cnt = file_utils.get_optimal_s3_partition_count(s3_path=S3_REF_TSI_CROSSWALK,
                                                                expected_file_size=PARQUET_FILE_SIZE)
    local_tsi_path = 'hdfs://' + LOCAL_REF_TSI
    hdfs_utils.clean_up_output_hdfs(local_tsi_path)
    driver.spark.table('tsi_crosswalk_race').repartition(repartition_cnt).write.parquet(
        local_tsi_path, compression='gzip', mode='append')
    subprocess.check_call([
        'aws',
        's3',
        'mv',
        '--recursive',
        S3_REF_TSI_CROSSWALK,
        S3_REF_TSI_CROSSWALK_BACKUP.format(date_input=date_input)
    ])
    normalized_records_unloader.distcp(dest=S3_REF_TSI_CROSSWALK, src='hdfs://' + LOCAL_REF_TSI)
    logger.log('Done')
