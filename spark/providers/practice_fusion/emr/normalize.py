import subprocess
import argparse
from datetime import datetime
import spark.providers.practice_fusion.emr.transactional_schemas as transactional_schemas
import spark.providers.practice_fusion.emr.transactional_schemas_v1 as transactional_schemas_v1
import spark.helpers.file_utils as file_utils
import spark.helpers.external_table_loader as external_table_loader
from spark.common.marketplace_driver import MarketplaceDriver
from spark.common.emr.clinical_observation import schemas as clinical_observation_schemas
from spark.common.emr.diagnosis import schemas as diagnosis_schemas
from spark.common.emr.encounter import schemas as encounter_schemas
from spark.common.emr.medication import schemas as medication_schemas
from spark.common.emr.procedure import schemas as procedure_schemas
from spark.common.emr.lab_test import schemas as lab_test_schema
import spark.common.utility.logger as logger
import spark.helpers.postprocessor as postprocessor

v_cutoff_date = '2020-10-01'
OPP_DW_PATH = "s3://salusv/opp_1186_warehouse/parquet/emr/2019-04-17/"
OPP_BACKUP_PATH = "s3://salusv/backup/opp_1186_warehouse/parquet/emr/2019-04-17/backup_file_date={}/"


def run(date_input, end_to_end_test=False, test=False, spark=None, runner=None):
    logger.log("Practice Fusion EMR Normalization ")
    # ------------------------ Provider specific configuration -----------------------
    provider_name = 'practice_fusion'
    output_table_names_to_schemas = {
        'practice_fusion_emr_norm_emr_clin_obsn': clinical_observation_schemas['schema_v9'],
        'practice_fusion_emr_norm_emr_diag': diagnosis_schemas['schema_v8'],
        'practice_fusion_emr_norm_emr_enc': encounter_schemas['schema_v8'],
        'practice_fusion_emr_norm_emr_medctn': medication_schemas['schema_v9'],
        'practice_fusion_emr_norm_emr_proc': procedure_schemas['schema_v10'],
        'practice_fusion_emr_norm_emr_lab_test': lab_test_schema['schema_v1']
    }
    provider_partition_name = '136'

    # New layout after 2020-10-01 (LABORDER supplied as LAB_RESULT name)
    has_template_v1 = \
        datetime.strptime(date_input, '%Y-%m-%d').date() >= datetime.strptime(v_cutoff_date, '%Y-%m-%d').date()
    if has_template_v1:
        logger.log('using new schema model with labtest schema')
        source_table_schemas = transactional_schemas_v1
    else:
        logger.log('using old schema model with laborder schema')
        source_table_schemas = transactional_schemas

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
        vdr_feed_id=136,
        output_to_transform_path=False
    )

    # init
    conf_parameters = {
        'spark.default.parallelism': 3000,
        'spark.sql.shuffle.partitions': 3000,
        'spark.executor.memoryOverhead': 1024,
        'spark.driver.memoryOverhead': 1024,
        'spark.sql.autoBroadcastJoinThreshold': 209715200
    }

    if not driver.output_to_transform_path:
        driver.output_path = OPP_DW_PATH

    if not test:
        driver.init_spark_context(conf_parameters=conf_parameters)
    else:
        driver.spark = spark
        driver.runner = runner

    logger.log('Loading external table: load_ref_gen_ref')
    external_table_loader.load_ref_gen_ref(runner.sqlContext)
    v_sql = """SELECT * FROM ref_gen_ref WHERE gen_ref_domn_nm = 'practice_fusion_emr.vitals'"""
    driver.spark.sql(v_sql).createOrReplaceTempView('ref_gen_ref_domn_nm_pf_emr_vitals')

    script_path = __file__
    if test:
        driver.input_path = file_utils.get_abs_path(
            script_path, '../../../test/providers/practice_fusion/emr/resources/input/'
        ) + '/'
        driver.matching_path = file_utils.get_abs_path(
            script_path, '../../../test/providers/practice_fusion/emr/resources/matching/'
        ) + '/'
    elif end_to_end_test:
        driver.input_path = 's3://salusv/testing/dewey/airflow/e2e/practice_fusion/out/{}/'.format(
            date_input.replace('-', '/')
        )
        driver.matching_path = 's3://salusv/testing/dewey/airflow/e2e/practice_fusion/payload/{}/'.format(
            date_input.replace('-', '/')
        )
        driver.output_path = 's3://salusv/testing/dewey/airflow/e2e/practice_fusion/spark-output-3/'

    driver.load()
    matching_payload_df = driver.spark.table('matching_payload')
    cleaned_matching_payload_df = (
        postprocessor.compose(postprocessor.trimmify, postprocessor.nullify)(matching_payload_df))
    cleaned_matching_payload_df.createOrReplaceTempView("matching_payload")

    logger.log('Apply custom nullify trimmify')
    for table in driver.source_table_schema.TABLE_CONF:
        postprocessor.nullify(
            postprocessor.trimmify(driver.spark.table(table))
            , ['NULL', 'Null', 'null', 'unknown', 'Unknown', 'UNKNOWN', '19000101', '']).createOrReplaceTempView(table)

    if has_template_v1:
        driver.spark.table('lab_result').createOrReplaceTempView('laborder')

    driver.transform()
    if not test:
        driver.save_to_disk()
        driver.log_run()
        driver.stop_spark()
        logger.log('Backup existing data before swap new data')
        backup_path = OPP_BACKUP_PATH.format(datetime.now().strftime('%Y-%m-%d-%H'))
        subprocess.check_output(['aws', 's3', 'rm', '--recursive', backup_path])
        subprocess.check_call(['aws', 's3', 'mv', '--recursive', driver.output_path, backup_path])
        logger.log('Backup completed: ' + backup_path)
        driver.copy_to_output_path(driver.output_path)
    logger.log('Done')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str)
    parser.add_argument('--end_to_end_test', default=False, action='store_true')
    args = parser.parse_known_args()[0]
    run(args.date, args.end_to_end_test)
