import argparse
from spark.helpers.normalized_records_unloader import distcp
import subprocess
from math import ceil
from spark.common.utility import logger
from spark.common.marketplace_driver import MarketplaceDriver
from spark.common.enrollment import schemas as enrollment_schemas
import spark.providers.express_scripts.enrollmentrecords.transactional_schemas as source_table_schemas
import spark.helpers.postprocessor as postprocessor
import spark.helpers.payload_loader as payload_loader
import spark.helpers.file_utils as file_utils
import spark.helpers.hdfs_utils as hdfs_utils
import spark.helpers.s3_utils as s3_utils

S3_EXPRESS_SCRIPTS_RX_MATCHING = 's3://salusv/matching/payload/pharmacyclaims/express_scripts/'
S3_REF_PHI = 's3://salusv/reference/express_scripts_phi/'
S3_REF_PHI_BACKUP = 's3://salusv/backup/reference/express_scripts_phi/date_input={date_input}/'
LOCAL_REF_PHI = '/local_phi/'
PARQUET_FILE_SIZE = 1024 * 1024 * 250


def run(date_input, end_to_end_test=False, test=False, spark=None, runner=None):
    logger.log(" -esi-enrollment: this normalization ")
    data_set_filename = '10130X001_HV_RX_ENROLLMENT_D{}.txt'.format(date_input.replace('-', ''))

    # ------------------------ Provider specific configuration -----------------------
    provider_name = 'express_scripts'
    schema = enrollment_schemas['schema_v4']
    output_table_names_to_schemas = {
        'esi_norm_final': schema
    }
    provider_partition_name = 'express_scripts'
    # ------------------------ Common for all providers -----------------------

    # Create and run driver
    driver = MarketplaceDriver(
        provider_name,
        provider_partition_name,
        source_table_schemas,
        output_table_names_to_schemas,
        date_input,
        end_to_end_test,
        test=test,
        vdr_feed_id=61,
        load_date_explode=False,
        use_ref_gen_values=True,
        output_to_transform_path=False
    )

    conf_parameters = {
        'spark.executor.memoryOverhead': 1024,
        'spark.driver.memoryOverhead': 1024
    }

    logger.log(' -Setting up input/output paths')
    date_path = date_input.replace('-', '/')

    if not test:
        driver.init_spark_context(conf_parameters=conf_parameters)
    else:
        driver.spark = spark
        driver.runner = runner

    script_path = __file__
    new_phi_path = ""
    ref_phi_path = ""
    this_input_path = driver.input_path
    this_matching_path = driver.matching_path
    if test:
        driver.input_path = file_utils.get_abs_path(
            script_path,
            '../../../test/providers/express_scripts/enrollmentrecords/resources/input/'
        ) + '/'
        driver.matching_path = file_utils.get_abs_path(
            script_path,
            '../../../test/providers/express_scripts/enrollmentrecords/resources/matching/'
        ) + '/'
        new_phi_path = file_utils.get_abs_path(
            script_path,
            '../../../test/providers/express_scripts/enrollmentrecords/resources/new_phi/'
        ) + '/'
        ref_phi_path = file_utils.get_abs_path(
            script_path,
            '../../../test/providers/express_scripts/enrollmentrecords/resources/ref_phi/'
        ) + '/'
        local_phi_path = '/tmp' + LOCAL_REF_PHI
    else:
        new_phi_path = S3_EXPRESS_SCRIPTS_RX_MATCHING + date_path + '/'
        ref_phi_path = S3_REF_PHI
        local_phi_path = 'hdfs://' + LOCAL_REF_PHI
        driver.input_path = s3_utils.get_list_of_2c_subdir(
            this_input_path.replace(date_path + '/', ''),
            True
        )
        driver.matching_path = s3_utils.get_list_of_2c_subdir(
            this_matching_path.replace(date_path + '/', ''),
            True
        )

    logger.log(' -Setting up the local file system')
    if test:
        file_utils.clean_up_output_local(local_phi_path)
        subprocess.check_call(['mkdir', '-p', local_phi_path])
    else:
        hdfs_utils.clean_up_output_hdfs(local_phi_path)
        subprocess.check_call(['hadoop', 'fs', '-mkdir', local_phi_path])

    logger.log(' -Loading new PHI data')
    payload_loader.load(driver.runner, new_phi_path, extra_cols=['hvJoinKey', 'patientId'])
    driver.runner.run_spark_query('ALTER TABLE matching_payload RENAME TO new_phi')
    logger.log(' -trimmify-nullify new PHI data')
    new_phi_payload_df = driver.spark.table('new_phi')
    cleaned_new_phi_payload_df = (
        postprocessor.compose(postprocessor.trimmify, postprocessor.nullify)(new_phi_payload_df))
    cleaned_new_phi_payload_df.createOrReplaceTempView("new_phi")

    driver.load(extra_payload_cols=['patientId', 'hvJoinKey'])
    logger.log(' -trimmify-nullify matching_payload data')
    matching_payload_df = driver.spark.table('matching_payload')
    cleaned_matching_payload_df = (
        postprocessor.compose(postprocessor.trimmify, postprocessor.nullify)(matching_payload_df))
    cleaned_matching_payload_df.createOrReplaceTempView("matching_payload")

    logger.log(' -Loading ref_phi data')
    driver.spark.read.parquet(ref_phi_path).createOrReplaceTempView("ref_phi")

    v_combine_sql = """
    SELECT DISTINCT a.* FROM (
        SELECT hvid, zip, gender, year_of_birth, patient_id FROM ref_phi
        UNION ALL 
        SELECT hvid, threeDigitZip, gender, yearOfBirth, patientId FROM new_phi
        ) a
        WHERE patient_id is not null
    """
    logger.log(' -Writing and Loading local_phi data')
    if not test:
        repartition_cnt = file_utils.get_optimal_s3_partition_count(
            s3_path=S3_REF_PHI,
            expected_file_size=PARQUET_FILE_SIZE
        )
    else:
        repartition_cnt = 1
    logger.log(' -Repartition into {} partitions'.format(repartition_cnt))
    driver.spark.sql(v_combine_sql).repartition(repartition_cnt).write.parquet(
        local_phi_path, compression='gzip', mode='append')
    driver.spark.read.parquet(local_phi_path).createOrReplaceTempView('local_phi')

    logger.log(' -Creating helper tables')
    driver.runner.run_spark_script('../../../common/zip3_to_state.sql')

    driver.transform()
    if not test:
        driver.save_to_disk()
        driver.stop_spark()
        driver.log_run()
        driver.copy_to_output_path()
        logger.log('- Saving PHI to s3: ' + S3_REF_PHI)
        # offload reference data
        subprocess.check_call([
            'aws',
            's3',
            'mv',
            '--recursive',
            S3_REF_PHI,
            S3_REF_PHI_BACKUP.format(date_input=date_input)
        ])
        subprocess.check_call([
            'aws',
            's3',
            'rm',
            '--recursive',
            S3_REF_PHI
        ])
        distcp(
            dest=S3_REF_PHI,
            src='hdfs://' + LOCAL_REF_PHI
        )
    logger.log("Done")


if __name__ == '__main__':
    # Parse input arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str)
    parser.add_argument('--end_to_end_test', default=False, action='store_true')
    args = parser.parse_known_args()[0]
    run(args.date,  args.end_to_end_test)
