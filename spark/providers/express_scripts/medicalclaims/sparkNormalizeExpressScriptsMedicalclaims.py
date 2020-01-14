import argparse
import subprocess
import re
from spark.common.marketplace_driver import MarketplaceDriver
from spark.common.medicalclaims_common_model import schemas as medicalclaims_schemas
from spark.common.utility import logger
import spark.helpers.payload_loader as payload_loader
import spark.helpers.external_table_loader as external_table_loader
import pyspark.sql.functions as F
from spark.common.utility.output_type import RunType

S3_TRANSACTION_KEY = 'salusv/incoming/medicalclaims/express_scripts/'
S3_MATCHING_KEY = 'salusv/matching/payload/medicalclaims/express_scripts/'
S3_EXPRESS_SCRIPTS_RX_MATCHING = 's3a://salusv/matching/payload/pharmacyclaims/esi/'
S3A_REF_PHI = 's3a://salusv/reference/express_scripts_phi/'
S3_REF_PHI = 's3://salusv/reference/express_scripts_phi/'
LOCAL_REF_PHI = 'hdfs:///local_phi/'
PROVIDER_PATH = 'medicalclaims/2018-06-06/part_provider=express_scripts/'

if __name__ == "__main__":

    # ------------------------ Provider specific configuration -----------------------
    provider_name = 'express_scripts'
    output_table_names_to_schemas = {
        'esi_norm_04_final_dx': medicalclaims_schemas['schema_v9']
    }
    provider_partition_name = 'express_scripts'

    # ------------------------ Common for all providers -----------------------

    # Parse input arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str)
    parser.add_argument('--end_to_end_test', default=False, action='store_true')
    args = parser.parse_args()
    date_input = args.date
    end_to_end_test = args.end_to_end_test

    # Create and run driver
    driver = MarketplaceDriver(
        provider_name,
        provider_partition_name,
        '',
        output_table_names_to_schemas,
        date_input,
        end_to_end_test
    )
    driver.init_spark_context()
    input_date_path = date_input.replace('-', '/')

    logger.log('Setting up the local file system')
    subprocess.check_call(['hadoop', 'fs', '-rm', '-r', '-f', LOCAL_REF_PHI])
    subprocess.check_call(['hadoop', 'fs', '-mkdir', LOCAL_REF_PHI])

    logger.log('Loading data:')
    logger.log(' -Loading ref_gen_ref table')
    external_table_loader.load_ref_gen_ref(driver.runner.sqlContext)

    logger.log('- Loading Enrollment data')
    driver.runner.run_spark_script('sql_loaders/load_transactions.sql', [['input_path', driver.input_path]])
    file_cmd = ['aws', 's3', 'ls', 's3://' + S3_TRANSACTION_KEY, '--recursive']
    files = subprocess.check_output(file_cmd).decode().split("\n")
    found_dates = [re.findall('20[0-9]{2}/../..', x)[0] for x in
                   [x for x in files if re.search('20[0-9]{2}/../..', x)]]
    found_dates = set(found_dates)
    for found_date in found_dates:
        location = 's3a://' + S3_TRANSACTION_KEY + found_date + '/'
        driver.runner.run_spark_query(
            """ALTER TABLE transactions
            ADD PARTITION (part_date_recv='{}') LOCATION '{}'""".format(found_date, location)
        )

    logger.log('Add input_file_name to transactions')
    txn_df = driver.spark.table('transactions').withColumn('input_file_name', F.input_file_name())
    txn_df.createOrReplaceTempView('txn')

    logger.log('- Loading new PHI data')
    new_phi_path = S3_EXPRESS_SCRIPTS_RX_MATCHING + input_date_path + '/'
    payload_loader.load(driver.runner, new_phi_path, ['hvJoinKey', 'patientId'])
    driver.runner.run_spark_query('ALTER TABLE matching_payload RENAME TO new_phi')

    logger.log('- Loading matching_payload data')
    driver.runner.run_spark_script('sql_loaders/load_matching_payloads.sql', [
        ['matching_path', S3_MATCHING_KEY]
    ])
    file_cmd = ['aws', 's3', 'ls', 's3://' + S3_MATCHING_KEY, '--recursive']
    files = subprocess.check_output(file_cmd).decode().split("\n")
    found_dates = [re.findall('20[0-9]{2}/../..', x)[0] for x in
                   [x for x in files if re.search('20[0-9]{2}/../..', x)]]
    found_dates = set(found_dates)
    for found_date in found_dates:
        location = 's3a://' + S3_MATCHING_KEY + found_date + '/'
        driver.runner.run_spark_query(
            """ALTER TABLE matching_payload
            ADD PARTITION (part_date_recv='{}') LOCATION '{}'""".format(found_date, location)
        )

    logger.log('Done loading data')

    logger.log('Combining PHI data tables')
    driver.runner.run_spark_script('sql_loaders/load_and_combine_phi.sql', [
        ['local_phi_path', LOCAL_REF_PHI],
        ['s3_phi_path', S3A_REF_PHI],
        ['partitions', driver.spark.conf.get('spark.sql.shuffle.partitions'), False]
    ])

    driver.transform()
    driver.save_to_disk()

    if not driver.test and not driver.end_to_end_test:
        logger.log_run_details(
            driver.provider_name,
            driver.data_type,
            driver.input_path,
            driver.matching_path,
            driver.output_path,
            RunType.MARKETPLACE,
            driver.date_input
        )

    driver.stop_spark()

    # the output path will be different for prod and e2e testing
    output_destination = driver.output_path + PROVIDER_PATH
    logger.log('Delete existing enrollment data: ' + output_destination)
    subprocess.check_output(['aws', 's3', 'rm', '--recursive', output_destination])

    driver.copy_to_output_path()

    if not driver.end_to_end_test:
        logger.log('Deleting the old PHI reference data from s3: ' + S3_REF_PHI)
        subprocess.check_call(['aws', 's3', 'rm', '--recursive', S3_REF_PHI])
        logger.log('Rewriting the updated PHI reference data to s3: ' + S3A_REF_PHI)
        subprocess.check_call(
            ['s3-dist-cp', '--s3ServerSideEncryption', '--src', LOCAL_REF_PHI, '--dest',
             S3A_REF_PHI])



