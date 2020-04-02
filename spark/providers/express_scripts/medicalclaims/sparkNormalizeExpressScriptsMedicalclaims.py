import argparse
import subprocess
import re
from datetime import datetime
import spark.helpers.constants as constants
from spark.common.marketplace_driver import MarketplaceDriver
from spark.common.medicalclaims_common_model import schemas as medicalclaims_schemas
from spark.common.utility import logger
from pyspark import StorageLevel
from spark.helpers import normalized_records_unloader
import spark.helpers.payload_loader as payload_loader
import spark.helpers.external_table_loader as external_table_loader
import pyspark.sql.functions as F

S3_MATCHING_KEY = 'salusv/matching/payload/medicalclaims/express_scripts/'
S3_EXPRESS_SCRIPTS_RX_MATCHING = '{}salusv/matching/payload/pharmacyclaims/esi/'
S3 = 's3://'
S3A = 's3a://'
REF_PHI = 'salusv/reference/express_scripts_phi/'
S3_REF_PHI = S3 + REF_PHI
S3A_REF_PHI = S3A + REF_PHI
UNMATCHED_REFERENCE = 'salusv/reference/express_scripts_unmatched/'
S3_UNMATCHED_REFERENCE = S3 + UNMATCHED_REFERENCE
S3A_UNMATCHED_REFERENCE = S3A + UNMATCHED_REFERENCE
LOCAL_REF_PHI = 'hdfs:///local_phi/'
LOCAL_UNMATCHED = '/unmatched/'

if __name__ == "__main__":

    # ------------------------ Provider specific configuration -----------------------
    provider_name = 'express_scripts'
    output_table_names_to_schemas = {
        'esi_final_matched_transactions_dx': medicalclaims_schemas['schema_v9']
    }
    provider_partition_name = 'express_scripts'

    # ------------------------ Common for all providers -----------------------

    # Parse input arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str)
    # RX and DX data does not come in at the same time.
    # This argument allows you to point to the correct Rx PHI date
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
        end_to_end_test,
        vdr_feed_id=155,
        use_ref_gen_values=True
    )

    def load_data():
        logger.log('Loading data:')

        # BPM - We're going to ignore historic records for now
        # logger.log(' -Loading unmatched records')
        # driver.spark.read.parquet(S3_UNMATCHED_REFERENCE) \
        #     .createOrReplaceTempView('historic_unmatched_records')

        num_buckets = int(driver.spark.sparkContext.getConf().get("spark.executor.instances"))

        logger.log(' -Loading ref_gen_ref table')
        external_table_loader.load_ref_gen_ref(driver.runner.sqlContext)

        logger.log('- Loading Transactions')
        driver.runner.run_spark_script('sql_loaders/load_transactions.sql',
                                       [['input_path', driver.input_path]])

        driver.spark.table('transactions').withColumn('input_file_name', F.input_file_name())\
            .createOrReplaceTempView('txn')

        logger.log('Load Rx payload reference location')
        driver.spark.read.parquet(S3_REF_PHI).createOrReplaceTempView('rx_payloads')

        driver\
                .spark\
                .table('rx_payloads')\
                .write\
                .mode('overwrite')\
                .format('orc')\
                .bucketBy(num_buckets, 'patient_id')\
                .saveAsTable('rx_bucketed')

        driver.spark.table('rx_bucketed').createOrReplaceTempView('rx_payloads')

        logger.log('- Loading Dx matching_payload data')
        payload_loader.load(driver.runner, driver.matching_path, load_file_name=True)

        driver\
                .spark\
                .table('matching_payload')\
                .write\
                .mode('overwrite')\
                .format('orc')\
                .bucketBy(num_buckets, 'patientid')\
                .saveAsTable('payload_bucketed')

        driver.spark.table('payload_bucketed').createOrReplaceTempView('matching_payload')

        logger.log('Done loading data')

    def transform_data():
        driver.transform()
        driver.spark.table('esi_join_transactions_and_payload_dx').persist(StorageLevel.MEMORY_AND_DISK)

    def save_to_disk():
        # save matched transactions to disk
        driver.save_to_disk()

        # output the matched historic records.
        # Ensure that the original file_date is maintained on the parquet file
        #logger.log('Rename newly matched historic files:')
        #staging_dir = constants.hdfs_staging_dir + 'matched/'
        #partition_by = ['file_date', 'part_provider', 'part_best_date']
        #driver.spark.table('esi_final_matched_historic_dx').coalesce(20).write.parquet(
            #staging_dir, partitionBy=partition_by, compression='gzip', mode='append'
        #)

        def mk_move_file_preserve_file_date():
            mv_cmd = ['hadoop', 'fs', '-mv']
            mkdir_cmd = ['hadoop', 'fs', '-mkdir', '-p']
            ls_cmd = ['hadoop', 'fs', '-ls']

            def move_file(part_file):
                if part_file.find("/part-") > -1:
                    old_pf = part_file.split(' ')[-1].strip()
                    old_pf_split = old_pf.split('/')
                    new_filename = old_pf_split[3].split('=')[1] + '_' + old_pf_split[-1]
                    new_pf_array = ['', 'staging', 'medicalclaims', '2018-06-06'] + \
                                   old_pf_split[4:-1] + \
                                   [new_filename]
                    new_directory = '/'.join(new_pf_array[:-1])
                    new_pf = '/'.join(new_pf_array)
                    try:
                        subprocess.check_call(mkdir_cmd + [new_directory])
                        subprocess.check_call(mv_cmd + [old_pf, new_pf])
                    except Exception as e:
                        # The move command will fail if the final has already
                        # been moved. Check here if the destination file exist
                        # and ignore the error if it does
                        try:
                            subprocess.check_call(ls_cmd + [new_pf])
                        except:
                            raise e

            return move_file

        #part_files_cmd = ['hadoop', 'fs', '-ls', '-R', staging_dir]
        #part_files = subprocess.check_output(part_files_cmd).decode().strip().split("\n")
        #driver.spark.sparkContext.parallelize(part_files).repartition(1000).foreach(
            #mk_move_file_preserve_file_date()
        #)

        logger.log('Save unmatched reference records to: /unmatched/')
        driver\
                .spark\
                .table('esi_final_unmatched_dx')\
                .repartition(100)\
                .write\
                .parquet('/unmatched/', partitionBy='part_best_date', compression='gzip', mode='overwrite')

    def overwrite_reference_data():
        if not driver.end_to_end_test:
            #logger.log('Deleting the PHI reference data from s3: ' + S3_REF_PHI)
            #subprocess.check_call(['aws', 's3', 'rm', '--recursive', S3_REF_PHI])
            #logger.log('Rewriting the updated PHI reference data to s3: ' + S3A_REF_PHI)
            #subprocess.check_call(
                #['s3-dist-cp', '--s3ServerSideEncryption', '--src', LOCAL_REF_PHI, '--dest',
                 #S3A_REF_PHI])

            logger.log('Deleting the unmatched reference data from s3: ' + S3_UNMATCHED_REFERENCE)
            subprocess.check_call(['aws', 's3', 'rm', '--recursive', S3_UNMATCHED_REFERENCE])
            logger.log('Rewrite the unmatched reference data to s3: ' + S3_UNMATCHED_REFERENCE)
            normalized_records_unloader.distcp(S3_UNMATCHED_REFERENCE, LOCAL_UNMATCHED)

    # Run the job
    driver.init_spark_context()
    load_data()
    transform_data()
    save_to_disk()
    driver.log_run()
    driver.stop_spark()
    driver.copy_to_output_path()
    overwrite_reference_data()
