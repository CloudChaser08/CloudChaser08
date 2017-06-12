#! /usr/bin/python
import argparse
import time
import subprocess
from datetime import timedelta, datetime
from spark.runner import Runner
from spark.spark_setup import init
import spark.helpers.file_utils as file_utils
import spark.helpers.normalized_records_unloader as normalized_records_unloader

TODAY = time.strftime('%Y-%m-%d', time.localtime())
S3_UBC_OUT = 's3://healthverity/pickup/ubc/'


# Every month we will send UBC 2 months of pharmacy claims data, a 'final'
# version of the data from 2 months ago, and a 'preliminary' version of
# the previous month's data. We will also send them a complete update of
# the enrollment data
def run(spark, runner, month, test=False):
    month_final = datetime.strftime(
        datetime.strptime(month, '%Y-%m') - timedelta(days=1),
        '%Y/%m'
    )
    month_prelim = month.replace('-', '/')

    if test:
        enrollment_outpath = '/tmp/ubc_enrollment_data'
        pharmacy_final     = '/tmp/ubc_pharmacy_final_data'
        pharmacy_prelim    = '/tmp/ubc_pharmacy_prelim_data'
    else:
        enrollment_outpath = 'hdfs:///ubc_enrollment_data/'
        pharmacy_final     = 'hdfs:///ubc_pharmacy_final_data/'
        pharmacy_prelim    = 'hdfs:///ubc_pharmacy_prelim_data/'

    runner.run_spark_script('extract_enrollment_records.sql', [
        ['out_path', enrollment_outpath]
    ])

    runner.run_spark_script('extract_pharmacy_records.sql', [
        ['table', 'express_scripts_rx_norm_final_out', False],
        ['month', month_final, False],
        ['out_path', pharmacy_final],
    ])

    runner.run_spark_script('extract_pharmacy_records.sql', [
        ['table', 'express_scripts_rx_norm_prelim_out', False],
        ['month', month_prelim, False],
        ['out_path', pharmacy_prelim]
    ])

    if test:
        pharmacy_final_part_files_cmd = ['find', pharmacy_final, '-type', 'f']
        pharmacy_prelim_part_files_cmd = ['find', pharmacy_prelim, '-type', 'f']
        enrollment_part_files_cmd = ['find', enrollment_outpath, '-type', 'f']
    else:
        pharmacy_final_part_files_cmd = ['hadoop', 'fs', '-ls', '-R', pharmacy_final.replace('hdfs://', '')]
        pharmacy_prelim_part_files_cmd = ['hadoop', 'fs', '-ls', '-R', pharmacy_prelim.replace('hdfs://', '')]
        enrollment_part_files_cmd = ['hadoop', 'fs', '-ls', '-R', enrollment_outpath.replace('hdfs://', '')]

    part_files = subprocess.check_output(pharmacy_final_part_files_cmd).strip().split("\n")
    prefix = 'pharmacyclaims_{}_final'.format(month_final.replace('/', '-'))
    spark.sparkContext.parallelize(part_files).repartition(1000).foreach(
        normalized_records_unloader.mk_move_file(prefix, test)
    )
    part_files = subprocess.check_output(pharmacy_prelim_part_files_cmd).strip().split("\n")
    prefix = 'pharmacyclaims_{}_prelim'.format(month)
    spark.sparkContext.parallelize(part_files).repartition(1000).foreach(
        normalized_records_unloader.mk_move_file(prefix, test)
    )
    part_files = subprocess.check_output(enrollment_part_files_cmd).strip().split("\n")
    prefix = 'enrollmentrecords_{}'.format(month)
    spark.sparkContext.parallelize(part_files).repartition(1000).foreach(
        normalized_records_unloader.mk_move_file(prefix, test)
    )

def main(args):
    # init
    spark, sqlContext = init("ESI export for UBC")

    # initialize runner
    runner = Runner(sqlContext)

    run(spark, runner, args.month)

    spark.stop()

    subprocess.check_call([
        's3-dist-cp', '--s3ServerSideEncryption', '--src',
        pharmacy_final, '--dest', S3_UBC_OUT + '/pharmacyclaims/'
    ])

    subprocess.check_call([
        's3-dist-cp', '--s3ServerSideEncryption', '--src',
        pharmacy_prelim, '--dest', S3_UBC_OUT + '/pharmacyclaims/'
    ])

    subprocess.check_call([
        's3-dist-cp', '--s3ServerSideEncryption', '--src',
        enrollment_outpath, '--dest', S3_UBC_OUT + '/enrollment/'
    ])

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--month', type=str)
    args = parser.parse_args()
    main(args)
