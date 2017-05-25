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


def run(spark, runner, month, test=False):
    start_date = datetime.strftime(
        datetime.strptime(month, '%Y-%m') - timedelta(days=1),
        '%Y/%m/01'
    )
    end_date = datetime.strftime(
        (datetime.strptime(month, '%Y-%m') + timedelta(days=31)).replace(day=1) - timedelta(days=1),
        '%Y/%m/%d'
    )

    if test:
        enrollment_outpath = '/tmp/ubc_enrollment_data'
        pharmacy_outpath   = '/tmp/ubc_pharmacy_data'
    else:
        enrollment_outpath = 'hdfs:///ubc_enrollment_data/'
        pharmacy_outpath   = 'hdfs:///ubc_pharmacy_data/'

    runner.run_spark_script('extract_enrollment_records.sql', [
        ['out_path', enrollment_outpath]
    ])

    runner.run_spark_script('extract_pharmacy_records.sql', [
        ['start_date', start_date],
        ['end_date', end_date],
        ['out_path', pharmacy_outpath],
    ])

    if test:
        pharmacy_part_files_cmd = ['find', pharmacy_outpath, '-type', 'f']
        enrollment_part_files_cmd = ['find', enrollment_outpath, '-type', 'f']
    else:
        pharmacy_part_files_cmd = ['hadoop', 'fs', '-ls', '-R', pharmacy_outpath.replace('hdfs://', '')]
        enrollment_part_files_cmd = ['hadoop', 'fs', '-ls', '-R', enrollment_outpath.replace('hdfs://', '')]

    part_files = subprocess.check_output(pharmacy_part_files_cmd).strip().split("\n")
    prefix = 'pharmacyclaims_{}_{}'.format(datetime.strftime(datetime.strptime(start_date, '%Y/%m/%d'), '%Y-%m'),
        datetime.strftime(datetime.strptime(end_date, '%Y/%m/%d'), '%Y-%m'))
    spark.sparkContext.parallelize(part_files).repartition(1000).foreach(
        normalized_records_unloader.mk_move_file(prefix, test)
    )
    part_files = subprocess.check_output(enrollment_part_files_cmd).strip().split("\n")
    prefix = 'enrollmentrecords_{}_{}'.format(datetime.strftime(datetime.strptime(start_date, '%Y/%m/%d'), '%Y-%m'),
        datetime.strftime(datetime.strptime(end_date, '%Y/%m/%d'), '%Y-%m'))
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
        pharmacy_outpath, '--dest', S3_UBC_OUT
    ])

    subprocess.check_call([
        's3-dist-cp', '--s3ServerSideEncryption', '--src',
        enrollment_outpath, '--dest', S3_UBC_OUT
    ])

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--month', type=str)
    args = parser.parse_args()
    main(args)
