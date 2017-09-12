#! /usr/bin/python
import argparse
import time
import subprocess
from spark.runner import Runner
from spark.spark_setup import init

TODAY = time.strftime('%Y-%m-%d', time.localtime())

ANALYTICSDB_SCHEMA = 'for321'

# TODO: Change?
S3_FORESITE_OUT = 's3://healthverity/pickup/foresite/'

FINAL_OUT_LOC = 'hdfs:///final_data/'


def run(spark, runner, date, test=False):

    runner.run_spark_script('reload_codes.sql', [
        ['analyticsdb_schema', ANALYTICSDB_SCHEMA, False]
    ])

    runner.run_spark_script('create_extract.sql', [
        ['analyticsdb_schema', ANALYTICSDB_SCHEMA, False],
        ['delivery_date', date]
    ])


def main(args):
    # init
    spark, sqlContext = init("Foresite Capital Delivery")

    # initialize runner
    runner = Runner(sqlContext)

    run(spark, runner, args.date)

    spark.stop()

    subprocess.check_call([
        's3-dist-cp', '--s3ServerSideEncryption', '--src',
        FINAL_OUT_LOC, '--dest', S3_FORESITE_OUT + args.month + '/'
    ])


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str)
    args = parser.parse_args()
    main(args)
