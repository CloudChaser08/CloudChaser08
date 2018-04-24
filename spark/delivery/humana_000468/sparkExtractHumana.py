#! /usr/bin/python
import argparse
import time
import subprocess
from datetime import timedelta, datetime
from spark.runner import Runner
from spark.spark_setup import init
import spark.helpers.file_utils as file_utils
import spark.helpers.normalized_records_unloader as normalized_records_unloader
import spark.helpers.payload_loader as payload_loader
from pyspark.sql.types import StructType, IntegerType
import pyspark.sql.functions as F

import extract_medicalclaims
import extract_pharmacyclaims

def run(spark, runner, group_id, test=False, airflow_test=False):
    matching_path = 's3://salusv/matching/payload/custom/humana/hv000468/{}/'.format(group_id)

    payload_loader.load(runner, matching_path, ['matchStatus'], return_output=True)
    matched_patients = payload_loader.where("matchStatus = 'exact_match' or matchStatus = 'inexact_match'")

    ts = time.time()
    start = '2016-12-01'
    end = '2018-03-01'

    if matching_patients.select('hvid').distinct().count() < 10:
        medical_extract    = spark.createDataFrame([], StructType([]))
        pharmacy_extract   = spark.createDataFrame([], StructType([]))
    else:
        medical_extract    = extract_medicalclaims.extract(
                runner, matching_patients.select('hvid').distinct(), ts,
                start, end)
        pharmacy_extract  = extract_pharmacyclaims.extract(
                runner, matching_patients.select('hvid').distinct(), ts,
                start, end)

    medical_extract.repartition(1).write.csv('s3a://salusv/deliverable/humana/hv000468/{}/'.format(group_id), sep='|', compression='gzip', mode='append')
    fn = [w for w in
            [r for r in subprocess.check_output(['aws', 's3', 'ls', 's3://salusv/deliverable/humana/hv000468/{}/'.format(group_id)]).split('\n')]
            if w.startswith('part-00000')][0]
    subprocess.check_call(['aws', 's3', 'mv', 's3://salusv/deliverable/humana/hv000468/{}/'.format(group_id) + fn,
        's3://salusv/deliverable/humana/hv000468/{}/'.format(group_id) + 'billing_medical_claims_{}.psv.gz'.format(group_id)]
    medical_extract.drop('hvm_tile_name').repartition(1).write.csv('s3a://salusv/deliverable/humana/hv000468/{}/'.format(group_id), sep='|', compression='gzip', mode='append')
    fn = [w for w in
            [r for r in subprocess.check_output(['aws', 's3', 'ls', 's3://salusv/deliverable/humana/hv000468/{}/'.format(group_id)]).split('\n')]
            if w.startswith('part-00000')][0]
    subprocess.check_call(['aws', 's3', 'mv', 's3://salusv/deliverable/humana/hv000468/{}/'.format(group_id) + fn,
        's3://salusv/deliverable/humana/hv000468/{}/'.format(group_id) + 'medical_claims_{}.psv.gz'.format(group_id)]
    pharmacy_extract.repartition(1).write.csv('s3a://salusv/deliverable/humana/hv000468/{}/'.format(group_id), sep='|', compression='gzip', mode='append')
    fn = [w for w in
            [r for r in subprocess.check_output(['aws', 's3', 'ls', 's3://salusv/deliverable/humana/hv000468/{}/'.format(group_id)]).split('\n')]
            if w.startswith('part-00000')][0]
    subprocess.check_call(['aws', 's3', 'mv', 's3://salusv/deliverable/humana/hv000468/{}/'.format(group_id) + fn,
        's3://salusv/deliverable/humana/hv000468/{}/'.format(group_id) + 'billing_pharmacy_claims_{}.psv.gz'.format(group_id))
    pharmacy_extract.drop('hvm_time_name').repartition(1).write.csv('s3a://salusv/deliverable/humana/hv000468/{}/'.format(group_id), sep='|', compression='gzip', mode='append')
    fn = [w for w in
            [r for r in subprocess.check_output(['aws', 's3', 'ls', 's3://salusv/deliverable/humana/hv000468/{}/'.format(group_id)]).split('\n')]
            if w.startswith('part-00000')][0]
    subprocess.check_call(['aws', 's3', 'mv', 's3://salusv/deliverable/humana/hv000468/{}/'.format(group_id) + fn,
        's3://salusv/deliverable/humana/hv000468/{}/'.format(group_id) + 'pharmacy_claims_{}.psv.gz'.format(group_id)]

def main(args):
    # init
    spark, sqlContext = init("Extract for Humana")

    # initialize runner
    runner = Runner(sqlContext)

    run(spark, runner, args.group_id, airflow_test=args.airflow_test)

    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--group_id', type=str)
    parser.add_argument('--airflow_test', default=False, action='store_true')
    args = parser.parse_args()
    main(args)
