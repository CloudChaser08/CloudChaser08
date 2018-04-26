#! /usr/bin/python
import argparse
import time
import subprocess
from datetime import timedelta, date
from spark.runner import Runner
from spark.spark_setup import init
import spark.helpers.file_utils as file_utils
import spark.helpers.normalized_records_unloader as normalized_records_unloader
import spark.helpers.payload_loader as payload_loader
from pyspark.sql.types import StructType
import pyspark.sql.functions as F

import extract_medicalclaims
import extract_pharmacyclaims

def run(spark, runner, group_id, test=False, airflow_test=False):
    ts = time.time()
    today = date.today()

    if airflow_test:
        output_path   = 's3://salusv/testing/dewey/airflow/e2e/humana/hv000468/deliverable/{}/'.format(group_id)
        matching_path = 's3a://salusv/testing/dewey/airflow/e2e/humana/hv000468/payload/{}/'.format(group_id)
        list_cmd      = ['aws', 's3', 'ls']
        move_cmd      = ['aws', 's3', 'mv']
    elif test:
        output_path = file_utils.get_abs_path(
            __file__, '../../test/delivery/humana/hv000468/out/test1234/'
        ) + '/'
        matching_path = file_utils.get_abs_path(
            __file__, '../../test/delivery/humana/hv000468/resources/matching/test1234/'
        ) + '/'
        list_cmd      = ['ls', '-la']
        move_cmd      = ['mv']

        # Need to be able to test consistantly
        today = date(2018, 4, 26)
        ts = 1524690702.12345
    else:
        output_path   = 's3://salusv/deliverable/humana/hv000468/{}/'.format(group_id)
        matching_path = 's3a://salusv/matching/payload/custom/humana/hv000468/{}/'.format(group_id)
        list_cmd      = ['aws', 's3', 'ls']
        move_cmd      = ['aws', 's3', 'mv']

    matched_patients = payload_loader.load(runner, matching_path, ['matchStatus'], return_output=True)
    matched_patients = matched_patients.where("matchStatus = 'exact_match' or matchStatus = 'inexact_match'")

    if today.day > 15:
        end   = (today.replace(day=15) - timedelta(days=30)).replace(day=1) # The 1st about 1.5 months back
        start = (end - timedelta(days=455)).replace(day=1) # 15 months before end
    else:
        end   = (today.replace(day=15) - timedelta(days=60)).replace(day=15) # The 15th about 1.5 months back
        start = (end - timedelta(days=455)).replace(day=15) # 15 months before end
    start = start.isoformat()
    end   = end.isoformat()

    matched_patients = matched_patients.select('hvid').distinct()

    if matched_patients.count() < 10:
        medical_extract    = spark.createDataFrame([], StructType([]))
        pharmacy_extract   = spark.createDataFrame([], StructType([]))
    else:
        medical_extract    = extract_medicalclaims.extract(
                runner, matched_patients, ts,
                start, end)
        pharmacy_extract  = extract_pharmacyclaims.extract(
                runner, matched_patients, ts,
                start, end)

    # for each testing
    medical_extract.createOrReplaceTempView('medical_extract')
    pharmacy_extract.createOrReplaceTempView('pharmacy_extract')

    medical_extract.repartition(1).write \
            .csv(output_path.replace('s3://', 's3a://'), sep='|', compression='gzip', mode='append')
    fn = [w for r in
            subprocess.check_output(list_cmd + [output_path]).split('\n')
            for w in r.split(' ') if w.startswith('part-00000')][0]
    subprocess.check_call(move_cmd + [output_path + fn, output_path + 'medical_claims_{}.psv.gz'.format(group_id)])
    pharmacy_extract.repartition(1).write \
            .csv(output_path, sep='|', compression='gzip', mode='append')
    fn = [w for r in
            subprocess.check_output(list_cmd + [output_path]).split('\n')
            for w in r.split(' ') if w.startswith('part-00000')][0]
    subprocess.check_call(move_cmd + [output_path + fn, output_path + 'pharmacy_claims_{}.psv.gz'.format(group_id)])

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
