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
import extract_enrollmentrecords

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
            __file__, '../../test/delivery/humana/hv000468/out/{}/'.format(group_id)
        ) + '/'
        subprocess.check_call(['mkdir', '-p', output_path])
        matching_path = file_utils.get_abs_path(
            __file__, '../../test/delivery/humana/hv000468/resources/matching/{}/'.format(group_id)
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

    all_patients = payload_loader.load(runner, matching_path, ['matchStatus'], return_output=True)
    matched_patients = all_patients.where("matchStatus = 'exact_match' or matchStatus = 'inexact_match'")

    if today.day > 15:
        end   = (today.replace(day=15) - timedelta(days=30)).replace(day=1) # The 1st about 1.5 months back
        start = (end - timedelta(days=455)).replace(day=1) # 15 months before end
    else:
        end   = (today.replace(day=15) - timedelta(days=60)).replace(day=15) # The 15th about 1.5 months back
        start = (end - timedelta(days=455)).replace(day=15) # 15 months before end

    matched_patients = matched_patients.select('hvid').distinct()
    all_patient_count = all_patients.count()
    matched_patient_count = matched_patients.count()

    if matched_patients.count() < 10:
        medical_extract    = spark.createDataFrame([], StructType([]))
        pharmacy_extract   = spark.createDataFrame([], StructType([]))
        enrollment_extract = spark.createDataFrame([], StructType([]))
        summary            = spark.createDataFrame([('-', 0)], ['data_vendor', 'count'])
    else:
        medical_extract    = extract_medicalclaims.extract(
                runner, matched_patients, ts,
                start, end).cache_and_track('medical_extract')
        pharmacy_extract   = extract_pharmacyclaims.extract(
                runner, matched_patients, ts,
                start, end).cache_and_track('pharmacy_extract')
        enrollment_extract = extract_enrollmentrecords.extract(
                spark, runner, matched_patients, ts,
                start, end, pharmacy_extract) \
                    .cache_and_track('enrollment_extract')

        # summary
        med_summary    =  medical_extract \
                .withColumn('claim', F.when(F.length(F.trim(F.col('claim_id'))) > 0, F.col('claim_id')).otherwise(F.col('record_id'))) \
                .select('data_vendor', 'claim').distinct() \
                .groupBy('data_vendor').count()
        pharma_summary = pharmacy_extract \
                .withColumn('claim', F.when(F.length(F.trim(F.col('claim_id'))) > 0, F.col('claim_id')).otherwise(F.col('record_id'))) \
                .select('data_vendor', 'claim').distinct() \
                .groupBy('data_vendor').count()

        summary = med_summary.union(pharma_summary)

    # for easy testing
    medical_extract.createOrReplaceTempView('medical_extract')
    pharmacy_extract.createOrReplaceTempView('pharmacy_extract')

    summary.createOrReplaceTempView('summary')

    summary_report = '\n'.join(['|'.join([
            group_id, str(all_patient_count), str(matched_patient_count),
            r['data_vendor'], str(r['count'])
        ]) for r in summary.collect()])

    with open('/tmp/summary_report_{}.txt'.format(group_id), 'w') as outf:
        outf.write(summary_report)
    subprocess.check_call(move_cmd + ['/tmp/summary_report_{}.txt'.format(group_id), output_path])

    medical_extract.repartition(1).write \
            .csv(output_path.replace('s3://', 's3a://'), sep='|', mode='append')
    fn = [w for r in
            subprocess.check_output(list_cmd + [output_path]).split('\n')
            for w in r.split(' ') if w.startswith('part-00000')][0]
    subprocess.check_call(move_cmd + [output_path + fn, output_path + 'medical_claims_{}.psv'.format(group_id)])
    pharmacy_extract.repartition(1).write \
            .csv(output_path.replace('s3://', 's3a://'), sep='|', mode='append')
    fn = [w for r in
            subprocess.check_output(list_cmd + [output_path]).split('\n')
            for w in r.split(' ') if w.startswith('part-00000')][0]
    subprocess.check_call(move_cmd + [output_path + fn, output_path + 'pharmacy_claims_{}.psv'.format(group_id)])
    enrollment_extract.repartition(1).write \
            .csv(output_path.replace('s3://', 's3a://'), sep='|', mode='append')
    fn = [w for r in
            subprocess.check_output(list_cmd + [output_path]).split('\n')
            for w in r.split(' ') if w.startswith('part-00000')][0]
    subprocess.check_call(move_cmd + [output_path + fn, output_path + 'enrollment_{}.psv'.format(group_id)])

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
