#! /usr/bin/python
import argparse
import time
import subprocess
from datetime import timedelta, date
from spark.runner import Runner
from spark.spark_setup import init
import spark.helpers.file_utils as file_utils
import spark.helpers.payload_loader as payload_loader
from pyspark.sql.types import StructType, StructField, StringType
import pyspark.sql.functions as F
import boto3

import extract_medicalclaims
import extract_pharmacyclaims
import extract_enrollmentrecords

EMPTY_HUMANA_EXTRACT = lambda spark, group_id: spark.createDataFrame([(F.lit(group_id))], StructType([StructField('humana_group_id', StringType(), True)]))
EMPTY_HUMANA_SUMMARY = lambda spark: spark.createDataFrame([('-', 0)], ['data_vendor', 'count'])

def run(spark, runner, group_ids, test=False, airflow_test=False):
    ts = time.time()
    today = date.today()

    if airflow_test:
        output_path_template   = 's3://salusv/testing/dewey/airflow/e2e/humana/hv000468/deliverable/{}/'
        matching_path_template = 's3a://salusv/testing/dewey/airflow/e2e/humana/hv000468/payload/{}/'
        list_cmd      = ['aws', 's3', 'ls']
        move_cmd      = ['aws', 's3', 'mv']
    elif test:
        output_path_template = file_utils.get_abs_path(
            __file__, '../../test/delivery/humana/hv000468/out/{}/'
        ) + '/'
        for group_id in group_ids:
            subprocess.check_call(['mkdir', '-p', output_path_template.format(group_id)])
        matching_path_template = file_utils.get_abs_path(
            __file__, '../../test/delivery/humana/hv000468/resources/matching/{}/'
        ) + '/'
        list_cmd      = ['ls', '-la']
        move_cmd      = ['mv']

        # Need to be able to test consistantly
        today = date(2018, 4, 26)
        ts = 1524690702.12345
    else:
        output_path_template   = 's3://salusv/deliverable/humana/hv000468/{}/'
        matching_path_template = 's3a://salusv/matching/payload/custom/humana/hv000468/{}/'
        list_cmd      = ['aws', 's3', 'ls']
        move_cmd      = ['aws', 's3', 'mv']

    all_patients = {
        group_id: payload_loader.load(runner, matching_path_template.format(group_id), ['matchStatus'], return_output=True) \
                .withColumn('humana_group_id', F.lit(group_id))
        for group_id in group_ids
    }
    matched_patients = {
        ap[0] : ap[1].where("matchStatus = 'exact_match' or matchStatus = 'inexact_match'") \
                .select('hvid', 'humana_group_id').distinct()
        for ap in all_patients.items()
    }

    if today.day > 15:
        end   = (today.replace(day=15) - timedelta(days=30)).replace(day=1) # The 1st about 1.5 months back
        start = (end - timedelta(days=455)).replace(day=1) # 15 months before end
    else:
        end   = (today.replace(day=15) - timedelta(days=60)).replace(day=15) # The 15th about 1.5 months back
        start = (end - timedelta(days=455)).replace(day=15) # 15 months before end

    group_all_patient_count       = {ap[0] : ap[1].count() for ap in all_patients.items()}
    group_matched_patient_count   = {mp[0] : mp[1].count() for mp in matched_patients.items()}
    group_patient_w_records_count = {group_id : 0 for group_id in group_ids}

    group_summary = {}
    valid_groups = []
    (medical_extracts, pharmacy_extracts, enrollment_extracts) = ([], [], [])
    medical_extract  = pharmacy_extract  = enrollment_extract  = None
    for group_id in group_ids:
        if group_matched_patient_count[group_id] < 10:
            medical_extracts.append(EMPTY_HUMANA_EXTRACT(spark, group_id))
            pharmacy_extracts.append(EMPTY_HUMANA_EXTRACT(spark, group_id))
            enrollment_extracts.append(EMPTY_HUMANA_EXTRACT(spark, group_id))
            group_summary[group_id] = EMPTY_HUMANA_SUMMARY(spark)
        else:
            valid_groups.append(matched_patients[group_id])

    if medical_extracts:
        medical_extract = reduce(lambda x, y: x.union(y), medical_extracts)
    if pharmacy_extracts:
        pharmacy_extract = reduce(lambda x, y: x.union(y), pharmacy_extracts)
    if enrollment_extracts:
        enrollment_extract = reduce(lambda x, y: x.union(y), enrollment_extracts)

    if valid_groups:
        unioned_matched_patients = reduce(lambda x, y: x.union(y), valid_groups)
        medical_extract    = extract_medicalclaims.extract(
                runner, unioned_matched_patients, ts,
                start, end).cache_and_track('medical_extract')
        pharmacy_extract   = extract_pharmacyclaims.extract(
                runner, unioned_matched_patients, ts,
                start, end).cache_and_track('pharmacy_extract')
        enrollment_extract = extract_enrollmentrecords.extract(
                spark, runner, unioned_matched_patients, ts,
                start, end, pharmacy_extract) \
                    .cache_and_track('enrollment_extract')

        # summary
        for group_id in group_ids:
            if group_matched_patient_count[group_id] >= 10:
                med_summary    = medical_extract.where(F.col('humana_group_id') == F.lit(group_id)) \
                        .withColumn('claim',
                                F.when(
                                    F.length(F.trim(F.col('claim_id'))) > 0,
                                    F.col('claim_id')
                                ).otherwise(F.col('record_id'))) \
                        .select('data_vendor', 'claim').distinct() \
                        .groupBy('data_vendor').count()

                pharma_summary = pharmacy_extract.where(F.col('humana_group_id') == F.lit(group_id)) \
                        .withColumn('claim',
                                F.when(
                                    F.length(F.trim(F.col('claim_id'))) > 0,
                                    F.col('claim_id')
                                ).otherwise(F.col('record_id'))) \
                        .select('data_vendor', 'claim').distinct() \
                        .groupBy('data_vendor').count()

                group_summary[group_id] = med_summary.union(pharma_summary)
                group_patient_w_records_count[group_id] = \
                    medical_extract \
                        .where(F.col('humana_group_id') == F.lit(group_id)) \
                        .select('hvid').union(
                            pharmacy_extract.where(F.col('humana_group_id') == F.lit(group_id)) \
                                .select('hvid')) \
                        .distinct().count()

                if group_patient_w_records_count[group_id] == 0:
                    group_summary[group_id] = EMPTY_HUMANA_SUMMARY(spark)

    # for easy testing
    for group_id in group_ids:
        medical_extract.where(F.col('humana_group_id') == F.lit(group_id)) \
            .drop('humana_group_id') \
            .createOrReplaceTempView(group_id.replace('-', '_') + '_medical_extract')
        pharmacy_extract.where(F.col('humana_group_id') == F.lit(group_id)) \
            .drop('humana_group_id') \
            .createOrReplaceTempView(group_id.replace('-', '_') + '_pharmacy_extract')

        group_summary[group_id].createOrReplaceTempView(group_id.replace('-', '_') + '_summary')

    for group_id in group_ids:
        all_patient_count       = group_all_patient_count[group_id]
        matched_patient_count   = group_matched_patient_count[group_id]
        patient_w_records_count = group_patient_w_records_count[group_id]
        summary_report = '\n'.join(['|'.join([
                group_id, str(all_patient_count), str(matched_patient_count),
                str(patient_w_records_count), r['data_vendor'], str(r['count'])
            ]) for r in group_summary[group_id].collect()])

        output_path = output_path_template.format(group_id)
        with open('/tmp/summary_report_{}.txt'.format(group_id), 'w') as outf:
            outf.write(summary_report)
        cmd = move_cmd + ['/tmp/summary_report_{}.txt'.format(group_id), output_path]
        subprocess.check_call(cmd)

        medical_extract.where(F.col('humana_group_id') == F.lit(group_id)) \
            .drop('humana_group_id') \
            .repartition(1).write \
            .csv(output_path.replace('s3://', 's3a://'), sep='|', mode='append')
        fn = [w for r in
            subprocess.check_output(list_cmd + [output_path]).split('\n')
            for w in r.split(' ') if w.startswith('part-00000')][0]
        cmd = move_cmd + [output_path + fn, output_path + 'medical_claims_{}.psv'.format(group_id)]
        subprocess.check_call(cmd)

        pharmacy_extract.where(F.col('humana_group_id') == F.lit(group_id)) \
            .drop('humana_group_id') \
            .repartition(1).write \
            .csv(output_path.replace('s3://', 's3a://'), sep='|', mode='append')
        fn = [w for r in
            subprocess.check_output(list_cmd + [output_path]).split('\n')
            for w in r.split(' ') if w.startswith('part-00000')][0]
        cmd = move_cmd + [output_path + fn, output_path + 'pharmacy_claims_{}.psv'.format(group_id)]
        subprocess.check_call(cmd)

        enrollment_extract.where(F.col('humana_group_id') == F.lit(group_id)) \
            .drop('humana_group_id') \
            .repartition(1).write \
            .csv(output_path.replace('s3://', 's3a://'), sep='|', mode='append')
        fn = [w for r in
            subprocess.check_output(list_cmd + [output_path]).split('\n')
            for w in r.split(' ') if w.startswith('part-00000')][0]
        cmd = move_cmd + [output_path + fn, output_path + 'enrollment_{}.psv'.format(group_id)]
        subprocess.check_call(cmd)

def main(args):
    # init
    spark, sqlContext = init("Extract for Humana")

    # initialize runner
    runner = Runner(sqlContext)

    if args.airflow_test:
        in_queue  = 'https://queue.amazonaws.com/581191604223/humana-inbox-test'
        out_queue = 'https://queue.amazonaws.com/581191604223/humana-outbox-test'
    else:
        in_queue  = 'https://queue.amazonaws.com/581191604223/humana-inbox-prod'
        out_queue = 'https://queue.amazonaws.com/581191604223/humana-outbox-prod'

    client = boto3.client('sqs')
    while True:
        msgs = []
        while True:
            resp = client.receive_message(QueueUrl=in_queue, MaxNumberOfMessages=10, VisibilityTimeout=30)
            if 'Messages' not in resp:
                break
            msgs += [(m['Body'], m['ReceiptHandle']) for m in resp['Messages']]

        if not msgs:
            break

        run(spark, runner, set([m[0] for m in msgs]), airflow_test=args.airflow_test)

        for m in msgs:
            client.delete_message(QueueUrl=in_queue, ReceiptHandle=m[1])

        for m in set(m[0] for m in msgs]):
            client.send_message(QueueUrl=out_queue, MessageBody=m)

    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--airflow_test', default=False, action='store_true')
    args = parser.parse_args()
    main(args)
