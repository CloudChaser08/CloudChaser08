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

def get_extract_summary(df):
    return df.withColumn('claim',
                F.when(
                    F.length(F.trim(F.col('claim_id'))) > 0,
                    F.col('claim_id')
                ).otherwise(F.col('record_id'))) \
        .select('data_vendor', 'claim', 'humana_group_id').distinct() \
        .groupBy('data_vendor', 'humana_group_id').count()

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

    all_patients = reduce(lambda x, y: x.union(y),
        [
            payload_loader.load(runner, matching_path_template.format(group_id), ['matchStatus'], return_output=True,
                    partitions=10) \
                .select('hvid', 'matchStatus') \
                .withColumn('humana_group_id', F.lit(group_id))
            for group_id in group_ids
        ]
    ).repartition(100).cache()
    matched_patients = all_patients.where("matchStatus = 'exact_match' or matchStatus = 'inexact_match'").cache()

    if today.day > 15:
        end   = (today.replace(day=15) - timedelta(days=30)).replace(day=1) # The 1st about 1.5 months back
        start = (end - timedelta(days=455)).replace(day=1) # 15 months before end
    else:
        end   = (today.replace(day=15) - timedelta(days=60)).replace(day=15) # The 15th about 1.5 months back
        start = (end - timedelta(days=455)).replace(day=15) # 15 months before end

    group_all_patient_count       = \
        {r.humana_group_id : r['count'] for r in all_patients.groupBy('humana_group_id').count().collect()}
    group_matched_patient_count   = \
        {r.humana_group_id : r['count'] for r in matched_patients.groupBy('humana_group_id').count().collect()}

    (valid_groups, invalid_groups) = ([], [])
    for group_id in group_ids:
        if group_matched_patient_count.get(group_id, 0) < 10:
            invalid_groups.append(group_id)
        else:
            valid_groups.append(group_id)

    if invalid_groups:
        medical_extract    = spark.createDataFrame([("NONE",)], ['humana_group_id'])
        pharmacy_extract   = spark.createDataFrame([("NONE",)], ['humana_group_id'])
        enrollment_extract = spark.createDataFrame([("NONE",)], ['humana_group_id'])

    matched_w_count = matched_patients.groupBy('humana_group_id').count().where(F.col('count') >= F.lit(10))
    matched_patients = matched_patients.join(matched_w_count, 'humana_group_id', 'left_semi')

    group_patient_w_records_count = {}
    summary = None
    if valid_groups:
        medical_extract    = extract_medicalclaims.extract(
                runner, matched_patients, ts,
                start, end).repartition(100) \
                    .cache_and_track('medical_extract')
        pharmacy_extract   = extract_pharmacyclaims.extract(
                runner, matched_patients, ts,
                start, end).repartition(100) \
                    .cache_and_track('pharmacy_extract')
        enrollment_extract = extract_enrollmentrecords.extract(
                spark, runner, matched_patients, ts,
                start, end, pharmacy_extract).repartition(100) \
                    .cache_and_track('enrollment_extract')

        # summary
        med_summary    = get_extract_summary(medical_extract)
        pharma_summary = get_extract_summary(pharmacy_extract)

        summary        = med_summary.union(pharma_summary)
        patient_w_records_counts = medical_extract \
                .select('hvid', 'humana_group_id').union(
                    pharmacy_extract.select('hvid', 'humana_group_id')) \
                .distinct().groupBy('humana_group_id').count().collect()

        for r in patient_w_records_counts:
            if r['humana_group_id'] in valid_groups:
                group_patient_w_records_count[r.humana_group_id] = r['count']

    empty_summaries = [('-', group_id, 0) for group_id in group_ids if group_id not in group_patient_w_records_count]

    if empty_summaries:
        if summary:
            summary = summary.union(spark.createDataFrame(empty_summaries, summary.schema))
        else:
            summary = spark.createDataFrame(empty_summaries, ['data_vendor', 'humana_group_id', 'count'])

    # for easy testing
    if test:
        for group_id in group_ids:
            medical_extract.where(F.col('humana_group_id') == F.lit(group_id)) \
                .drop('humana_group_id') \
                .createOrReplaceTempView(group_id.replace('-', '_') + '_medical_extract')
            pharmacy_extract.where(F.col('humana_group_id') == F.lit(group_id)) \
                .drop('humana_group_id') \
                .createOrReplaceTempView(group_id.replace('-', '_') + '_pharmacy_extract')
            summary.where(F.col('humana_group_id') == F.lit(group_id)) \
                .drop('humana_group_id') \
                .createOrReplaceTempView(group_id.replace('-', '_') + '_summary')

    local_summary = {}
    for r in summary.collect():
        if r['humana_group_id'] not in local_summary:
            local_summary[r['humana_group_id']] = []
        local_summary[r['humana_group_id']].append((r['data_vendor'], r['count']))

    move_procs = []
    for group_id in group_ids:
        all_patient_count       = group_all_patient_count.get(group_id, 0)
        matched_patient_count   = group_matched_patient_count.get(group_id, 0)
        patient_w_records_count = group_patient_w_records_count.get(group_id, 0)
        summary_report = '\n'.join(['|'.join([
                group_id, str(all_patient_count), str(matched_patient_count),
                str(patient_w_records_count), r[0], str(r[1])
            ]) for r in local_summary.get(group_id, [('-', 0)])])

        output_path = output_path_template.format(group_id)
        with open('/tmp/summary_report_{}.txt'.format(group_id), 'w') as outf:
            outf.write(summary_report)
        cmd = move_cmd + ['/tmp/summary_report_{}.txt'.format(group_id), output_path]
        move_procs.append(subprocess.Popen(cmd, stderr=subprocess.PIPE))

        medical_extract.where(F.col('humana_group_id') == F.lit(group_id)) \
            .drop('humana_group_id') \
            .repartition(1).write \
            .csv(output_path.replace('s3://', 's3a://'), sep='|', mode='append')
        fn = [w for r in
            subprocess.check_output(list_cmd + [output_path]).split('\n')
            for w in r.split(' ') if w.startswith('part-00000')][0]
        cmd = move_cmd + [output_path + fn, output_path + 'medical_claims_{}.psv'.format(group_id)]
        move_procs.append(subprocess.Popen(cmd, stderr=subprocess.PIPE))

        pharmacy_extract.where(F.col('humana_group_id') == F.lit(group_id)) \
            .drop('humana_group_id') \
            .repartition(1).write \
            .csv(output_path.replace('s3://', 's3a://'), sep='|', mode='append')
        fn = [w for r in
            subprocess.check_output(list_cmd + [output_path]).split('\n')
            for w in r.split(' ') if w.startswith('part-00000')][0]
        cmd = move_cmd + [output_path + fn, output_path + 'pharmacy_claims_{}.psv'.format(group_id)]
        move_procs.append(subprocess.Popen(cmd, stderr=subprocess.PIPE))

        enrollment_extract.where(F.col('humana_group_id') == F.lit(group_id)) \
            .drop('humana_group_id') \
            .repartition(1).write \
            .csv(output_path.replace('s3://', 's3a://'), sep='|', mode='append')
        fn = [w for r in
            subprocess.check_output(list_cmd + [output_path]).split('\n')
            for w in r.split(' ') if w.startswith('part-00000')][0]
        cmd = move_cmd + [output_path + fn, output_path + 'enrollment_{}.psv'.format(group_id)]
        move_procs.append(subprocess.Popen(cmd, stderr=subprocess.PIPE))

    for p in move_procs:
        p.wait()
        if p.returncode != 0:
            print p.stderr.read()
            raise Exception("Subprocess returned with code {}".format(p.returncode))

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

        for m in set([m[0] for m in msgs]):
            client.send_message(QueueUrl=out_queue, MessageBody=m)

    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--airflow_test', default=False, action='store_true')
    args = parser.parse_args()
    main(args)
