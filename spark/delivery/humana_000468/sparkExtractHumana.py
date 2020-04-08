#! /usr/bin/python
import argparse
import time
import subprocess
from datetime import timedelta, date
from spark.runner import Runner
from spark.spark_setup import init
import spark.helpers.file_utils as file_utils
import spark.helpers.payload_loader as payload_loader
import spark.helpers.normalized_records_unloader as normalized_records_unloader
from pyspark.sql.types import StructType, StructField, StringType
import pyspark.sql.functions as F
import boto3

from spark.delivery.humana_000468 import extract_medicalclaims
from spark.delivery.humana_000468 import extract_pharmacyclaims
from spark.delivery.humana_000468 import extract_enrollmentrecords
from spark.delivery.humana_000468 import prepare_emr

def get_extract_summary(df):
    return df.withColumn(
        'claim',
        F.when(
            F.length(F.trim(F.col('claim_id'))) > 0,
            F.col('claim_id')
        ).otherwise(F.col('record_id'))
    ) \
        .select('data_vendor', 'claim', 'humana_group_id').distinct() \
        .groupBy('data_vendor', 'humana_group_id').count()

def get_part_file_path(list_cmd, directory):
    for row in subprocess.check_output(list_cmd + [directory]).decode().split('\n'):
        file_path = row.split(' ')[-1].replace('//', '/')
        prefix = (directory + 'part-00000')
        if file_path.startswith(prefix):
            return file_path

def group_validity_check(group_dfs, group_ids):
    """
    A group is valid if it contains at least 10 valid records.

    A record is valid as long as it has an entry for first clusterA.

    If a row in matching payload does not contain invalidReason, then the record contains clusterA.
    If a row in matching payload does contain invalidReason AND invalidReason['clusterA'] is True,
        then the record contains clusterA.
    """
    valid_groups = []
    invalid_groups = []
    for group_df, group_id in zip(group_dfs, group_ids):
        total_patient_count = group_df.count()
        invalid_patient_count = 0
        if ('invalidReason', 'string') not in group_df.dtypes:
            invalid_patients = group_df.where(~group_df.invalidReason.isNull())
            invalid_patients = invalid_patients.where(invalid_patients['invalidReason']['clusterA'] == False).cache()

            invalid_patient_count = invalid_patients.count()

        if total_patient_count - invalid_patient_count < 10:
            invalid_groups.append(group_id)
        else:
            valid_groups.append(group_id)

    return valid_groups, invalid_groups

def run(spark, runner, group_ids, test=False, airflow_test=False, is_prod=False):
    ts = time.time()
    today = date.today()

    if airflow_test:
        output_path_template = '/staging/{}/'
        matching_path_template = 's3a://salusv/testing/dewey/airflow/e2e/humana/hv000468/payload/{}/'
        list_cmd = ['hadoop', 'fs', '-ls']
        move_cmd = ['hadoop', 'fs', '-mv']
    elif test:
        output_path_template = file_utils.get_abs_path(
            __file__, '../../test/delivery/humana/hv000468/out/{}/'
        ) + '/'
        for group_id in group_ids:
            subprocess.check_call(['mkdir', '-p', output_path_template.format(group_id)])
        matching_path_template = file_utils.get_abs_path(
            __file__, '../../test/delivery/humana/hv000468/resources/matching/{}/'
        ) + '/'
        list_cmd = ['find']
        move_cmd = ['mv']

        # Need to be able to test consistantly
        today = date(2018, 4, 26)
        ts = 1524690702.12345
    else:
        output_path_template = '/staging/{}/'
        matching_path_template = 's3a://salusv/matching/payload/custom/humana/hv000468/{}/'
        list_cmd = ['hadoop', 'fs', '-ls']
        move_cmd = ['hadoop', 'fs', '-mv']


    group_dfs = [
        payload_loader.load(runner, matching_path_template.format(group_id), ['matchStatus', 'invalidReason'], return_output=True,
                            partitions=10) \
            .select('hvid', 'matchStatus', 'invalidReason') \
            .withColumn('humana_group_id', F.lit(group_id))
        for group_id in group_ids
    ]

    valid_groups, invalid_groups = group_validity_check(group_dfs, group_ids)

    all_patient_dfs = [
        payload_loader.load(runner, matching_path_template.format(group_id), ['matchStatus'], return_output=True,
                            partitions=10) \
            .select('hvid', 'matchStatus') \
            .withColumn('humana_group_id', F.lit(group_id))
        for group_id in group_ids
    ]

    all_patients = all_patient_dfs[0]
    for patients_df in all_patient_dfs[1:]:
        all_patients = all_patients.union(patients_df)

    all_patients.repartition(100).checkpoint().cache()
    matched_patients = all_patients.where("matchStatus = 'exact_match' or matchStatus = 'inexact_match'").cache()

    if today.day > 15:
        end = (today.replace(day=15) - timedelta(days=30)).replace(day=1) # The 1st about 1.5 months back
        start = (end - timedelta(days=455)).replace(day=1) # 15 months before end
    else:
        end = (today.replace(day=15) - timedelta(days=60)).replace(day=15) # The 15th about 1.5 months back
        start = (end - timedelta(days=455)).replace(day=15) # 15 months before end

    group_all_patient_count = \
        {r.humana_group_id : r['count'] for r in all_patients.groupBy('humana_group_id').count().collect()}
    group_matched_patient_count = \
        {r.humana_group_id : r['count'] for r in matched_patients.groupBy('humana_group_id').count().collect()}

    if invalid_groups:
        medical_extract = spark.createDataFrame([("NONE",)], ['humana_group_id'])
        pharmacy_extract = spark.createDataFrame([("NONE",)], ['humana_group_id'])
        enrollment_extract = spark.createDataFrame([("NONE",)], ['humana_group_id'])

    matched_patients = (matched_patients.where(F.col('humana_group_id').isin(valid_groups)))

    group_patient_w_records_count = {}
    summary = None
    if valid_groups:

#   This is comented out until Humana wants us to turn synthetic claims back on
        # prepare_emr.prepare(runner, matched_patients, start, is_prod)
        medical_extract = extract_medicalclaims.extract(
            runner, matched_patients, ts,
            start, end).repartition(5 if test else 100) \
                    .cache_and_track('medical_extract')
        pharmacy_extract = extract_pharmacyclaims.extract(
            runner, matched_patients, ts,
            start, end).repartition(5 if test else 100) \
                    .cache_and_track('pharmacy_extract')
        enrollment_extract = extract_enrollmentrecords.extract(
            spark, runner, matched_patients, ts,
            start, end, pharmacy_extract).repartition(5 if test else 100) \
                    .cache_and_track('enrollment_extract')

        # summary
        med_summary = get_extract_summary(medical_extract)
        pharma_summary = get_extract_summary(pharmacy_extract)

        summary = med_summary.union(pharma_summary)
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

    for group_id in group_ids:
        all_patient_count = group_all_patient_count.get(group_id, 0)
        matched_patient_count = group_matched_patient_count.get(group_id, 0)
        patient_w_records_count = group_patient_w_records_count.get(group_id, 0)
        summary_report = '\n'.join(['|'.join([
            group_id, str(all_patient_count), str(matched_patient_count),
            str(patient_w_records_count), r[0], str(r[1])
        ]) for r in local_summary.get(group_id, [('-', 0)])])

        output_path = output_path_template.format(group_id)
        with open('/tmp/summary_report_{}.txt'.format(group_id), 'w') as outf:
            outf.write(summary_report)

        medical_extract.where(F.col('humana_group_id') == F.lit(group_id)) \
            .drop('humana_group_id') \
            .repartition(1).write \
            .csv(output_path, sep='|', mode='overwrite')
        fn = get_part_file_path(list_cmd, output_path)
        cmd = move_cmd + [fn, output_path + 'medical_claims_{}.psv'.format(group_id)]
        subprocess.check_call(cmd)

        pharmacy_extract.where(F.col('humana_group_id') == F.lit(group_id)) \
            .drop('humana_group_id') \
            .repartition(1).write \
            .csv(output_path, sep='|', mode='append')
        fn = get_part_file_path(list_cmd, output_path)
        cmd = move_cmd + [fn, output_path + 'pharmacy_claims_{}.psv'.format(group_id)]
        subprocess.check_call(cmd)

        enrollment_extract.where(F.col('humana_group_id') == F.lit(group_id)) \
            .drop('humana_group_id') \
            .repartition(1).write \
            .csv(output_path, sep='|', mode='append')
        fn = get_part_file_path(list_cmd, output_path)
        cmd = move_cmd + [fn, output_path + 'enrollment_{}.psv'.format(group_id)]
        subprocess.check_call(cmd)
        
        if not test:
            cmd = ['hadoop', 'fs', '-put', '/tmp/summary_report_{}.txt'.format(group_id), output_path + 'summary_report_{0}.txt'.format(group_id)]
            subprocess.check_call(cmd)

def main(args):
    # init
    spark, sqlContext = init("Extract for Humana")

    # initialize runner
    runner = Runner(sqlContext)

    if args.airflow_test:
        output_path = 's3a://salusv/testing/dewey/airflow/e2e/humana/hv000468/deliverable/'
        in_queue = 'https://queue.amazonaws.com/581191604223/humana-inbox-test'
        out_queue = 'https://queue.amazonaws.com/581191604223/humana-outbox-test'
    elif args.is_prod:
        output_path = 's3a://salusv/deliverable/humana/hv000468/'
        in_queue = 'https://queue.amazonaws.com/581191604223/humana-inbox-prod'
        out_queue = 'https://queue.amazonaws.com/581191604223/humana-outbox-prod'
    else:
        output_path = 's3a://salusv/deliverable/humana/hv000468/'
        in_queue = 'https://queue.amazonaws.com/581191604223/humana-inbox-uat'
        out_queue = 'https://queue.amazonaws.com/581191604223/humana-outbox-uat'

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

        run(spark, runner, set([m[0] for m in msgs]), airflow_test=args.airflow_test, is_prod=args.is_prod)

        spark.stop()

        normalized_records_unloader.distcp(output_path, '/staging/')

        for m in msgs:
            client.delete_message(QueueUrl=in_queue, ReceiptHandle=m[1])

        for m in set([m[0] for m in msgs]):
            client.send_message(QueueUrl=out_queue, MessageBody=m)

        spark, sqlContext = init("Extract for Humana")
        runner = Runner(sqlContext)

    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--airflow_test', default=False, action='store_true')
    parser.add_argument('--is_prod', default=False, action='store_true')
    args = parser.parse_args()
    main(args)
