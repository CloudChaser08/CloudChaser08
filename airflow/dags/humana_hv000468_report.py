from airflow.operators import *
from airflow.models import Variable, DagRun, TaskInstance
from airflow import settings
from datetime import datetime, timedelta

# hv-specific modules
import common.HVDAG as HVDAG
import subdags.detect_move_normalize as detect_move_normalize
import subdags.s3_push_files as s3_push_files

import util.s3_utils as s3_utils
import util.sftp_utils as sftp_utils

import logging
import json
import os
import csv
import shutil

for m in [s3_push_files, detect_move_normalize, HVDAG,
        s3_utils, sftp_utils]:
    reload(m)

# Applies to all files
DAG_NAME = 'humana_hv000468_report'

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2018, 4, 26, 12),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2)
}

mdag = HVDAG.HVDAG(
    dag_id=DAG_NAME,
    schedule_interval='21 7 * * *',
    default_args=default_args
)

TMP_PATH_TEMPLATE = '/tmp/humana/hv000468_report/{}/'

# Applies to all transaction files
if HVDAG.HVDAG.airflow_env == 'test':
    S3_TRANSACTION_RAW_URL = 's3://salusv/testing/dewey/airflow/e2e/humana/hv000468/raw/'
    S3_NORMALIZED_FILE_URL_TEMPLATE = 's3://salusv/testing/dewey/airflow/e2e/humana/hv000468/deliverable/{}/'
else:
    S3_TRANSACTION_RAW_URL = 's3://healthverity/incoming/humana/'
    S3_NORMALIZED_FILE_URL_TEMPLATE = 's3://salusv/deliverable/humana/hv000468/{}/'

DEID_FILE_NAME_TEMPLATE = 'deid_data_{}'

# Return files
MEDICAL_CLAIMS_EXTRACT_TEMPLATE = 'medical_claims_{}.psv.gz'
PHARMACY_CLAIMS_EXTRACT_TEMPLATE = 'pharmacy_claims_{}.psv.gz'
ENROLLMENT_EXTRACT_TEMPLATE = 'enrollment_{}.psv.gz'
EXTRACT_SUMMARY_TEMPLATE = 'summary_report_{}.txt'

def get_delivered_groups(exec_date):
    session = settings.Session()
    deliveries = session.query(TaskInstance) \
            .filter(TaskInstance.dag_id == 'humana_hv000468_delivery', TaskInstance.task_id == 'deliver_extracted_data') \
            .all()
    groups = []
    for deliv in deliveries:
        if exec_date.date() == deliv.end_date.date():
            gid = session.query(DagRun) \
                .filter(DagRun.dag_id == 'humana_hv000468_delivery', DagRun.execution_date == deliv.execution_date) \
                .one().conf['group_id']
            groups.append({'id': gid, 'delivery_ts': deliv.end_date})

    if HVDAG.HVDAG.airflow_env == 'test':
        return [
            {'id': '01234567', 'delivery_ts': datetime(2018, 4, 30, 22, 5, 8)},
            {'id': '87654322', 'delivery_ts': datetime(2018, 4, 30, 22, 7, 43)}
        ]
    return groups

def get_tmp_dir(ds, kwargs):
    return TMP_PATH_TEMPLATE.format(kwargs['ds_nodash'])

def do_create_tmp_dir(ds, **kwargs):
    os.makedirs(get_tmp_dir(ds, kwargs))

create_tmp_dir = PythonOperator(
    task_id='create_tmp_dir',
    provide_context=True,
    python_callable=do_create_tmp_dir,
    dag=mdag
)

def do_fetch_extract_summaries(ds, **kwargs):
    groups = get_delivered_groups(kwargs['execution_date'])
    for group in groups:
        fn   = EXTRACT_SUMMARY_TEMPLATE.format(group['id'])
        src  = S3_NORMALIZED_FILE_URL_TEMPLATE.format(group['id']) + fn
        dest = get_tmp_dir(ds, kwargs) + fn

        s3_utils.copy_file(src, dest)

fetch_extract_summaries = PythonOperator(
    task_id='fetch_extract_summaries',
    provide_context=True,
    python_callable=do_fetch_extract_summaries,
    dag=mdag
)

def get_group_received_ts(group_id):
    path = S3_TRANSACTION_RAW_URL + DEID_FILE_NAME_TEMPLATE.format(group_id)
    return s3_utils.get_file_modified_date(path)

def do_generate_daily_report(ds, **kwargs):
    groups = get_delivered_groups(kwargs['execution_date'])
    total = {'records': {}, 'patients': 0, 'matched': 0}

    daily_report_file = open(get_tmp_dir(ds, kwargs) + 'daily_report_' + kwargs['ds_nodash'] + '.csv', 'w')
    report_writer = csv.writer(daily_report_file, quoting=csv.QUOTE_MINIMAL)
    report_writer.writerow(['Group ID', 'Date Received', 'Date Delivered',
        'Patients Sent', 'Patients Matched', 'Source', 'Source Record Count'])
    for group in groups:
        fn = EXTRACT_SUMMARY_TEMPLATE.format(group['id'])
        f  = get_tmp_dir(ds, kwargs) + fn
        received_ts  = get_group_received_ts(group['id'])
        delivered_ts = group['delivery_ts']
        with open(f) as fin:
            for line in fin:
                fields = line.strip().split('|')
                patients = int(fields[1])
                matched  = int(fields[2])
                total['records'][fields[3]] = total['records'].get(fields[3], 0) + int(fields[4])
                report_writer.writerow([group['id'], received_ts.isoformat(), delivered_ts.isoformat()] + fields[1:])
        total['patients'] += patients
        total['matched']  += matched

    report_writer.writerow([])
    report_writer.writerow([])
    for source in total['records'].keys():
        report_writer.writerow(['TOTAL', received_ts.isoformat(), delivered_ts.isoformat(),
            total['patients'], total['matched'], source, total['records'][source]])
    if not total['records'].keys():
        report_writer.writerow(['TOTAL', '-', '-',
            total['patients'], total['matched'], '-', 0])

    daily_report_file.close()
    shutil.copy2(get_tmp_dir(ds, kwargs) + 'daily_report_' + kwargs['ds_nodash'] + '.csv', '/tmp/humana/daily_report.csv')

generate_daily_report = PythonOperator(
    task_id='generate_daily_report',
    provide_context=True,
    python_callable=do_generate_daily_report,
    dag=mdag
)

email_daily_report = EmailOperator(
    task_id='email_daily_report',
    to=['ifishbein@healthverity.com'], # PLACEHOLDER
    subject='Humana Delivery Report for {{ ds }}',
    files=['/tmp/humana/daily_report.csv'],
    html_content='',
    dag=mdag
)

def do_clean_up_workspace(ds, **kwargs):
    shutil.rmtree(get_tmp_dir(ds, kwargs))

clean_up_workspace = PythonOperator(
    task_id='clean_up_workspace',
    provide_context=True,
    python_callable=do_clean_up_workspace,
    trigger_rule='all_done',
    dag=mdag
)

if HVDAG.HVDAG.airflow_env == 'test':
    for t in ['email_daily_report']:
        del mdag.task_dict[t]
        globals()[t] = DummyOperator(
            task_id = t,
            dag = mdag
        )

fetch_extract_summaries.set_upstream(create_tmp_dir)
generate_daily_report.set_upstream(fetch_extract_summaries)
email_daily_report.set_upstream(generate_daily_report)
clean_up_workspace.set_upstream(email_daily_report)
