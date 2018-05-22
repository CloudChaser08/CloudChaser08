from airflow.operators import *
from airflow.models import Variable, DagRun, TaskInstance
from airflow import settings
from datetime import datetime, timedelta

# hv-specific modules
import common.HVDAG as HVDAG

import util.s3_utils as s3_utils
import util.ses_utils as ses_utils

import os
import csv
import shutil

for m in [HVDAG, s3_utils, ses_utils]:
    reload(m)

# Applies to all files
DAG_NAME = 'humana_hv000468_report'

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2018, 5, 15),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2)
}

mdag = HVDAG.HVDAG(
    dag_id=DAG_NAME,
    schedule_interval='0 0 * * *',
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

DEID_FILE_NAME_TEMPLATE = '{}_deid.txt'

# Return files
MEDICAL_CLAIMS_EXTRACT_TEMPLATE = 'medical_claims_{}.psv.gz'
PHARMACY_CLAIMS_EXTRACT_TEMPLATE = 'pharmacy_claims_{}.psv.gz'
ENROLLMENT_EXTRACT_TEMPLATE = 'enrollment_{}.psv.gz'
EXTRACT_SUMMARY_TEMPLATE = 'summary_report_{}.txt'
RECIPIENTS = [
    'Andrew Goldberg<agoldberg@healthverity.com>',
    'John Cappiello<jcappiello@healthverity.com>',
    'Ilia Fishbein<ifishbein@healthverity.com>', 
    'Justin Newton<jnewton@humana.com>',
    'Evan Neises<eneises1@humana.com>',
    'Brandon Schoenfeldt<bschoenfeldt@humana.com>'
]

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

        s3_utils.fetch_file_from_s3(src, dest)

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
    total = {'records': {}, 'patients': 0, 'matched': 0, 'w_records': 0}

    daily_report_file = open(get_tmp_dir(ds, kwargs) + 'daily_report_' + kwargs['ds_nodash'] + '.csv', 'w')
    report_writer = csv.writer(daily_report_file, quoting=csv.QUOTE_MINIMAL)
    report_writer.writerow(['Group ID', 'Date Received', 'Date Delivered',
        'Patients Sent', 'Patients Matched', 'Patients with Records', 'Source',
        'Source Record Count'])
    for group in groups:
        fn = EXTRACT_SUMMARY_TEMPLATE.format(group['id'])
        f  = get_tmp_dir(ds, kwargs) + fn
        received_ts  = get_group_received_ts(group['id'])
        delivered_ts = group['delivery_ts']
        with open(f) as fin:
            for line in fin:
                fields = line.strip().split('|')
                patients  = int(fields[1])
                matched   = int(fields[2])
                w_records = int(fields[3])
                total['records'][fields[4]] = total['records'].get(fields[4], 0) + int(fields[5])
                report_writer.writerow([group['id'], received_ts.isoformat(), delivered_ts.isoformat()] + fields[1:])
        total['patients']  += patients
        total['matched']   += matched
        total['w_records'] += w_records

    if len(total['records'].keys()) > 1 and '-' in total['records']:
        del total['records']['-']

    report_writer.writerow([])
    report_writer.writerow([])
    for source in total['records'].keys():
        report_writer.writerow(['TOTAL', received_ts.isoformat(), delivered_ts.isoformat(),
            total['patients'], total['matched'], total['w_records'], source, total['records'][source]])
    if not total['records'].keys():
        report_writer.writerow(['TOTAL', '-', '-',
            total['patients'], total['matched'], '-', 0])

    daily_report_file.close()

generate_daily_report = PythonOperator(
    task_id='generate_daily_report',
    provide_context=True,
    python_callable=do_generate_daily_report,
    dag=mdag
)

def do_email_daily_report(ds, **kwargs):
    ses_utils.send_email(
        'delivery-receipts@healthverity.com',
        RECIPIENTS,
        'Humana Delivery Report for ' + ds,
        '',
        [get_tmp_dir(ds, kwargs) + 'daily_report_' + kwargs['ds_nodash'] + '.csv']
    )

email_daily_report = PythonOperator(
    task_id='email_daily_report',
    provide_context=True,
    python_callable=do_email_daily_report,
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
