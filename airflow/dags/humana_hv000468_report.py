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
    S3_TRANSACTION_RAW_URL_UAT = 's3://healthverity/incoming/humana_uat/'
    S3_NORMALIZED_FILE_URL_TEMPLATE = 's3://salusv/deliverable/humana/hv000468/{}/'

DEID_FILE_NAME_TEMPLATE = '{}_deid.txt'

# Return files
EXTRACT_SUMMARY_TEMPLATE = 'summary_report_{}.txt'
RECIPIENTS = [
    'Andrew Goldberg<agoldberg@healthverity.com>',
    'John Cappiello<jcappiello@healthverity.com>',
    'Ilia Fishbein<ifishbein@healthverity.com>',
    'Humana Risk Assessment Team<HealthVerity_AutomatedRiskAssessment@humana.com>'
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
            groups.append({'id': gid, 'delivery_ts': deliv.end_date if deliv.state == "success" else None})

    if HVDAG.HVDAG.airflow_env == 'test':
        return [
            {'id': '01234567', 'delivery_ts': datetime(2018, 4, 30, 22, 5, 8)},
            {'id': '87654322', 'delivery_ts': datetime(2018, 4, 30, 22, 7, 43)}
        ]
    return groups

def get_delivered_groups_past_month(exec_date):
    groups = []
    if exec_date.day == 1:
        last_month_start = (exec_date - timedelta(days=1)).replace(day=1)
        days = 0
        while last_month_start + timedelta(days=days) < exec_date:
            groups.extend(get_delivered_groups(last_month_start + timedelta(days)))
            days += 1
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
    groups = get_delivered_groups(kwargs['execution_date']) + get_delivered_groups_past_month(kwargs['execution_date'])
    for group in groups:
        if group['delivery_ts'] is None:
            continue

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
    if 'UAT-' in group_id:
        path = S3_TRANSACTION_RAW_URL_UAT + DEID_FILE_NAME_TEMPLATE.format(group_id.replace('UAT-',''))
    else:
        path = S3_TRANSACTION_RAW_URL + DEID_FILE_NAME_TEMPLATE.format(group_id)
    return s3_utils.get_file_modified_date(path)

def do_generate_daily_report(ds, **kwargs):
    groups = get_delivered_groups(kwargs['execution_date'])
    total = {
        'Production' : {'records': {}, 'patients': 0, 'matched': 0, 'w_records': 0},
        'UAT'        : {'records': {}, 'patients': 0, 'matched': 0, 'w_records': 0}
    }

    daily_report_file = open(get_tmp_dir(ds, kwargs) + 'daily_report_' + kwargs['ds_nodash'] + '.csv', 'w')
    report_writer = csv.writer(daily_report_file, quoting=csv.QUOTE_MINIMAL)
    report_writer.writerow(['Group ID', 'Environment', 'Date Received', 'Date Delivered',
        'Patients Sent', 'Patients Matched', 'Patients with Records', 'Source',
        'Source Record Count'])
    for group in groups:
        fn = EXTRACT_SUMMARY_TEMPLATE.format(group['id'])
        f  = get_tmp_dir(ds, kwargs) + fn
        received_ts  = get_group_received_ts(group['id'])
        delivered_ts = group['delivery_ts']
        env = 'UAT' if ('UAT-' in f or ds < '2018-07-01') else 'Production'
        with open(f) as fin:
            for line in fin:
                fields = line.strip().split('|')
                # Early versions of the delivery summary did not include the "w/records" count
                fields.insert(3, 0) if ds < '2018-05-21' else None
                patients  = int(fields[1])
                matched   = int(fields[2])
                w_records = int(fields[3])
                total[env]['records'][fields[4]] = total[env]['records'].get(fields[4], 0) + int(fields[5])
                if delivered_ts:
                    report_writer.writerow([group['id'], env, received_ts.isoformat(), delivered_ts.isoformat()] + fields[1:])
                else:
                    report_writer.writerow([group['id'], env, received_ts.isoformat(), "Error processing file"] + fields[1:])
        total[env]['patients']  += patients
        total[env]['matched']   += matched
        total[env]['w_records'] += w_records

    if len(total['Production']['records'].keys()) > 1 and '-' in total['Production']['records']:
        del total['Production']['records']['-']
    if len(total['UAT']['records'].keys()) > 1 and '-' in total['UAT']['records']:
        del total['UAT']['records']['-']

    report_writer.writerow([])
    report_writer.writerow([])
    for source in total['Production']['records'].keys():
        report_writer.writerow(['TOTAL', 'Production', '-', '-',
            total['Production']['patients'], total['Production']['matched'], total['Production']['w_records'], source, total['Production']['records'][source]])
    if not total['Production']['records'].keys():
        report_writer.writerow(['TOTAL', 'Production', '-', '-',
            total['Production']['patients'], total['Production']['matched'], total['Production']['w_records'], '-', 0])
    for source in total['UAT']['records'].keys():
        report_writer.writerow(['TOTAL', 'UAT', '-', '-',
            total['UAT']['patients'], total['UAT']['matched'], total['UAT']['w_records'], source, total['UAT']['records'][source]])
    if not total['UAT']['records'].keys():
        report_writer.writerow(['TOTAL', 'UAT', '-', '-',
            total['UAT']['patients'], total['UAT']['matched'], total['UAT']['w_records'], '-', 0])

    daily_report_file.close()

generate_daily_report = PythonOperator(
    task_id='generate_daily_report',
    provide_context=True,
    python_callable=do_generate_daily_report,
    dag=mdag
)

def do_generate_monthly_report(ds, **kwargs):
    groups = get_delivered_groups_past_month(kwargs['execution_date'])
    total = {
        'Production' : {'records': {}, 'patients': 0, 'matched': 0, 'w_records': 0},
        'UAT'        : {'records': {}, 'patients': 0, 'matched': 0, 'w_records': 0}
    }

    month = (kwargs['execution_date'] - timedelta(days=1)).strftime('%B')

    monthly_report_file = open(get_tmp_dir(ds, kwargs) + month +'_monthly_report.csv', 'w')
    report_writer = csv.writer(monthly_report_file, quoting=csv.QUOTE_MINIMAL)
    report_writer.writerow(['', 'Environment', 'Patients Sent', 'Patients Matched',
        'Patients with Records', 'Source', 'Source Record Count'])
    for group in groups:
        fn = EXTRACT_SUMMARY_TEMPLATE.format(group['id'])
        f  = get_tmp_dir(ds, kwargs) + fn
        env = 'UAT' if ('UAT-' in f or ds < '2018-07-01') else 'Production'
        with open(f) as fin:
            for line in fin:
                fields = line.strip().split('|')
                # Early versions of the delivery summary did not include the "w/records" count
                fields.insert(3, 0) if ds < '2018-05-21' else None
                patients  = int(fields[1])
                matched   = int(fields[2])
                w_records = int(fields[3])
                total[env]['records'][fields[4]] = total[env]['records'].get(fields[4], 0) + int(fields[5])
        total[env]['patients']  += patients
        total[env]['matched']   += matched
        total[env]['w_records'] += w_records

    if len(total['Production']['records'].keys()) > 1 and '-' in total['Production']['records']:
        del total['Production']['records']['-']
    if len(total['UAT']['records'].keys()) > 1 and '-' in total['UAT']['records']:
        del total['UAT']['records']['-']

    for source in total['Production']['records'].keys():
        report_writer.writerow(['TOTAL', 'Production',
            total['Production']['patients'], total['Production']['matched'], total['Production']['w_records'], source, total['Production']['records'][source]])
    if not total['Production']['records'].keys():
        report_writer.writerow(['TOTAL', 'Production',
            total['Production']['patients'], total['Production']['matched'], total['Production']['w_records'], '-', 0])
    for source in total['UAT']['records'].keys():
        report_writer.writerow(['TOTAL', 'UAT',
            total['UAT']['patients'], total['UAT']['matched'], total['UAT']['w_records'], source, total['UAT']['records'][source]])
    if not total['UAT']['records'].keys():
        report_writer.writerow(['TOTAL', 'UAT',
            total['UAT']['patients'], total['UAT']['matched'], total['UAT']['w_records'], '-', 0])

    monthly_report_file.close()

generate_monthly_report = PythonOperator(
    task_id='generate_monthly_report',
    provide_context=True,
    python_callable=do_generate_monthly_report,
    dag=mdag
)

def do_email_report(ds, **kwargs):
    report_files = [get_tmp_dir(ds, kwargs) + 'daily_report_' + kwargs['ds_nodash'] + '.csv']
    if kwargs['execution_date'].day == 1:
        month = (kwargs['execution_date'] - timedelta(days=1)).strftime('%B')
        report_files.append(get_tmp_dir(ds, kwargs) + month + '_monthly_report.csv')

    ses_utils.send_email(
        'delivery-receipts@healthverity.com',
        RECIPIENTS,
        'Humana Delivery Report for ' + ds,
        '', report_files
    )

email_report = PythonOperator(
    task_id='email_report',
    provide_context=True,
    python_callable=do_email_report,
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
    for t in ['email_report']:
        del mdag.task_dict[t]
        globals()[t] = DummyOperator(
            task_id = t,
            dag = mdag
        )

fetch_extract_summaries.set_upstream(create_tmp_dir)
generate_daily_report.set_upstream(fetch_extract_summaries)
generate_monthly_report.set_upstream(fetch_extract_summaries)
email_report.set_upstream([generate_daily_report, generate_monthly_report])
clean_up_workspace.set_upstream(email_report)
