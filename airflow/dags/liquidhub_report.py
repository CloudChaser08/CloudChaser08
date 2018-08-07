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
DAG_NAME = 'liquidhub_report'

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2018, 7, 24),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2)
}

mdag = HVDAG.HVDAG(
    dag_id=DAG_NAME,
    schedule_interval='0 0 * * *',
    default_args=default_args
)

TMP_PATH_TEMPLATE = '/tmp/liquidhub/report/{}/'

# Applies to all transaction files
if HVDAG.HVDAG.airflow_env == 'test':
    S3_TRANSACTION_RAW_URL = 's3://salusv/testing/dewey/airflow/e2e/liquidhub/raw/'
    S3_NORMALIZED_FILE_URL_TEMPLATE = 's3://salusv/testing/dewey/airflow/e2e/liquidhub/deliverable/{}/'
else:
    S3_TRANSACTION_RAW_URL = 's3://healthverity/incoming/lh_amgen_hv/'
    S3_NORMALIZED_FILE_URL_TEMPLATE = 's3://salusv/deliverable/lhv2/{}/'

DEID_FILE_NAME_TEMPLATE = '{}_output.txt'

# Return files
SUMMARY_TEMPLATE = 'summary_report_{}.txt'
RECIPIENTS = [
    'John Cappiello<jcappiello@healthverity.com>',
    'Cree Flory <cflory@healthverity.com>',
    'Lauren Langhauser <lfrancis@healthverity.com>',
    'Ilia Fishbein<ifishbein@healthverity.com>'
]

def get_opp_id(source, manufacturer, version):
    OPP_ID_MAPPING = {
        ('amgen', None, 'lhv2')      : 'HV-000387',
        ('novartis', None, 'lhv2')   : 'HV-000728',
        ('amgen', 'accredo', 'lhv1') : 'HV-000213',
        ('amgen', 'cigna', 'lhv1')   : 'HV-000213',
        ('amgen', 'briova', 'lhv1')  : 'HV-000213',
        ('amgen', None, 'lhv1')      : 'HV-000213',
        ('lilly', None, 'lhv2')      : 'HV-000637',
        ('lilly', 'accredo', 'lhv2') : 'HV-000637',
        ('lilly', 'cigna', 'lhv2')   : 'HV-000637'
    }

    opp_id = OPP_ID_MAPPING.get((manufacturer.lower(), source.lower(), version.lower()))
    if opp_id:
        return opp_id

    return OPP_ID_MAPPING.get((manufacturer.lower(), None, version.lower()), 'UNKNOWN')

def get_delivered_groups(exec_date):
    session = settings.Session()
    deliveries = session.query(TaskInstance) \
            .filter(TaskInstance.dag_id == 'liquidhub_delivery_pipeline', TaskInstance.task_id == 'deliver_return_file') \
            .all()
    groups = []

    for deliv in deliveries:
        if exec_date.date() == deliv.end_date.date():
            gid = session.query(DagRun) \
                .filter(DagRun.dag_id == 'liquidhub_delivery_pipeline', DagRun.execution_date == deliv.execution_date) \
                .one().conf['group_id']
            groups.append({'id': gid, 'delivery_ts': deliv.end_date})

    if HVDAG.HVDAG.airflow_env == 'test':
        return [
            {'id': 'LHV2_LiquidHub_PatDemo_20180720_v1', 'delivery_ts': datetime(2018, 7, 20, 15, 5, 8)},
            {'id': 'LHV2_LiquidHub_PatDemo_20180720_v2', 'delivery_ts': datetime(2018, 7, 20, 15, 7, 43)},
            {'id': 'LHV2_LiquidHub_PatDemo_20180720_v3', 'delivery_ts': datetime(2018, 7, 20, 15, 9, 41)},
            {'id': 'LHV2_LiquidHub_PatDemo_20180720_v4', 'delivery_ts': datetime(2018, 7, 20, 15, 11, 28)},
            {'id': 'LHV2_LiquidHub_PatDemo_20180720_v5', 'delivery_ts': datetime(2018, 7, 20, 15, 13, 16)},
            {'id': 'LHV2_LiquidHub_PatDemo_20180720_v6', 'delivery_ts': datetime(2018, 7, 20, 15, 15, 37)}
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

def do_fetch_summaries(ds, **kwargs):
    groups = get_delivered_groups(kwargs['execution_date']) + get_delivered_groups_past_month(kwargs['execution_date'])
    for group in groups:
        fn   = SUMMARY_TEMPLATE.format(group['id'])
        src  = S3_NORMALIZED_FILE_URL_TEMPLATE.format(group['id']) + fn
        dest = get_tmp_dir(ds, kwargs) + fn

        s3_utils.fetch_file_from_s3(src, dest)

fetch_summaries = PythonOperator(
    task_id='fetch_summaries',
    provide_context=True,
    python_callable=do_fetch_summaries,
    dag=mdag
)

def get_group_received_ts(group_id):
    path = S3_TRANSACTION_RAW_URL + DEID_FILE_NAME_TEMPLATE.format(group_id)
    return s3_utils.get_file_modified_date(path)

def do_generate_daily_report(ds, **kwargs):
    groups = get_delivered_groups(kwargs['execution_date'])
    source_manufacturer_file_count = {}

    daily_report_file = open(get_tmp_dir(ds, kwargs) + 'daily_report_' + kwargs['ds_nodash'] + '.csv', 'w')
    report_writer = csv.writer(daily_report_file, quoting=csv.QUOTE_MINIMAL)
    if groups:
        report_writer.writerow(['File ID', 'Date Received', 'Date Returned'])
    for group in groups:
        feed_version = group['id'].split('_')[0]
        fn = SUMMARY_TEMPLATE.format(group['id'])
        f  = get_tmp_dir(ds, kwargs) + fn
        received_ts  = get_group_received_ts(group['id'])
        delivered_ts = group['delivery_ts']
        report_writer.writerow([group['id'], received_ts.isoformat(), delivered_ts.isoformat()])
        with open(f) as fin:
            for line in fin:
                fields = tuple(line.strip().split('|') + [feed_version])
                if fields not in source_manufacturer_file_count:
                    source_manufacturer_file_count[fields] = 0
                source_manufacturer_file_count[fields] += 1

    if groups:
        report_writer.writerow([])
        report_writer.writerow([])

    report_writer.writerow(['Opp ID', 'Source', 'Manufacturer', 'File Count'])
    for (key, val) in source_manufacturer_file_count.items():
        report_writer.writerow([get_opp_id(*key), key[0], key[1], val])

    if groups:
        report_writer.writerow([])
        report_writer.writerow([])
    report_writer.writerow(['TOTAL', '-', '-', len(groups)])

    daily_report_file.close()

generate_daily_report = PythonOperator(
    task_id='generate_daily_report',
    provide_context=True,
    python_callable=do_generate_daily_report,
    dag=mdag
)

def do_generate_monthly_report(ds, **kwargs):
    groups = get_delivered_groups_past_month(kwargs['execution_date'])
    source_manufacturer_file_count = {}

    month = (kwargs['execution_date'] - timedelta(days=1)).strftime('%B')

    monthly_report_file = open(get_tmp_dir(ds, kwargs) + month +'_monthly_report.csv', 'w')
    report_writer = csv.writer(monthly_report_file, quoting=csv.QUOTE_MINIMAL)
    if groups:
        report_writer.writerow(['File ID', 'Date Received', 'Date Returned'])
    for group in groups:
        feed_version = group['id'].split('_')[0]
        fn = SUMMARY_TEMPLATE.format(group['id'])
        f  = get_tmp_dir(ds, kwargs) + fn
        received_ts  = get_group_received_ts(group['id'])
        delivered_ts = group['delivery_ts']
        report_writer.writerow([group['id'], received_ts.isoformat(), delivered_ts.isoformat()])
        with open(f) as fin:
            for line in fin:
                fields = tuple(line.strip().split('|') + [feed_version])
                if fields not in source_manufacturer_file_count:
                    source_manufacturer_file_count[fields] = 0
                source_manufacturer_file_count[fields] += 1

    if groups:
        report_writer.writerow([])
        report_writer.writerow([])

    report_writer.writerow(['Opp ID', 'Source', 'Manufacturer', 'File Count'])
    for (key, val) in source_manufacturer_file_count.items():
        report_writer.writerow([get_opp_id(*key), key[0], key[1], val])

    if groups:
        report_writer.writerow([])
        report_writer.writerow([])
    report_writer.writerow(['TOTAL', '-', '-', len(groups)])

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
        'LiquidHub Delivery Report for ' + ds,
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

fetch_summaries.set_upstream(create_tmp_dir)
generate_daily_report.set_upstream(fetch_summaries)
generate_monthly_report.set_upstream(fetch_summaries)
email_report.set_upstream([generate_daily_report, generate_monthly_report])
clean_up_workspace.set_upstream(email_report)
