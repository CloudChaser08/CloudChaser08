from airflow.operators import *
from airflow.models import Variable, DagRun
from airflow import settings
from datetime import datetime, timedelta

# hv-specific modules
import common.HVDAG as HVDAG
import subdags.detect_move_normalize as detect_move_normalize
import subdags.s3_push_files as s3_push_files

import util.s3_utils as s3_utils
import util.sftp_utils as sftp_utils
import util.sqs_utils as sqs_utils

import logging
import json
import os
import subprocess
import time
import random

for m in [s3_push_files, detect_move_normalize, HVDAG,
        s3_utils, sftp_utils, sqs_utils]:
    reload(m)

# Applies to all files
DAG_NAME = 'humana_hv000468_delivery'

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2018, 4, 26, 12),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2)
}

mdag = HVDAG.HVDAG(
    dag_id=DAG_NAME,
    schedule_interval=None,
    default_args=default_args
)

TMP_PATH_TEMPLATE = '/tmp/humana/hv000468/{}/'

# Applies to all transaction files
if HVDAG.HVDAG.airflow_env == 'test':
    S3_PAYLOAD_DEST = 's3://salusv/testing/dewey/airflow/e2e/humana/hv000468/payload/'
    S3_NORMALIZED_FILE_URL_TEMPLATE = 's3://salusv/testing/dewey/airflow/e2e/humana/hv000468/deliverable/{}/'
else:
    S3_PAYLOAD_DEST = 's3://salusv/matching/payload/custom/humana/hv000468/'
    S3_NORMALIZED_FILE_URL_TEMPLATE = 's3://salusv/deliverable/humana/hv000468/{}/'

# SQS
HUMANA_INBOX_PROD = 'https://sqs.us-east-1.amazonaws.com/581191604223/humana-inbox-prod'
HUMANA_OUTBOX_PROD = 'https://sqs.us-east-1.amazonaws.com/581191604223/humana-outbox-prod'

HUMANA_INBOX_UAT = 'https://sqs.us-east-1.amazonaws.com/581191604223/humana-inbox-uat'
HUMANA_OUTBOX_UAT = 'https://sqs.us-east-1.amazonaws.com/581191604223/humana-outbox-uat'

# Matching payloads
S3_PROD_MATCHING_URL='s3://salusv/matching/humana-matching-engine/payload/53769d77-189e-4d79-a5d4-d2d22d09331e/'

# Deid file
DEID_FILE_NAME_TEMPLATE = '{}_deid.txt'

# Return files
MEDICAL_CLAIMS_EXTRACT_TEMPLATE = 'medical_claims_{}.psv'
PHARMACY_CLAIMS_EXTRACT_TEMPLATE = 'pharmacy_claims_{}.psv'
ENROLLMENT_EXTRACT_TEMPLATE = 'enrollment_{}.psv'
RETURN_FILE_TEMPLATE = 'results_{}.tar.gz'

# Identify the group_id passed into this DagRun and push it to xcom
def do_get_group_id(ds, **kwargs):
    group_id = kwargs['dag_run'].conf['group_id']

    kwargs['ti'].xcom_push(key='group_id', value=group_id)

get_group_id = PythonOperator(
    task_id='get_group_id',
    provide_context=True,
    python_callable=do_get_group_id,
    dag=mdag
)

#
# Post-Matching
#

def get_expected_matching_files(ds, kwargs):
    group_id = kwargs['ti'].xcom_pull(dag_id=DAG_NAME, task_ids='get_group_id', key='group_id')

    expected_files = [
        DEID_FILE_NAME_TEMPLATE.format(group_id.replace('UAT-', ''))
    ]
    if HVDAG.HVDAG.airflow_env != 'prod':
        logging.info(expected_files)
        return []
    return expected_files

def do_detect_matching_done(ds, **kwargs):
    deid_files = get_expected_matching_files(ds, kwargs)
    s3_path_prefix = S3_PROD_MATCHING_URL
    template = '{}{}*DONE*'
    for deid_file in deid_files:
        s3_key = template.format(s3_path_prefix, deid_file)
        logging.info('Poking for key : {}'.format(s3_key))
        if not s3_utils.s3_key_exists(s3_key):
            raise ValueError('S3 key not found')

detect_matching_done = PythonOperator(
    task_id='detect_matching_done',
    provide_context=True,
    python_callable=do_detect_matching_done,
    retry_delay=timedelta(minutes=2),
    retries=180, #6 hours of retrying
    dag=mdag
)

def do_move_matching_payload(ds, **kwargs):
    deid_files = get_expected_matching_files(ds, kwargs)
    for deid_file in deid_files:
        s3_prefix = S3_PROD_MATCHING_URL + deid_file
        for payload_file in s3_utils.list_s3_bucket(s3_prefix):
            directory = kwargs['ti'].xcom_pull(dag_id=DAG_NAME, task_ids='get_group_id', key='group_id')
            s3_utils.copy_file(payload_file, S3_PAYLOAD_DEST + directory + '/' + payload_file.split('/')[-1])

move_matching_payload = PythonOperator(
    task_id='move_matching_payload',
    provide_context=True,
    python_callable=do_move_matching_payload,
    dag=mdag
)

def do_queue_for_extraction(ds, **kwargs):
    group_id = kwargs['ti'].xcom_pull(dag_id=DAG_NAME, task_ids='get_group_id', key='group_id')
    if group_id.startswith('UAT-'):
        sqs_utils.send_message(HUMANA_INBOX_UAT, group_id)
    else:
        sqs_utils.send_message(HUMANA_INBOX_PROD, group_id)

queue_for_extraction = PythonOperator(
    task_id='queue_for_extraction',
    provide_context=True,
    python_callable=do_queue_for_extraction,
    dag=mdag,
)

def detect_extraction_done(ds, **kwargs):
    group_id = kwargs['ti'].xcom_pull(dag_id=DAG_NAME, task_ids='get_group_id', key='group_id')
    outbox = HUMANA_OUTBOX_UAT if 'UAT-' else HUMANA_OUTBOX_PROD
    msgs = sqs_utils.get_messages(outbox, visibility_timeout=2)
    relevant = [m for m in msgs if m['Body'] == group_id]
    if relevant:
        for m in relevant:
            sqs_utils.delete_message(outbox, m['ReceiptHandle'])
    else:
        # Sleep for a random amount of time to allow other instances to
        # fetch the queue
        time.sleep(random.randint(5, 30))
        raise ValueError("Processed group not found")

detect_extraction_done = PythonOperator(
    task_id='detect_extraction_done',
    provide_context=True,
    python_callable=detect_extraction_done,
    retry_delay=timedelta(seconds=30),
    retries=240, #2 hours of retrying
    dag=mdag,
)

def get_tmp_dir(ds, kwargs):
    return TMP_PATH_TEMPLATE.format(
        kwargs['ti'].xcom_pull(dag_id=DAG_NAME, task_ids='get_group_id', key='group_id')
    )


def do_create_tmp_dir(ds, **kwargs):
    os.makedirs(TMP_PATH_TEMPLATE.format(
        kwargs['ti'].xcom_pull(dag_id=DAG_NAME, task_ids='get_group_id', key='group_id')
    ))

# Fetch, decrypt, push up transactions file
create_tmp_dir = PythonOperator(
    task_id='create_tmp_dir',
    provide_context=True,
    python_callable=do_create_tmp_dir,
    dag=mdag
)

def do_fetch_extracted_data(ds, **kwargs):
    gid = kwargs['ti'].xcom_pull(dag_id=DAG_NAME, task_ids='get_group_id', key='group_id')
    for t in [MEDICAL_CLAIMS_EXTRACT_TEMPLATE, PHARMACY_CLAIMS_EXTRACT_TEMPLATE,
        ENROLLMENT_EXTRACT_TEMPLATE]:

        s3_utils.fetch_file_from_s3(
            S3_NORMALIZED_FILE_URL_TEMPLATE.format(gid) + \
                t.format(gid),
            get_tmp_dir(ds, kwargs) + t.format(gid.replace('UAT-', ''))
        )

fetch_extracted_data = PythonOperator(
    task_id='fetch_extracted_data',
    provide_context=True,
    python_callable=do_fetch_extracted_data,
    dag=mdag
)

def do_create_return_file(ds, **kwargs):
    gid = kwargs['ti'].xcom_pull(dag_id=DAG_NAME, task_ids='get_group_id', key='group_id').replace('UAT-', '')
    tmp_dir = get_tmp_dir(ds, kwargs)
    extracted_files = [t.format(gid) for t in [
        MEDICAL_CLAIMS_EXTRACT_TEMPLATE,
        PHARMACY_CLAIMS_EXTRACT_TEMPLATE,
        ENROLLMENT_EXTRACT_TEMPLATE]
    ]
    subprocess.check_call(['tar', '-C', tmp_dir, '-zcf',
        tmp_dir + RETURN_FILE_TEMPLATE.format(gid)] + extracted_files)

create_return_file = PythonOperator(
    task_id='create_return_file',
    provide_context=True,
    python_callable=do_create_return_file,
    dag=mdag
)

def do_deliver_extracted_data(ds, **kwargs):
    gid = kwargs['ti'].xcom_pull(dag_id=DAG_NAME, task_ids='get_group_id', key='group_id')

    if 'UAT-' in gid:
        sftp_config = json.loads(Variable.get('humana_test_sftp_configuration'))
        gid = gid.replace('UAT-', '')
    else:
        sftp_config = json.loads(Variable.get('humana_prod_sftp_configuration'))
    path = sftp_config['path']
    del sftp_config['path']

    sftp_utils.upload_file(
        get_tmp_dir(ds, kwargs) + RETURN_FILE_TEMPLATE.format(gid),
        path,
        ignore_host_key=True,
         **sftp_config
    )

deliver_extracted_data = PythonOperator(
    task_id='deliver_extracted_data',
    provide_context=True,
    python_callable=do_deliver_extracted_data,
    dag=mdag
)

def do_clean_up_workspace(ds, **kwargs):
    gid = kwargs['ti'].xcom_pull(dag_id=DAG_NAME, task_ids='get_group_id', key='group_id')
    for t in [MEDICAL_CLAIMS_EXTRACT_TEMPLATE, PHARMACY_CLAIMS_EXTRACT_TEMPLATE,
        ENROLLMENT_EXTRACT_TEMPLATE, RETURN_FILE_TEMPLATE]:

        os.remove(TMP_PATH_TEMPLATE.format(gid) + t.format(gid.replace('UAT-', '')))

    os.rmdir(TMP_PATH_TEMPLATE.format(gid))

clean_up_workspace = PythonOperator(
    task_id='clean_up_workspace',
    provide_context=True,
    python_callable=do_clean_up_workspace,
    trigger_rule='all_done',
    dag=mdag
)

if HVDAG.HVDAG.airflow_env == 'test':
    for t in ['deliver_extracted_data']:
        del mdag.task_dict[t]
        globals()[t] = DummyOperator(
            task_id = t,
            dag = mdag
        )

detect_matching_done.set_upstream(get_group_id)
move_matching_payload.set_upstream(detect_matching_done)
queue_for_extraction.set_upstream(move_matching_payload)
detect_extraction_done.set_upstream(queue_for_extraction)
create_tmp_dir.set_upstream(detect_extraction_done)
fetch_extracted_data.set_upstream(create_tmp_dir)
create_return_file.set_upstream(fetch_extracted_data)
deliver_extracted_data.set_upstream(create_return_file)
clean_up_workspace.set_upstream(deliver_extracted_data)
