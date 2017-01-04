from airflow import DAG
from airflow.models import Variable
from airflow.operators import *
from datetime import datetime, timedelta
from subprocess import check_output, check_call, STDOUT
from json import loads as json_loads
import airflow.hooks.S3_hook
import logging
import os
import time

REDSHIFT_DB='dev'
REDSHIFT_HOST='emdeon-dx-norm'
S3_PATH_PREFIX='s3://salusv/matching/prod/payload/86396771-0345-4d67-83b3-7e22fded9e1d/'
S3_PREFIX='matching/prod/payload/86396771-0345-4d67-83b3-7e22fded9e1d/'
S3_PAYLOAD_LOC='s3://salusv/matching/payload/medicalclaims/emdeon/'

def do_move_matching_payload(ds, **kwargs):
    hook = airflow.hooks.S3_hook.S3Hook(s3_conn_id='my_conn_s3')
    deid_filename = kwargs['dag_run'].conf['deid_filename']
    s3_prefix = '{}{}'.format(S3_PREFIX, deid_filename)
    for payload_file in hook.list_keys('salusv', s3_prefix):
        date = '{}/{}/{}'.format(deid_filename[0:4], deid_filename[4:6], deid_filename[6:8])
        env = os.environ
        env["AWS_ACCESS_KEY_ID"] = Variable.get('AWS_ACCESS_KEY_ID')
        env["AWS_SECRET_ACCESS_KEY"] = Variable.get('AWS_SECRET_ACCESS_KEY')
        check_call(['aws', 's3', 'cp', 's3://salusv/' + payload_file, S3_PAYLOAD_LOC + date + '/' + payload_file.split('/')[-1]])

# This is essentially what S3KeySensorOperator does, but with the
# flexibility of determining the key programatically
def do_detect_matching_done(ds, **kwargs):
    hook = airflow.hooks.S3_hook.S3Hook(s3_conn_id='my_conn_s3')
    row_count = int(kwargs['dag_run'].conf['row_count'])
    chunk_start = row_count / 1000000 * 1000000
    deid_filename = kwargs['dag_run'].conf['deid_filename']
    template = '{}{}*'
    if row_count >= 1000000:
        template += '{}-{}'
    template += '*'
    s3_key = template.format(S3_PATH_PREFIX, deid_filename, chunk_start, row_count)
    logging.info('Poking for key : {}'.format(s3_key))
    while not hook.check_for_wildcard_key(s3_key, None):
        time.sleep(60)

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2017, 1, 1, 12),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(hours=1)
}

mdag = DAG(
    dag_id='emdeon_dx_post_matching_pipeline',
    schedule_interval="None",
    default_args=default_args
)

move_matching_payload = PythonOperator(
    task_id='move_matching_payload',
    provide_context=True,
    python_callable=do_move_matching_payload,
    dag=mdag
)

detect_matching_done = PythonOperator(
    task_id='detect_matching_done',
    provide_context=True,
    python_callable=do_detect_matching_done,
    execution_timeout=timedelta(hours=6),
    dag=mdag
)

move_matching_payload.set_upstream(detect_matching_done)
