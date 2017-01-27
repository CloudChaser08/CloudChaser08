from airflow import DAG
from airflow.models import Variable
from airflow.operators import *
from datetime import datetime, timedelta
from subprocess import check_output, check_call, STDOUT
from json import loads as json_loads
import logging
import os
import pysftp
import re
import sys

subdags = [
    'subdags.s3_validate_file',
    'subdags.s3_fetch_file',
    'subdags.decrypt_file',
    'subdags.decompress_split_push_file',
    'subdags.queue_up_for_matching',
    'subdags.detect_move_normalize'
]

for subdag in subdags:
    if sys.modules.get(subdag):
        del sys.modules[subdag]
    
from subdags.s3_validate_file import s3_validate_file
from subdags.s3_fetch_file import s3_fetch_file
from subdags.decrypt_file import decrypt_file
from subdags.decompress_split_push_file import decompress_split_push_file
from subdags.queue_up_for_matching import queue_up_for_matching
from subdags.detect_move_normalize import detect_move_normalize

# Applies to all files
TMP_PATH_TEMPLATE='/tmp/express_scripts/pharmacyclaims/{}/'
DAG_NAME='express_scripts_pipeline'

# Transaction file
TRANSACTION_FILE_DESCRIPTION='Express Scripts transaction file'
S3_TRANSACTION_PREFIX='incoming/pharmacyclaims/esi/'
S3_TRANSACTION_SPLIT_PATH='s3://salusv/' + S3_TRANSACTION_PREFIX
S3_TRANSACTION_RAW_PATH='incoming/esi/'
TRANSACTION_FILE_NAME_TEMPLATE='10130X001_HV_RX_Claims_D{}.txt'
MINIMUM_TRANSACTION_FILE_SIZE=500

# Deid file
DEID_FILE_DESCRIPTION='Express Scripts deid file'
S3_DEID_RAW_PATH='incoming/esi/'
DEID_FILE_NAME_TEMPLATE='10130X001_HV_RX_Claims_D{}_key.txt'
MINIMUM_DEID_FILE_SIZE=500

S3_EXPRESS_SCRIPTS_PREFIX = 'warehouse/text/pharmacyclaims/express_scripts/'
S3_EXPRESS_SCRIPTS_WAREHOUSE = 's3://salusv/' + S3_EXPRESS_SCRIPTS_PREFIX

S3_PAYLOAD_LOC_PATH = 's3://salusv/matching/payload/pharmacyclaims/esi/'

def get_expected_transaction_file_name(kwargs):
    return TRANSACTION_FILE_NAME_TEMPLATE.format(kwargs['ds_nodash'])

def get_expected_transaction_file_name_gz(kwargs):
    return TRANSACTION_FILE_NAME_TEMPLATE.format(kwargs['ds_nodash']) + '.gz'

def get_expected_transaction_file_regex(kwargs):
    return TRANSACTION_FILE_NAME_TEMPLATE.format('\d{8}')

def get_expected_deid_file_name(kwargs):
    return DEID_FILE_NAME_TEMPLATE.format(kwargs['ds_nodash'])

def get_expected_deid_file_regex(kwargs):
    return DEID_FILE_NAME_TEMPLATE.format('\d{8}')

def get_file_date(kwargs):
    return kwargs['ds'].replace('-', '/')

def get_parquet_dates(kwargs):
    date_path = kwargs['ds'].replace('-', '/')

    warehouse_files = subprocess.check_output(['aws', 's3', 'ls', '--recursive', S3_EXPRESS_SCRIPTS_WAREHOUSE]).split("\n")
    file_dates = map(lambda f: '/'.join(f.split(' ')[-1].replace(S3_EXPRESS_SCRIPTS_PREFIX, '').split('/')[:-1]), warehouse_files)
    file_dates = filter(lambda d: len(d) == 10, file_dates)
    file_dates = sorted(list(set(file_dates)))
    return filter(lambda d: d < date_path, file_dates)[-2:]

def get_row_count(kwargs):
    return 3000000

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2016, 12, 1, 12),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'priority_weight': 5
}

mdag = DAG(
    dag_id=DAG_NAME,
    schedule_interval=None if Variable.get('AIRFLOW_ENV', default_var='').find('prod') != -1 else None,
    default_args=default_args
)

validate_transaction_file_dag = SubDagOperator(
    subdag=s3_validate_file(
        DAG_NAME,
        'validate_transaction_file',
        default_args['start_date'],
        mdag.schedule_interval,
        {
            'expected_file_name_func': get_expected_transaction_file_name,
            'file_name_pattern_func' : get_expected_transaction_file_regex,
            'minimum_file_size'      : MINIMUM_TRANSACTION_FILE_SIZE,
            's3_prefix'              : S3_TRANSACTION_RAW_PATH,
            'file_description'       : TRANSACTION_FILE_DESCRIPTION
        }
    ),
    task_id='validate_transaction_file',
    dag=mdag
)

validate_deid_file_dag = SubDagOperator(
    subdag=s3_validate_file(
        DAG_NAME,
        'validate_deid_file',
        default_args['start_date'],
        mdag.schedule_interval,
        {
            'expected_file_name_func': get_expected_deid_file_name,
            'file_name_pattern_func' : get_expected_deid_file_regex,
            'minimum_file_size'      : MINIMUM_DEID_FILE_SIZE,
            's3_prefix'              : S3_DEID_RAW_PATH,
            'file_description'       : DEID_FILE_DESCRIPTION
        }
    ),
    task_id='validate_deid_file',
    dag=mdag
)

fetch_transaction_file_dag = SubDagOperator(
    subdag=s3_fetch_file(
        DAG_NAME,
        'fetch_transaction_file',
        default_args['start_date'],
        mdag.schedule_interval,
        {
            'tmp_path_template'      : TMP_PATH_TEMPLATE,
            'expected_file_name_func': get_expected_transaction_file_name,
            's3_prefix'              : S3_DEID_RAW_PATH
        }
    ),
    task_id='fetch_transaction_file',
    dag=mdag
)

decrypt_transaction_file_dag = SubDagOperator(
    subdag=decrypt_file(
        DAG_NAME,
        'decrypt_transaction_file',
        default_args['start_date'],
        mdag.schedule_interval,
        {
            'tmp_path_template'       : TMP_PATH_TEMPLATE,
            'encrypted_file_name_func': get_expected_transaction_file_name,
            'decrypted_file_name_func': get_expected_transaction_file_name_gz
        }
    ),
    task_id='decrypt_transaction_file',
    dag=mdag
)

decompress_split_push_transaction_file_dag = SubDagOperator(
    subdag=decompress_split_push_file(
        DAG_NAME,
        'decompress_split_push_transaction_file',
        default_args['start_date'],
        mdag.schedule_interval,
        {
            'tmp_path_template'          : TMP_PATH_TEMPLATE,
            'decrypted_file_name_func'   : get_expected_transaction_file_name_gz,
            'decompressed_file_name_func': get_expected_transaction_file_name,
            'num_splits'                 : 100,
            's3_destination_prefix'      : S3_TRANSACTION_SPLIT_PATH
        
        }
    ),
    task_id='decompress_split_push_transaction_file',
    dag=mdag
)

queue_up_for_matching_dag = SubDagOperator(
    subdag=queue_up_for_matching(
        DAG_NAME,
        'queue_up_for_matching',
        default_args['start_date'],
        mdag.schedule_interval,
        {
            'expected_file_name_func': get_expected_deid_file_name,
            's3_prefix'              : S3_DEID_RAW_PATH
        }
    ),
    task_id='queue_up_for_matching',
    dag=mdag
)

detect_move_normalize_dag = SubDagOperator(
    subdag=detect_move_normalize(
        DAG_NAME,
        'detect_move_normalize',
        default_args['start_date'],
        mdag.schedule_interval,
        {
            'expected_deid_file_name_func': get_expected_deid_file_name,
            'file_date_func': get_file_date,
            'incoming_path': S3_TRANSACTION_PREFIX,
            'normalization_routine_directory': '/home/airflow/airflow/dags/providers/express_scripts/pharmacyclaims/',
            'normalization_routine_script': '/home/airflow/airflow/dags/providers/express_scripts/pharmacyclaims/rsNormalizeExpressScriptsRX.py',
            'parquet_dates_func': get_parquet_dates,
            'row_count_func': get_row_count,
            's3_path_prefix': S3_EXPRESS_SCRIPTS_PREFIX,
            's3_payload_loc': S3_PAYLOAD_LOC_PATH,
            'vendor_description': 'Express Scripts RX',
            'vendor_uuid': 'f726747e-9dc0-4023-9523-e077949ae865'
        }
    ),
    task_id='detect_move_normalize',
    dag=mdag
)

fetch_transaction_file_dag.set_upstream(validate_transaction_file_dag)
decrypt_transaction_file_dag.set_upstream(fetch_transaction_file_dag)
decompress_split_push_transaction_file_dag.set_upstream(decrypt_transaction_file_dag)
queue_up_for_matching_dag.set_upstream(validate_deid_file_dag)
detect_move_normalize_dag.set_upstream(queue_up_for_matching_dag)
