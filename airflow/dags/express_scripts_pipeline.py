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

import subdags.s3_validate_file as s3_validate_file
import subdags.s3_fetch_file as s3_fetch_file
import subdags.decrypt_file as decrypt_file
import subdags.split_push_file as split_push_file
import subdags.queue_up_for_matching as queue_up_for_matching
import subdags.detect_move_normalize as detect_move_normalize
import subdags.clean_up_tmp_dir as clean_up_tmp_dir

reload(s3_validate_file)
reload(s3_fetch_file)
reload(decrypt_file)
reload(split_push_file)
reload(queue_up_for_matching)
reload(detect_move_normalize)
reload(clean_up_tmp_dir)

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

S3_TEXT_EXPRESS_SCRIPTS_PREFIX = 'warehouse/text/pharmacyclaims/express_scripts/'
S3_PARQUET_EXPRESS_SCRIPTS_PREFIX = 'warehouse/parquet/pharmacyclaims/express_scripts/'
S3_TEXT_EXPRESS_SCRIPTS_WAREHOUSE = 's3://salusv/' + S3_TEXT_EXPRESS_SCRIPTS_PREFIX

S3_PAYLOAD_LOC_PATH = 's3://salusv/matching/payload/pharmacyclaims/esi/'

S3_ORIGIN_BUCKET = 'healthverity'

def get_expected_transaction_file_name(ds, kwargs):
    return TRANSACTION_FILE_NAME_TEMPLATE.format(kwargs['ds_nodash'])

def get_expected_transaction_file_name_gz(ds, kwargs):
    return TRANSACTION_FILE_NAME_TEMPLATE.format(kwargs['ds_nodash']) + '.gz'

def get_expected_transaction_file_regex(ds, kwargs):
    return TRANSACTION_FILE_NAME_TEMPLATE.format('\d{8}')

def get_expected_deid_file_name(ds, kwargs):
    return DEID_FILE_NAME_TEMPLATE.format(kwargs['ds_nodash'])

def get_expected_deid_file_regex(ds, kwargs):
    return DEID_FILE_NAME_TEMPLATE.format('\d{8}')

def get_file_date(ds, kwargs):
    return ds.replace('-', '/')

def get_parquet_dates(ds, kwargs):
    date_path = ds.replace('-', '/')

    warehouse_files = check_output(['aws', 's3', 'ls', '--recursive', S3_TEXT_EXPRESS_SCRIPTS_WAREHOUSE]).split("\n")
    file_dates = map(lambda f: '/'.join(f.split(' ')[-1].replace(S3_TEXT_EXPRESS_SCRIPTS_PREFIX, '').split('/')[:-1]), warehouse_files)
    file_dates = filter(lambda d: len(d) == 10, file_dates)
    file_dates = sorted(list(set(file_dates)))
    return filter(lambda d: d < date_path, file_dates)[-2:] + [date_path]

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
    subdag=s3_validate_file.s3_validate_file(
        DAG_NAME,
        'validate_transaction_file',
        default_args['start_date'],
        mdag.schedule_interval,
        {
            'expected_file_name_func': get_expected_transaction_file_name,
            'file_name_pattern_func' : get_expected_transaction_file_regex,
            'minimum_file_size'      : MINIMUM_TRANSACTION_FILE_SIZE,
            's3_prefix'              : S3_TRANSACTION_RAW_PATH,
            's3_bucket'              : S3_ORIGIN_BUCKET,
            'file_description'       : TRANSACTION_FILE_DESCRIPTION
        }
    ),
    task_id='validate_transaction_file',
    dag=mdag
)

validate_deid_file_dag = SubDagOperator(
    subdag=s3_validate_file.s3_validate_file(
        DAG_NAME,
        'validate_deid_file',
        default_args['start_date'],
        mdag.schedule_interval,
        {
            'expected_file_name_func': get_expected_deid_file_name,
            'file_name_pattern_func' : get_expected_deid_file_regex,
            'minimum_file_size'      : MINIMUM_DEID_FILE_SIZE,
            's3_prefix'              : S3_DEID_RAW_PATH,
            's3_bucket'              : S3_ORIGIN_BUCKET,
            'file_description'       : DEID_FILE_DESCRIPTION
        }
    ),
    task_id='validate_deid_file',
    dag=mdag
)

fetch_transaction_file_dag = SubDagOperator(
    subdag=s3_fetch_file.s3_fetch_file(
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
    subdag=decrypt_file.decrypt_file(
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

split_push_transaction_file_dag = SubDagOperator(
    subdag=split_push_file.split_push_file(
        DAG_NAME,
        'decompress_split_push_transaction_file',
        default_args['start_date'],
        mdag.schedule_interval,
        {
            'tmp_path_template'          : TMP_PATH_TEMPLATE,
            'source_file_name_func'      : get_expected_transaction_file_name,
            'num_splits'                 : 100,
            's3_dest_path_func': lambda ds, k:
            S3_TRANSACTION_SPLIT_PATH + '{}/{}/{}/'.format(
                k['ds_nodash'][0:4],
                k['ds_nodash'][4:6],
                k['ds_nodash'][6:8]
            )
        }
    ),
    task_id='decompress_split_push_transaction_file',
    dag=mdag
)

queue_up_for_matching_dag = SubDagOperator(
    subdag=queue_up_for_matching.queue_up_for_matching(
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
    subdag=detect_move_normalize.detect_move_normalize(
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
            's3_text_path_prefix': S3_TEXT_EXPRESS_SCRIPTS_PREFIX,
            's3_parquet_path_prefix': S3_PARQUET_EXPRESS_SCRIPTS_PREFIX,
            's3_payload_loc': S3_PAYLOAD_LOC_PATH,
            'vendor_description': 'Express Scripts RX',
            'vendor_uuid': 'f726747e-9dc0-4023-9523-e077949ae865'
        }
    ),
    task_id='detect_move_normalize',
    dag=mdag
)

clean_up_tmp_dir_dag = SubDagOperator(
    subdag=clean_up_tmp_dir.clean_up_tmp_dir(
        DAG_NAME,
        'clean_up_tmp_dir',
        default_args['start_date'],
        mdag.schedule_interval,
        {
            'tmp_path_template'      : TMP_PATH_TEMPLATE,
        }
    ),
    task_id='clean_up_tmp_dir',
    dag=mdag
)

fetch_transaction_file_dag.set_upstream(validate_transaction_file_dag)
decrypt_transaction_file_dag.set_upstream(fetch_transaction_file_dag)
split_push_transaction_file_dag.set_upstream(decrypt_transaction_file_dag)
queue_up_for_matching_dag.set_upstream(validate_deid_file_dag)
detect_move_normalize_dag.set_upstream([queue_up_for_matching_dag, split_push_transaction_file_dag])
clean_up_tmp_dir_dag.set_upstream(split_push_transaction_file_dag)
