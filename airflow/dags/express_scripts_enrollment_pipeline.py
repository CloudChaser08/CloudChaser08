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
import subdags.decrypt_files as decrypt_files
import subdags.split_push_files as split_push_files
import subdags.queue_up_for_matching as queue_up_for_matching
import subdags.detect_move_normalize as detect_move_normalize
import subdags.clean_up_tmp_dir as clean_up_tmp_dir

for m in [s3_validate_file, s3_fetch_file, decrypt_files, split_push_files,
        queue_up_for_matching, detect_move_normalize, clean_up_tmp_dir]:
    reload(m)

# Applies to all files
TMP_PATH_TEMPLATE='/tmp/express_scripts/enrollmentrecords/{}/'
DAG_NAME='express_scripts_enrollment_pipeline'

# Transaction file
TRANSACTION_FILE_DESCRIPTION='Express Scripts enrollment file'
S3_TRANSACTION_PREFIX='incoming/enrollmentrecords/express_scripts/'
S3_TRANSACTION_SPLIT_PATH='s3://salusv/' + S3_TRANSACTION_PREFIX
S3_TRANSACTION_RAW_PATH='incoming/esi/'
TRANSACTION_FILE_NAME_TEMPLATE='10130X001_HV_RX_ENROLLMENT_D{}.txt'
MINIMUM_TRANSACTION_FILE_SIZE=500

# Deid file
DEID_FILE_DESCRIPTION='Express Scripts enrollment deid file'
S3_DEID_RAW_PATH='incoming/esi/'
DEID_FILE_NAME_TEMPLATE='10130X001_HV_RX_ENROLLMENT_D{}_key.txt'
MINIMUM_DEID_FILE_SIZE=500

S3_PAYLOAD_LOC_URL = 's3://salusv/matching/payload/enrollmentrecords/express_scripts/'

S3_ORIGIN_BUCKET = 'healthverity'

def get_tmp_dir(ds, kwargs):
    return TMP_PATH_TEMPLATE.format(kwargs['ds_nodash'])

def get_expected_transaction_file_name(ds, kwargs):
    file_date = get_file_date(ds, kwargs).replace('-', '')
    return TRANSACTION_FILE_NAME_TEMPLATE.format(file_date)

def get_expected_transaction_file_name_decrypted(ds, kwargs):
    file_date = get_file_date(ds, kwargs).replace('-', '')
    return TRANSACTION_FILE_NAME_TEMPLATE.format(file_date) + '.decrypted'

def get_encrypted_decrypted_file_paths(ds, kwargs):
    tmp_dir = get_tmp_dir(ds, kwargs)
    expected_input = get_expected_transaction_file_name(ds, kwargs)
    expected_output = get_expected_transaction_file_name_gz(ds, kwargs)
    return [
        [tmp_dir + expected_input, tmp_dir + expected_output]
    ]

def get_expected_transaction_file_regex(ds, kwargs):
    return TRANSACTION_FILE_NAME_TEMPLATE.format('\d{8}')

def get_decrypted_transaction_files_paths(ds, kwargs):
    return [get_tmp_dir(ds, kwargs) + get_expected_transaction_file_name(ds, kwargs) + '.decrypted']

def get_s3_transaction_prefix(ds, kwargs):
    return S3_TRANSACTION_SPLIT_PATH + get_file_date(ds, kwargs).replace('-', '/') + '/'

def get_expected_deid_file_name(ds, kwargs):
    file_date = get_file_date(ds, kwargs).replace('-', '')
    return DEID_FILE_NAME_TEMPLATE.format(file_date)

def get_expected_deid_file_regex(ds, kwargs):
    return DEID_FILE_NAME_TEMPLATE.format('\d{8}')

def get_file_date(ds, kwargs):
    return (kwargs['execution_date'] + timedelta(days=6)).strftime('%Y-%m-%d')

def get_deid_file_urls(ds, kwargs):
    return ['s3://healthverity/' + S3_DEID_RAW_PATH + get_expected_deid_file_name(ds, kwargs)]

def get_expected_matching_files(ds, kwargs):
    return [get_expected_deid_file_name(ds, kwargs)]

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2017, 3, 26),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'priority_weight': 5
}

mdag = DAG(
    dag_id=DAG_NAME,
    schedule_interval='0 0 * * 0' if Variable.get('AIRFLOW_ENV', default_var='').find('prod') != -1 else None,
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
            's3_prefix'              : S3_DEID_RAW_PATH,
            's3_bucket'              : S3_ORIGIN_BUCKET
        }
    ),
    task_id='fetch_transaction_file',
    dag=mdag
)

decrypt_transaction_file_dag = SubDagOperator(
    subdag=decrypt_files.decrypt_files(
        DAG_NAME,
        'decrypt_transaction_file',
        default_args['start_date'],
        mdag.schedule_interval,
        {
            'tmp_dir_func'                        : get_tmp_dir,
            'encrypted_decrypted_file_paths_func' : get_encrypted_decrypted_file_paths
        }
    ),
    task_id='decrypt_transaction_file',
    dag=mdag
)

split_push_transaction_files_dag = SubDagOperator(
    subdag=split_push_files.split_push_files(
        DAG_NAME,
        'split_push_transaction_files',
        default_args['start_date'],
        mdag.schedule_interval,
        {
            'tmp_dir_func'             : get_tmp_dir,
            'file_paths_to_split_func' : get_decrypted_transaction_files_paths,
            's3_prefix_func'           : get_s3_transaction_prefix,
            'num_splits'               : 100
        }
    ),
    task_id='split_push_transaction_files',
    dag=mdag
)

queue_up_for_matching_dag = SubDagOperator(
    subdag=queue_up_for_matching.queue_up_for_matching(
        DAG_NAME,
        'queue_up_for_matching',
        default_args['start_date'],
        mdag.schedule_interval,
        {
            'source_files_func' : get_deid_file_urls,
            'passthrough_only'  : 'true'
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
            'expected_matching_files_func'   : get_expected_matching_files,
            'file_date_func'                 : get_file_date,
            'incoming_path'                  : S3_TRANSACTION_PREFIX,
            'normalization_routine_directory': '/home/airflow/airflow/dags/spark/providers/express_scripts/enrollmentrecords/',
            'normalization_routine_script'   : '/home/airflow/airflow/dags/spark/providers/express_scripts/enrollmentrecords/sparkNormalizeExpressScriptsEnrollment.py',
            's3_payload_loc_url'             : S3_PAYLOAD_LOC_URL,
            'vendor_description'             : 'Express Scripts Enrollment',
            'vendor_uuid'                    : 'f726747e-9dc0-4023-9523-e077949ae865'
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
split_push_transaction_files_dag.set_upstream(decrypt_transaction_file_dag)
queue_up_for_matching_dag.set_upstream(validate_deid_file_dag)
detect_move_normalize_dag.set_upstream([queue_up_for_matching_dag, split_push_transaction_files_dag])
clean_up_tmp_dir_dag.set_upstream(split_push_transaction_files_dag)
