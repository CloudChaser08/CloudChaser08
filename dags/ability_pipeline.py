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
    'subdags.s3_push_file',
    'subdags.clean_up_tmp_dir'
]

for subdag in subdags:
    if sys.modules.get(subdag):
        del sys.modules[subdag]
    
from subdags.s3_validate_file import s3_validate_file
from subdags.s3_fetch_file import s3_fetch_file
from subdags.s3_push_file import s3_push_file
from subdags.clean_up_tmp_dir import clean_up_tmp_dir

# Applies to all files
TMP_PATH_TEMPLATE='/tmp/ability/medicalclaims/{}/'
DAG_NAME='ability_pipeline'

# Ability S3 bucket access
ABILITY_S3_BUCKET=Variable.get('Ability_S3_Bucket')
ABILITY_S3_CONNECTION='ability_s3_conn'

HV_S3_RAW_PREFIX='incoming/ability/'
HV_S3_RAW_BUCKET='healthverity'

# Ability AP file
AP_FILE_DESCRIPTION='Ability AP file'
ABILITY_S3_AP_PREFIX='ap-daily/'
AP_FILE_NAME_TEMPLATE='ap.from_{0}.to_{0}.zip'
MINIMUM_AP_FILE_SIZE=15000

# Ability SES file
SES_FILE_DESCRIPTION='Ability SES file'
ABILITY_S3_SES_PREFIX='ses-daily/'
SES_FILE_NAME_TEMPLATE='ses.from_{0}.to_{1}.zip'
MINIMUM_SES_FILE_SIZE=15000

# Ability EASE file
EASE_FILE_DESCRIPTION='Ability EASE file'
ABILITY_S3_EASE_PREFIX='ease-daily/'
EASE_FILE_NAME_TEMPLATE='ease.from_{0}.to_{0}.zip'
MINIMUM_EASE_FILE_SIZE=15000

def get_expected_ap_file_name(ds, kwargs):
    return AP_FILE_NAME_TEMPLATE.format(ds)

def get_expected_ap_file_regex(ds, kwargs):
    return AP_FILE_NAME_TEMPLATE.format('\d{4}-\d{2}-\d{2}')

def get_expected_ses_file_name(ds, kwargs):
    return SES_FILE_NAME_TEMPLATE.format(ds, kwargs['tomorrow_ds'])

def get_expected_ses_file_regex(ds, kwargs):
    return SES_FILE_NAME_TEMPLATE.format('\d{4}-\d{2}-\d{2}', '\d{4}-\d{2}-\d{2}')

def get_expected_ease_file_name(ds, kwargs):
    return EASE_FILE_NAME_TEMPLATE.format(ds)

def get_expected_ease_file_regex(ds, kwargs):
    return EASE_FILE_NAME_TEMPLATE.format('\d{4}-\d{2}-\d{2}')

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2016, 12, 24),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'priority_weight': 5
}

mdag = DAG(
    dag_id=DAG_NAME,
    schedule_interval='@daily' if Variable.get('AIRFLOW_ENV', default_var='').find('prod') != -1 else None,
    default_args=default_args
)

validate_ap_file_dag = SubDagOperator(
    subdag=s3_validate_file(
        DAG_NAME,
        'validate_ap_file',
        default_args['start_date'],
        mdag.schedule_interval,
        {
            'expected_file_name_func': get_expected_ap_file_name,
            'file_name_pattern_func' : get_expected_ap_file_regex,
            'minimum_file_size'      : MINIMUM_AP_FILE_SIZE,
            's3_prefix'              : ABILITY_S3_AP_PREFIX,
            's3_bucket'              : ABILITY_S3_BUCKET,
            's3_connection'          : ABILITY_S3_CONNECTION,
            'file_description'       : AP_FILE_DESCRIPTION
        }
    ),
    task_id='validate_ap_file',
    dag=mdag
)

fetch_ap_file_dag = SubDagOperator(
    subdag=s3_fetch_file(
        DAG_NAME,
        'fetch_ap_file',
        default_args['start_date'],
        mdag.schedule_interval,
        {
            'tmp_path_template'      : TMP_PATH_TEMPLATE,
            'expected_file_name_func': get_expected_ap_file_name,
            's3_prefix'              : ABILITY_S3_AP_PREFIX,
            's3_bucket'              : ABILITY_S3_BUCKET,
            's3_connection'          : ABILITY_S3_CONNECTION
        }
    ),
    task_id='fetch_ap_file',
    dag=mdag
)

push_ap_file_dag = SubDagOperator(
    subdag=s3_push_file(
        DAG_NAME,
        'push_ap_file',
        default_args['start_date'],
        mdag.schedule_interval,
        {
            'tmp_path_template'      : TMP_PATH_TEMPLATE,
            'file_name_func'         : get_expected_ap_file_name,
            's3_prefix'              : HV_S3_RAW_PREFIX,
            's3_bucket'              : HV_S3_RAW_BUCKET
        }
    ),
    task_id='push_ap_file',
    dag=mdag
)

validate_ses_file_dag = SubDagOperator(
    subdag=s3_validate_file(
        DAG_NAME,
        'validate_ses_file',
        default_args['start_date'],
        mdag.schedule_interval,
        {
            'expected_file_name_func': get_expected_ses_file_name,
            'file_name_pattern_func' : get_expected_ses_file_regex,
            'minimum_file_size'      : MINIMUM_SES_FILE_SIZE,
            's3_prefix'              : ABILITY_S3_SES_PREFIX,
            's3_bucket'              : ABILITY_S3_BUCKET,
            's3_connection'          : ABILITY_S3_CONNECTION,
            'file_description'       : SES_FILE_DESCRIPTION
        }
    ),
    task_id='validate_ses_file',
    dag=mdag
)

fetch_ses_file_dag = SubDagOperator(
    subdag=s3_fetch_file(
        DAG_NAME,
        'fetch_ses_file',
        default_args['start_date'],
        mdag.schedule_interval,
        {
            'tmp_path_template'      : TMP_PATH_TEMPLATE,
            'expected_file_name_func': get_expected_ses_file_name,
            's3_prefix'              : ABILITY_S3_SES_PREFIX,
            's3_bucket'              : ABILITY_S3_BUCKET,
            's3_connection'          : ABILITY_S3_CONNECTION
        }
    ),
    task_id='fetch_ses_file',
    dag=mdag
)

push_ses_file_dag = SubDagOperator(
    subdag=s3_push_file(
        DAG_NAME,
        'push_ses_file',
        default_args['start_date'],
        mdag.schedule_interval,
        {
            'tmp_path_template'      : TMP_PATH_TEMPLATE,
            'file_name_func'         : get_expected_ses_file_name,
            's3_prefix'              : HV_S3_RAW_PREFIX,
            's3_bucket'              : HV_S3_RAW_BUCKET
        }
    ),
    task_id='push_ses_file',
    dag=mdag
)

validate_ease_file_dag = SubDagOperator(
    subdag=s3_validate_file(
        DAG_NAME,
        'validate_ease_file',
        default_args['start_date'],
        mdag.schedule_interval,
        {
            'expected_file_name_func': get_expected_ease_file_name,
            'file_name_pattern_func' : get_expected_ease_file_regex,
            'minimum_file_size'      : MINIMUM_EASE_FILE_SIZE,
            's3_prefix'              : ABILITY_S3_EASE_PREFIX,
            's3_bucket'              : ABILITY_S3_BUCKET,
            's3_connection'          : ABILITY_S3_CONNECTION,
            'file_description'       : EASE_FILE_DESCRIPTION
        }
    ),
    task_id='validate_ease_file',
    dag=mdag
)

fetch_ease_file_dag = SubDagOperator(
    subdag=s3_fetch_file(
        DAG_NAME,
        'fetch_ease_file',
        default_args['start_date'],
        mdag.schedule_interval,
        {
            'tmp_path_template'      : TMP_PATH_TEMPLATE,
            'expected_file_name_func': get_expected_ease_file_name,
            's3_prefix'              : ABILITY_S3_EASE_PREFIX,
            's3_bucket'              : ABILITY_S3_BUCKET,
            's3_connection'          : ABILITY_S3_CONNECTION
        }
    ),
    task_id='fetch_ease_file',
    dag=mdag
)

push_ease_file_dag = SubDagOperator(
    subdag=s3_push_file(
        DAG_NAME,
        'push_ease_file',
        default_args['start_date'],
        mdag.schedule_interval,
        {
            'tmp_path_template'      : TMP_PATH_TEMPLATE,
            'file_name_func'         : get_expected_ease_file_name,
            's3_prefix'              : HV_S3_RAW_PREFIX,
            's3_bucket'              : HV_S3_RAW_BUCKET
        }
    ),
    task_id='push_ease_file',
    dag=mdag
)

clean_up_tmp_dir_dag = SubDagOperator(
    subdag=clean_up_tmp_dir(
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

fetch_ap_file_dag.set_upstream(validate_ap_file_dag)
push_ap_file_dag.set_upstream(fetch_ap_file_dag)
fetch_ses_file_dag.set_upstream(validate_ses_file_dag)
push_ses_file_dag.set_upstream(fetch_ses_file_dag)
fetch_ease_file_dag.set_upstream(validate_ease_file_dag)
push_ease_file_dag.set_upstream(fetch_ease_file_dag)
clean_up_tmp_dir_dag.set_upstream([push_ap_file_dag, push_ses_file_dag, push_ease_file_dag])
