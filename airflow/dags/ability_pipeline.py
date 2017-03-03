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
import subdags.s3_push_files as s3_push_files
import subdags.queue_up_for_matching as queue_up_for_matching
import subdags.clean_up_tmp_dir as clean_up_tmp_dir

for m in [s3_validate_file, s3_fetch_file, s3_push_files, queue_up_for_matching, clean_up_tmp_dir]:
    reload(m)

# Applies to all files
TMP_PATH_TEMPLATE='/tmp/ability/medicalclaims/{}/'
DAG_NAME='ability_pipeline'

# Ability S3 bucket access
ABILITY_S3_BUCKET=Variable.get('Ability_S3_Bucket')
ABILITY_S3_CONNECTION='ability_s3_conn'

HV_S3_RAW_PREFIX='incoming/ability/'
HV_S3_RAW_BUCKET='healthverity'

HV_S3_TRANSACTION_PREFIX_TEMPLATE='incoming/medicalclaims/ability/{}/'
HV_S3_TRANSACTION_BUCKET='salusv'

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

def get_s3_transaction_prefix(ds, kwargs):
    return HV_S3_TRANSACTION_PREFIX_TEMPLATE.format(ds.replace('-', '/'))

def get_s3_raw_prefix(ds, kwargs):
    return HV_S3_RAW_PREFIX

def get_ap_deid_file_paths(ds, kwargs):
    file_dir = TMP_PATH_TEMPLATE.format(kwargs['ds_nodash']) + 'ap/transaction/';
    files = os.listdir(file_dir)
    return map(lambda f: file_dir + f, files)

def get_ses_deid_file_paths(ds, kwargs):
    file_dir = TMP_PATH_TEMPLATE.format(kwargs['ds_nodash']) + 'ses/transaction/';
    files = os.listdir(file_dir)
    return map(lambda f: file_dir + f, files)

def get_ease_deid_file_paths(ds, kwargs):
    file_dir = TMP_PATH_TEMPLATE.format(kwargs['ds_nodash']) + 'ease/transaction/';
    files = os.listdir(file_dir)
    return map(lambda f: file_dir + f, files)

def do_unzip_files(ds, **kwargs):
    check_call([
        'unzip', kwargs['tmp_path_template'].format(kwargs['ds_nodash']) + kwargs['expected_file_name_func'](ds, kwargs),
        '-d', kwargs['tmp_path_template'].format(kwargs['ds_nodash']) + kwargs['dest_dir']
    ])

def do_move_files(ds, **kwargs):
    old_file_dir = kwargs['tmp_path_template'].format(kwargs['ds_nodash']) + kwargs['origin_dir']
    new_file_dir = kwargs['tmp_path_template'].format(kwargs['ds_nodash']) + kwargs['dest_dir']
    check_call(['mkdir', '-p', new_file_dir])
    for f in os.listdir(old_file_dir):
        if os.path.isfile(old_file_dir + f) and re.search(kwargs['filename_pattern'], f):
            check_call(['mv', old_file_dir + f, new_file_dir + f])

def do_rename_files(ds, **kwargs):
    file_dir = kwargs['tmp_path_template'].format(kwargs['ds_nodash']) + kwargs['file_dir']
    files = os.listdir(file_dir)
    for f in files:
        if os.path.isfile(file_dir + f):
            check_call(['mv', file_dir + f, file_dir + ds.replace("-","_") + "_" + kwargs['prefix'] + "_" + f])

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2016, 12, 24, 15),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'priority_weight': 5
}

mdag = DAG(
    dag_id=DAG_NAME,
    schedule_interval='0 15 * * *' if Variable.get('AIRFLOW_ENV', default_var='').find('prod') != -1 else None,
    default_args=default_args
)

validate_ap_file_dag = SubDagOperator(
    subdag=s3_validate_file.s3_validate_file(
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
            'aws_access_key_id'      : Variable.get('Ability_AWS_ACCESS_KEY_ID'),
            'aws_secret_access_key'  : Variable.get('Ability_AWS_SECRET_ACCESS_KEY'),
            's3_connection'          : ABILITY_S3_CONNECTION,
            'file_description'       : AP_FILE_DESCRIPTION
        }
    ),
    task_id='validate_ap_file',
    dag=mdag
)

fetch_ap_file_dag = SubDagOperator(
    subdag=s3_fetch_file.s3_fetch_file(
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
    subdag=s3_push_files.s3_push_files(
        DAG_NAME,
        'push_ap_file',
        default_args['start_date'],
        mdag.schedule_interval,
        {
            'tmp_path_template'      : TMP_PATH_TEMPLATE,
            'file_name_func'         : get_expected_ap_file_name,
            's3_prefix_func'         : get_s3_raw_prefix,
            's3_bucket'              : HV_S3_RAW_BUCKET
        }
    ),
    task_id='push_ap_file',
    dag=mdag
)

unzip_ap_files = PythonOperator(
    task_id='unzip_ap_files',
    provide_context=True,
    python_callable=do_unzip_files,
    op_kwargs={
        'expected_file_name_func': get_expected_ap_file_name,
        'tmp_path_template'      : TMP_PATH_TEMPLATE,
        'dest_dir'               : 'ap/'
        },
    dag=mdag
)

move_ap_deid_files = PythonOperator(
    task_id='move_ap_deid_files',
    provide_context=True,
    python_callable=do_move_files,
    op_kwargs={
        'tmp_path_template'      : TMP_PATH_TEMPLATE,
        'origin_dir'             : 'ap/',
        'filename_pattern'       : '^deid',
        'dest_dir'               : 'ap/deid/'
        },
    dag=mdag
)

move_ap_transaction_files = PythonOperator(
    task_id='move_ap_transaction_files',
    provide_context=True,
    python_callable=do_move_files,
    op_kwargs={
        'tmp_path_template'      : TMP_PATH_TEMPLATE,
        'origin_dir'             : 'ap/',
        'filename_pattern'       : '^(?!deid)',
        'dest_dir'               : 'ap/transaction/'
        },
    dag=mdag
)

rename_ap_deid_files = PythonOperator(
    task_id='rename_ap_deid_files',
    provide_context=True,
    python_callable=do_rename_files,
    op_kwargs={
        'tmp_path_template'      : TMP_PATH_TEMPLATE,
        'file_dir'               : 'ap/deid/',
        'prefix'                 : 'ap'
        },
    dag=mdag
)

rename_ap_transaction_files = PythonOperator(
    task_id='rename_ap_transaction_files',
    provide_context=True,
    python_callable=do_rename_files,
    op_kwargs={
        'tmp_path_template'      : TMP_PATH_TEMPLATE,
        'file_dir'               : 'ap/transaction/',
        'prefix'                 : 'ap'
        },
    dag=mdag
)

push_ap_transaction_files_dag = SubDagOperator(
    subdag=s3_push_files.s3_push_files(
        DAG_NAME,
        'push_ap_transaction_files',
        default_args['start_date'],
        mdag.schedule_interval,
        {
            'tmp_path_template'      : TMP_PATH_TEMPLATE + 'ap/transaction/',
            's3_prefix_func'         : get_s3_transaction_prefix,
            's3_bucket'              : HV_S3_TRANSACTION_BUCKET,
            'all_files_in_dir'       : True
        }
    ),
    task_id='push_ap_transaction_files',
    dag=mdag
)

ap_queue_up_for_matching_dag =  SubDagOperator(
    subdag=queue_up_for_matching.queue_up_for_matching(
        DAG_NAME,
        'ap_queue_up_for_matching',
        default_args['start_date'],
        mdag.schedule_interval,
        {
            'source_files_func'      : get_ap_deid_file_paths
        }
    ),
    task_id='ap_queue_up_for_matching',
    dag=mdag
)


validate_ses_file_dag = SubDagOperator(
    subdag=s3_validate_file.s3_validate_file(
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
    subdag=s3_fetch_file.s3_fetch_file(
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
    subdag=s3_push_files.s3_push_files(
        DAG_NAME,
        'push_ses_file',
        default_args['start_date'],
        mdag.schedule_interval,
        {
            'tmp_path_template'      : TMP_PATH_TEMPLATE,
            'file_name_func'         : get_expected_ses_file_name,
            's3_prefix_func'         : get_s3_raw_prefix,
            's3_bucket'              : HV_S3_RAW_BUCKET
        }
    ),
    task_id='push_ses_file',
    dag=mdag
)

unzip_ses_files = PythonOperator(
    task_id='unzip_ses_files',
    provide_context=True,
    python_callable=do_unzip_files,
    op_kwargs={
        'expected_file_name_func': get_expected_ses_file_name,
        'tmp_path_template'      : TMP_PATH_TEMPLATE,
        'dest_dir'               : 'ses/'
        },
    dag=mdag
)

move_ses_deid_files = PythonOperator(
    task_id='move_ses_deid_files',
    provide_context=True,
    python_callable=do_move_files,
    op_kwargs={
        'tmp_path_template'      : TMP_PATH_TEMPLATE,
        'origin_dir'             : 'ses/',
        'filename_pattern'       : '^deid',
        'dest_dir'               : 'ses/deid/'
        },
    dag=mdag
)

move_ses_transaction_files = PythonOperator(
    task_id='move_ses_transaction_files',
    provide_context=True,
    python_callable=do_move_files,
    op_kwargs={
        'tmp_path_template'      : TMP_PATH_TEMPLATE,
        'origin_dir'             : 'ses/',
        'filename_pattern'       : '^(?!deid)',
        'dest_dir'               : 'ses/transaction/'
        },
    dag=mdag
)

rename_ses_deid_files = PythonOperator(
    task_id='rename_ses_deid_files',
    provide_context=True,
    python_callable=do_rename_files,
    op_kwargs={
        'tmp_path_template'      : TMP_PATH_TEMPLATE,
        'file_dir'               : 'ses/deid/',
        'prefix'                 : 'ses'
        },
    dag=mdag
)

rename_ses_transaction_files = PythonOperator(
    task_id='rename_ses_transaction_files',
    provide_context=True,
    python_callable=do_rename_files,
    op_kwargs={
        'tmp_path_template'      : TMP_PATH_TEMPLATE,
        'file_dir'               : 'ses/transaction/',
        'prefix'                 : 'ses'
        },
    dag=mdag
)

push_ses_transaction_files_dag = SubDagOperator(
    subdag=s3_push_files.s3_push_files(
        DAG_NAME,
        'push_ses_transaction_files',
        default_args['start_date'],
        mdag.schedule_interval,
        {
            'tmp_path_template'      : TMP_PATH_TEMPLATE + 'ses/transaction/',
            's3_prefix_func'         : get_s3_transaction_prefix,
            's3_bucket'              : HV_S3_TRANSACTION_BUCKET,
            'all_files_in_dir'       : True
        }
    ),
    task_id='push_ses_transaction_files',
    dag=mdag
)

ses_queue_up_for_matching_dag =  SubDagOperator(
    subdag=queue_up_for_matching.queue_up_for_matching(
        DAG_NAME,
        'ses_queue_up_for_matching',
        default_args['start_date'],
        mdag.schedule_interval,
        {
            'source_files_func'      : get_ses_deid_file_paths
        }
    ),
    task_id='ses_queue_up_for_matching',
    dag=mdag
)

validate_ease_file_dag = SubDagOperator(
    subdag=s3_validate_file.s3_validate_file(
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
    subdag=s3_fetch_file.s3_fetch_file(
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
    subdag=s3_push_files.s3_push_files(
        DAG_NAME,
        'push_ease_file',
        default_args['start_date'],
        mdag.schedule_interval,
        {
            'tmp_path_template'      : TMP_PATH_TEMPLATE,
            'file_name_func'         : get_expected_ease_file_name,
            's3_prefix_func'         : get_s3_raw_prefix,
            's3_bucket'              : HV_S3_RAW_BUCKET
        }
    ),
    task_id='push_ease_file',
    dag=mdag
)

unzip_ease_files = PythonOperator(
    task_id='unzip_ease_files',
    provide_context=True,
    python_callable=do_unzip_files,
    op_kwargs={
        'expected_file_name_func': get_expected_ease_file_name,
        'tmp_path_template'      : TMP_PATH_TEMPLATE,
        'dest_dir'               : 'ease/'
        },
    dag=mdag
)

move_ease_deid_files = PythonOperator(
    task_id='move_ease_deid_files',
    provide_context=True,
    python_callable=do_move_files,
    op_kwargs={
        'tmp_path_template'      : TMP_PATH_TEMPLATE,
        'origin_dir'             : 'ease/',
        'filename_pattern'       : '^deid',
        'dest_dir'               : 'ease/deid/'
        },
    dag=mdag
)

move_ease_transaction_files = PythonOperator(
    task_id='move_ease_transaction_files',
    provide_context=True,
    python_callable=do_move_files,
    op_kwargs={
        'tmp_path_template'      : TMP_PATH_TEMPLATE,
        'origin_dir'             : 'ease/',
        'filename_pattern'       : '^(?!deid)',
        'dest_dir'               : 'ease/transaction/'
        },
    dag=mdag
)

rename_ease_deid_files = PythonOperator(
    task_id='rename_ease_deid_files',
    provide_context=True,
    python_callable=do_rename_files,
    op_kwargs={
        'tmp_path_template'      : TMP_PATH_TEMPLATE,
        'file_dir'               : 'ease/deid/',
        'prefix'                 : 'ease'
        },
    dag=mdag
)

rename_ease_transaction_files = PythonOperator(
    task_id='rename_ease_transaction_files',
    provide_context=True,
    python_callable=do_rename_files,
    op_kwargs={
        'tmp_path_template'      : TMP_PATH_TEMPLATE,
        'file_dir'               : 'ease/transaction/',
        'prefix'                 : 'ease'
        },
    dag=mdag
)

push_ease_transaction_files_dag = SubDagOperator(
    subdag=s3_push_files.s3_push_files(
        DAG_NAME,
        'push_ease_transaction_files',
        default_args['start_date'],
        mdag.schedule_interval,
        {
            'tmp_path_template'      : TMP_PATH_TEMPLATE + 'ease/transaction/',
            's3_prefix_func'         : get_s3_transaction_prefix,
            's3_bucket'              : HV_S3_TRANSACTION_BUCKET,
            'all_files_in_dir'       : True
        }
    ),
    task_id='push_ease_transaction_files',
    dag=mdag
)

ease_queue_up_for_matching_dag =  SubDagOperator(
    subdag=queue_up_for_matching.queue_up_for_matching(
        DAG_NAME,
        'ease_queue_up_for_matching',
        default_args['start_date'],
        mdag.schedule_interval,
        {
            'source_files_func'      : get_ease_deid_file_paths
        }
    ),
    task_id='ease_queue_up_for_matching',
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

fetch_ap_file_dag.set_upstream(validate_ap_file_dag)
push_ap_file_dag.set_upstream(fetch_ap_file_dag)
unzip_ap_files.set_upstream(push_ap_file_dag)
move_ap_deid_files.set_upstream(unzip_ap_files)
rename_ap_deid_files.set_upstream(move_ap_deid_files)
ap_queue_up_for_matching_dag.set_upstream(rename_ap_deid_files)
move_ap_transaction_files.set_upstream(unzip_ap_files)
rename_ap_transaction_files.set_upstream(move_ap_deid_files)
push_ap_transaction_files_dag.set_upstream(rename_ap_transaction_files)

fetch_ses_file_dag.set_upstream(validate_ses_file_dag)
push_ses_file_dag.set_upstream(fetch_ses_file_dag)
unzip_ses_files.set_upstream(push_ses_file_dag)
move_ses_deid_files.set_upstream(unzip_ses_files)
rename_ses_deid_files.set_upstream(move_ses_deid_files)
ses_queue_up_for_matching_dag.set_upstream(rename_ses_deid_files)
move_ses_transaction_files.set_upstream(unzip_ses_files)
rename_ses_transaction_files.set_upstream(move_ses_deid_files)
push_ses_transaction_files_dag.set_upstream(rename_ses_transaction_files)

fetch_ease_file_dag.set_upstream(validate_ease_file_dag)
push_ease_file_dag.set_upstream(fetch_ease_file_dag)
unzip_ease_files.set_upstream(push_ease_file_dag)
move_ease_deid_files.set_upstream(unzip_ease_files)
rename_ease_deid_files.set_upstream(move_ease_deid_files)
ease_queue_up_for_matching_dag.set_upstream(rename_ease_deid_files)
move_ease_transaction_files.set_upstream(unzip_ease_files)
rename_ease_transaction_files.set_upstream(move_ease_deid_files)
push_ease_transaction_files_dag.set_upstream(rename_ease_transaction_files)

clean_up_tmp_dir_dag.set_upstream([
    push_ap_transaction_files_dag,
    push_ses_transaction_files_dag,
    push_ease_transaction_files_dag,
    ap_queue_up_for_matching_dag,
    ses_queue_up_for_matching_dag,
    ease_queue_up_for_matching_dag
])
