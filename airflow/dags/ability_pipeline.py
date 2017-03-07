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
import subdags.decrypt_files as decrypt_files
import subdags.split_push_files as split_push_files
import subdags.queue_up_for_matching as queue_up_for_matching
import subdags.clean_up_tmp_dir as clean_up_tmp_dir

for m in [s3_validate_file, s3_fetch_file, s3_push_files, decrypt_files,
        split_push_files, queue_up_for_matching, clean_up_tmp_dir]:
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

def get_tmp_dir(ds, kwargs):
    return TMP_PATH_TEMPLATE.format(kwargs['ds_nodash'])

def get_expected_ap_file_name(ds, kwargs):
    return AP_FILE_NAME_TEMPLATE.format(ds)

def get_ap_transaction_tmp_dir(ds, kwargs):
    return get_tmp_dir(ds, kwargs) + 'ap/transaction/'

def get_ap_file_paths(ds, kwargs):
    return [get_tmp_dir(ds, kwargs) + get_expected_ap_file_name(ds, kwargs)]

def get_expected_ap_file_regex(ds, kwargs):
    return AP_FILE_NAME_TEMPLATE.format('\d{4}-\d{2}-\d{2}')

def get_ap_transaction_files_paths(ds, kwargs):
    file_dir = get_ap_transaction_tmp_dir(ds, kwargs)
    files = os.listdir(file_dir)
    fs = []
    for f in files:
        if f[-4:] == ".txt" or f[-10:] == ".decrypted":
            fs.append(file_dir + f)
    return fs

def get_expected_ses_file_name(ds, kwargs):
    return SES_FILE_NAME_TEMPLATE.format(kwargs['yesterday_ds'], ds)

def get_ses_transaction_tmp_dir(ds, kwargs):
    return get_tmp_dir(ds, kwargs) + 'ses/transaction/'

def get_ses_file_paths(ds, kwargs):
    return [get_tmp_dir(ds, kwargs) + get_expected_ses_file_name(ds, kwargs)]

def get_expected_ses_file_regex(ds, kwargs):
    return SES_FILE_NAME_TEMPLATE.format('\d{4}-\d{2}-\d{2}', '\d{4}-\d{2}-\d{2}')

def get_ses_transaction_files_paths(ds, kwargs):
    file_dir = get_ses_transaction_tmp_dir(ds, kwargs)
    files = os.listdir(file_dir)
    fs = []
    for f in files:
        if f[-4:] == ".txt" or f[-10:] == ".decrypted":
            fs.append(file_dir + f)
    return fs

def get_expected_ease_file_name(ds, kwargs):
    return EASE_FILE_NAME_TEMPLATE.format(ds)

def get_ease_file_paths(ds, kwargs):
    return [get_tmp_dir(ds, kwargs) + get_expected_ease_file_name(ds, kwargs)]

def get_expected_ease_file_regex(ds, kwargs):
    return EASE_FILE_NAME_TEMPLATE.format('\d{4}-\d{2}-\d{2}')

def get_ease_transaction_tmp_dir(ds, kwargs):
    return get_tmp_dir(ds, kwargs) + 'ease/transaction/'

def get_ease_transaction_files_paths(ds, kwargs):
    file_dir = get_ease_transaction_tmp_dir(ds, kwargs)
    files = os.listdir(file_dir)
    fs = []
    for f in files:
        if f[-4:] == ".txt" or f[-10:] == ".decrypted":
            fs.append(file_dir + f)
    return fs

def get_s3_transaction_prefix(ds, kwargs):
    return 's3://' + HV_S3_TRANSACTION_BUCKET + '/' + HV_S3_TRANSACTION_PREFIX_TEMPLATE.format(ds.replace('-', '/'))

def get_s3_raw_prefix(ds, kwargs):
    return HV_S3_RAW_PREFIX

def get_ap_deid_file_paths(ds, kwargs):
    file_dir = get_tmp_dir(ds, kwargs) + 'ap/deid/'
    files = os.listdir(file_dir)
    return map(lambda f: file_dir + f, files)

def get_ses_deid_file_paths(ds, kwargs):
    file_dir = get_tmp_dir(ds, kwargs) + 'ses/deid/'
    files = os.listdir(file_dir)
    return map(lambda f: file_dir + f, files)

def get_ease_deid_file_paths(ds, kwargs):
    file_dir = get_tmp_dir(ds, kwargs) + 'ease/deid/'
    files = os.listdir(file_dir)
    return map(lambda f: file_dir + f, files)

def get_ap_encrypted_decrypted_file_names(ds, kwargs):
    file_dir = get_ap_transaction_tmp_dir(ds, kwargs)
    files = os.listdir(file_dir)
    fs = []
    for f in files:
        if f.find("record") == 14 and (f[-4:] == ".txt" or f[-10:] == ".decrypted.gz"):
            fs.append([file_dir + f, file_dir + f + '.decrypted.gz'])
    return fs

def get_ses_encrypted_decrypted_file_names(ds, kwargs):
    file_dir = get_ses_transaction_tmp_dir(ds, kwargs)
    files = os.listdir(file_dir)
    fs = []
    for f in files:
        if f.find("record") == 15 and (f[-4:] == ".txt" or f[-10:] == ".decrypted.gz"):
            fs.append([file_dir + f, file_dir + f + '.decrypted.gz'])
    return fs

def get_ease_encrypted_decrypted_file_names(ds, kwargs):
    file_dir = get_ease_transaction_tmp_dir(ds, kwargs)
    files = os.listdir(file_dir)
    fs = []
    for f in files:
        if f.find("record") == 16 and (f[-4:] == ".txt" or f[-10:] == ".decrypted.gz"):
            fs.append([file_dir + f, file_dir + f + '.decrypted.gz'])
    return fs

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
            'file_paths_func'        : get_ap_file_paths,
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

decrypt_ap_transaction_files_dag = SubDagOperator(
    subdag=decrypt_files.decrypt_files(
        DAG_NAME,
        'decrypt_ap_transaction_files',
        default_args['start_date'],
        mdag.schedule_interval,
        {
            'tmp_dir_func'                        : get_ap_transaction_tmp_dir,
            'encrypted_decrypted_file_names_func' : get_ap_encrypted_decrypted_file_names
        }
    ),
    task_id='decrypt_ap_transaction_files',
    dag=mdag
)

split_push_ap_transaction_files_dag = SubDagOperator(
    subdag=split_push_files.split_push_files(
        DAG_NAME,
        'split_push_ap_transaction_files',
        default_args['start_date'],
        mdag.schedule_interval,
        {
            'tmp_dir_func'             : get_ap_transaction_tmp_dir,
            'file_paths_to_split_func' : get_ap_transaction_files_paths,
            's3_prefix_func'           : get_s3_transaction_prefix,
            's3_bucket'                : HV_S3_TRANSACTION_BUCKET,
            'num_splits'               : 1
        }
    ),
    task_id='split_push_ap_transaction_files',
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
            'file_paths_func'        : get_ses_file_paths,
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

decrypt_ses_transaction_files_dag = SubDagOperator(
    subdag=decrypt_files.decrypt_files(
        DAG_NAME,
        'decrypt_ses_transaction_files',
        default_args['start_date'],
        mdag.schedule_interval,
        {
            'tmp_dir_func'                        : get_ses_transaction_tmp_dir,
            'encrypted_decrypted_file_names_func' : get_ses_encrypted_decrypted_file_names
        }
    ),
    task_id='decrypt_ses_transaction_files',
    dag=mdag
)

split_push_ses_transaction_files_dag = SubDagOperator(
    subdag=split_push_files.split_push_files(
        DAG_NAME,
        'split_push_ses_transaction_files',
        default_args['start_date'],
        mdag.schedule_interval,
        {
            'tmp_dir_func'             : get_ses_transaction_tmp_dir,
            'file_paths_to_split_func' : get_ses_transaction_files_paths,
            's3_prefix_func'           : get_s3_transaction_prefix,
            's3_bucket'                : HV_S3_TRANSACTION_BUCKET,
            'num_splits'               : 1
        }
    ),
    task_id='split_push_ses_transaction_files',
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
            'file_paths_func'        : get_ease_file_paths,
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

decrypt_ease_transaction_files_dag = SubDagOperator(
    subdag=decrypt_files.decrypt_files(
        DAG_NAME,
        'decrypt_ease_transaction_files',
        default_args['start_date'],
        mdag.schedule_interval,
        {
            'tmp_dir_func'                        : get_ease_transaction_tmp_dir,
            'encrypted_decrypted_file_names_func' : get_ease_encrypted_decrypted_file_names
        }
    ),
    task_id='decrypt_ease_transaction_files',
    dag=mdag
)

split_push_ease_transaction_files_dag = SubDagOperator(
    subdag=split_push_files.split_push_files(
        DAG_NAME,
        'split_push_ease_transaction_files',
        default_args['start_date'],
        mdag.schedule_interval,
        {
            'tmp_dir_func'             : get_ease_transaction_tmp_dir,
            'file_paths_to_split_func' : get_ease_transaction_files_paths,
            's3_prefix_func'           : get_s3_transaction_prefix,
            'num_splits'               : 1
        }
    ),
    task_id='split_push_ease_transaction_files',
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
rename_ap_transaction_files.set_upstream(move_ap_transaction_files)
decrypt_ap_transaction_files_dag.set_upstream(rename_ap_transaction_files)
split_push_ap_transaction_files_dag.set_upstream(decrypt_ap_transaction_files_dag)

fetch_ses_file_dag.set_upstream(validate_ses_file_dag)
push_ses_file_dag.set_upstream(fetch_ses_file_dag)
unzip_ses_files.set_upstream(push_ses_file_dag)
move_ses_deid_files.set_upstream(unzip_ses_files)
rename_ses_deid_files.set_upstream(move_ses_deid_files)
ses_queue_up_for_matching_dag.set_upstream(rename_ses_deid_files)
move_ses_transaction_files.set_upstream(unzip_ses_files)
rename_ses_transaction_files.set_upstream(move_ses_transaction_files)
decrypt_ses_transaction_files_dag.set_upstream(rename_ses_transaction_files)
split_push_ses_transaction_files_dag.set_upstream(decrypt_ses_transaction_files_dag)

fetch_ease_file_dag.set_upstream(validate_ease_file_dag)
push_ease_file_dag.set_upstream(fetch_ease_file_dag)
unzip_ease_files.set_upstream(push_ease_file_dag)
move_ease_deid_files.set_upstream(unzip_ease_files)
rename_ease_deid_files.set_upstream(move_ease_deid_files)
ease_queue_up_for_matching_dag.set_upstream(rename_ease_deid_files)
move_ease_transaction_files.set_upstream(unzip_ease_files)
rename_ease_transaction_files.set_upstream(move_ease_transaction_files)
decrypt_ease_transaction_files_dag.set_upstream(rename_ease_transaction_files)
split_push_ease_transaction_files_dag.set_upstream(decrypt_ease_transaction_files_dag)

clean_up_tmp_dir_dag.set_upstream([
    split_push_ap_transaction_files_dag,
    split_push_ses_transaction_files_dag,
    split_push_ease_transaction_files_dag,
    ap_queue_up_for_matching_dag,
    ses_queue_up_for_matching_dag,
    ease_queue_up_for_matching_dag
])
