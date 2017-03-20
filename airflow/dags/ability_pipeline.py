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
import subdags.detect_move_normalize as detect_move_normalize

for m in [s3_validate_file, s3_fetch_file, s3_push_files, decrypt_files,
        split_push_files, queue_up_for_matching, clean_up_tmp_dir,
        detect_move_normalize]:
    reload(m)

# Applies to all files
TMP_PATH_TEMPLATE='/tmp/ability/medicalclaims/{}/'
DAG_NAME='ability_pipeline'

S3_TEXT_ABILITY_PREFIX = 'warehouse/text/medicalclaims/ability/'
S3_PARQUET_ABILITY_PREFIX = 'warehouse/parquet/medicalclaims/abillity/'
S3_PAYLOAD_LOC_ABILITY_URL = 's3://salusv/matching/payload/medicalclaims/ability/'

# Ability S3 bucket access
ABILITY_S3_BUCKET=Variable.get('Ability_S3_Bucket')
ABILITY_S3_CONNECTION='ability_s3_conn'

HV_S3_RAW_PREFIX='incoming/ability/'
HV_S3_RAW_BUCKET='healthverity'

HV_S3_TRANSACTION_PREFIX='incoming/medicalclaims/ability/'
HV_S3_TRANSACTION_PREFIX_TEMPLATE=HV_S3_TRANSACTION_PREFIX+'{}/'
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

def get_transaction_files_paths_func(tmp_dir_func):
    def get_transaction_files_paths(ds, kwargs):
        file_dir = tmp_dir_func(ds, kwargs)
        files = os.listdir(file_dir)
        fs = []
        for f in files:
            if f[-4:] == ".txt" or f[-10:] == ".decrypted":
                fs.append(file_dir + f)
        return fs
    return get_transaction_files_paths

get_ap_transaction_files_paths = get_transaction_files_paths_func(get_ap_transaction_tmp_dir)

def get_expected_ses_file_name(ds, kwargs):
    return SES_FILE_NAME_TEMPLATE.format(kwargs['yesterday_ds'], ds)

def get_ses_transaction_tmp_dir(ds, kwargs):
    return get_tmp_dir(ds, kwargs) + 'ses/transaction/'

def get_ses_file_paths(ds, kwargs):
    return [get_tmp_dir(ds, kwargs) + get_expected_ses_file_name(ds, kwargs)]

def get_expected_ses_file_regex(ds, kwargs):
    return SES_FILE_NAME_TEMPLATE.format('\d{4}-\d{2}-\d{2}', '\d{4}-\d{2}-\d{2}')

get_ses_transaction_files_paths = get_transaction_files_paths_func(get_ses_transaction_tmp_dir)

def get_expected_ease_file_name(ds, kwargs):
    return EASE_FILE_NAME_TEMPLATE.format(ds)

def get_ease_file_paths(ds, kwargs):
    return [get_tmp_dir(ds, kwargs) + get_expected_ease_file_name(ds, kwargs)]

def get_expected_ease_file_regex(ds, kwargs):
    return EASE_FILE_NAME_TEMPLATE.format('\d{4}-\d{2}-\d{2}')

def get_ease_transaction_tmp_dir(ds, kwargs):
    return get_tmp_dir(ds, kwargs) + 'ease/transaction/'

get_ease_transaction_files_paths = get_transaction_files_paths_func(get_ease_transaction_tmp_dir)

def get_s3_transaction_path(ds, kwargs):
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

def get_ap_encrypted_decrypted_file_paths(ds, kwargs):
    file_dir = get_ap_transaction_tmp_dir(ds, kwargs)
    files = os.listdir(file_dir)
    fs = []
    for f in files:
        if f.find("record") == 14 and f[-4:] == ".txt":
            fs.append([file_dir + f, file_dir + f + '.gz'])
    return fs

def get_ses_encrypted_decrypted_file_paths(ds, kwargs):
    file_dir = get_ses_transaction_tmp_dir(ds, kwargs)
    files = os.listdir(file_dir)
    fs = []
    for f in files:
        if f.find("record") == 15 and f[-4:] == ".txt":
            fs.append([file_dir + f, file_dir + f + '.gz'])
    return fs

def get_ease_encrypted_decrypted_file_paths(ds, kwargs):
    file_dir = get_ease_transaction_tmp_dir(ds, kwargs)
    files = os.listdir(file_dir)
    fs = []
    for f in files:
        if f.find("record") == 16 and f[-4:] == ".txt":
            fs.append([file_dir + f, file_dir + f + '.gz'])
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

def get_expected_matching_files(ds, kwargs):
    payloads_per_product = [
        'deid.vwheader.txt', 
        'deid.vwpatient.txt',
        'deid.vwpayer.txt',
        'deid.vwsubscriber.txt'
    ]
    res = []
    for product in ['ap', 'ses', 'ease']:
        for payload in payloads_per_product:
            res.append(ds.replace('-', '_') + '_' + product + '_' + payload)

    return res
    
def get_file_date(ds, kwargs):
    return ds

def get_parquet_dates(ds, kwargs):
    return [ds[:7]]

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

def validate_file_subdag(product, expected_file_name_func, file_name_pattern_func, minimum_file_size,
        s3_prefix, file_description):
    return SubDagOperator(
        subdag=s3_validate_file.s3_validate_file(
            DAG_NAME,
            'validate_{}_file'.format(product),
            default_args['start_date'],
            mdag.schedule_interval,
            {
                'expected_file_name_func': expected_file_name_func,
                'file_name_pattern_func' : file_name_pattern_func,
                'minimum_file_size'      : minimum_file_size,
                's3_prefix'              : s3_prefix,
                's3_bucket'              : ABILITY_S3_BUCKET,
                'aws_access_key_id'      : Variable.get('Ability_AWS_ACCESS_KEY_ID'),
                'aws_secret_access_key'  : Variable.get('Ability_AWS_SECRET_ACCESS_KEY'),
                's3_connection'          : ABILITY_S3_CONNECTION,
                'file_description'       : file_description
            }
        ),
        task_id='validate_{}_file'.format(product),
        dag=mdag
    )

def fetch_file_subdag(product, expected_file_name_func, s3_prefix):
    return SubDagOperator(
        subdag=s3_fetch_file.s3_fetch_file(
            DAG_NAME,
            'fetch_{}_file'.format(product),
            default_args['start_date'],
            mdag.schedule_interval,
            {
                'tmp_path_template'      : TMP_PATH_TEMPLATE,
                'expected_file_name_func': expected_file_name_func,
                's3_prefix'              : s3_prefix,
                's3_bucket'              : ABILITY_S3_BUCKET,
                's3_connection'          : ABILITY_S3_CONNECTION
            }
        ),
        task_id='fetch_{}_file'.format(product),
        dag=mdag
    )

def push_file_subdag(product, file_paths_func):
    return SubDagOperator(
        subdag=s3_push_files.s3_push_files(
            DAG_NAME,
            'push_{}_file'.format(product),
            default_args['start_date'],
            mdag.schedule_interval,
            {
                'file_paths_func'        : file_paths_func,
                's3_prefix_func'         : get_s3_raw_prefix,
                's3_bucket'              : HV_S3_RAW_BUCKET
            }
        ),
        task_id='push_{}_file'.format(product),
        dag=mdag
    )

def unzip_files_operator(product, expected_file_name_func, dest_dir):
    return PythonOperator(
        task_id='unzip_{}_files'.format(product),
        provide_context=True,
        python_callable=do_unzip_files,
        op_kwargs={
            'expected_file_name_func': expected_file_name_func,
            'tmp_path_template'      : TMP_PATH_TEMPLATE,
            'dest_dir'               : dest_dir
            },
        dag=mdag
    )

def move_files_operator(product, files_type, origin_dir, filename_pattern, dest_dir):
    return PythonOperator(
        task_id='move_{}_{}_files'.format(product, files_type),
        provide_context=True,
        python_callable=do_move_files,
        op_kwargs={
            'tmp_path_template'      : TMP_PATH_TEMPLATE,
            'origin_dir'             : origin_dir,
            'filename_pattern'       : filename_pattern,
            'dest_dir'               : dest_dir
            },
        dag=mdag
    )

def rename_files_operator(product, files_type, file_dir, prefix):
    return PythonOperator(
        task_id='rename_{}_{}_files'.format(product, files_type),
        provide_context=True,
        python_callable=do_rename_files,
        op_kwargs={
            'tmp_path_template'      : TMP_PATH_TEMPLATE,
            'file_dir'               : file_dir,
            'prefix'                 : prefix
            },
        dag=mdag
    )

def decrypt_transaction_files_subdag(product, tmp_dir_func, encrypted_decrypted_file_paths_func):
    return SubDagOperator(
        subdag=decrypt_files.decrypt_files(
            DAG_NAME,
            'decrypt_{}_transaction_files'.format(product),
            default_args['start_date'],
            mdag.schedule_interval,
            {
                'tmp_dir_func'                        : tmp_dir_func,
                'encrypted_decrypted_file_paths_func' : encrypted_decrypted_file_paths_func
            }
        ),
        task_id='decrypt_{}_transaction_files'.format(product),
        dag=mdag
    )

def split_push_transaction_files_subdag(product, tmp_dir_func, file_paths_to_split_func):
    return SubDagOperator(
        subdag=split_push_files.split_push_files(
            DAG_NAME,
            'split_push_{}_transaction_files'.format(product),
            default_args['start_date'],
            mdag.schedule_interval,
            {
                'tmp_dir_func'             : tmp_dir_func,
                'file_paths_to_split_func' : file_paths_to_split_func,
                's3_dest_path_func'        : get_s3_transaction_path,
                'num_splits'               : 1
            }
        ),
        task_id='split_push_{}_transaction_files'.format(product),
        dag=mdag
    )

def queue_up_for_matching_subdag(product, source_files_func):
    return SubDagOperator(
        subdag=queue_up_for_matching.queue_up_for_matching(
            DAG_NAME,
            'queue_up_{}_for_matching'.format(product),
            default_args['start_date'],
            mdag.schedule_interval,
            {
                'source_files_func' : source_files_func
            }
        ),
        task_id='queue_up_{}_for_matching'.format(product),
        dag=mdag
    )


validate_ap_file_dag = validate_file_subdag('ap', get_expected_ap_file_name, get_expected_ap_file_regex,
        MINIMUM_AP_FILE_SIZE, ABILITY_S3_AP_PREFIX, AP_FILE_DESCRIPTION)

fetch_ap_file_dag = fetch_file_subdag('ap', get_expected_ap_file_name, ABILITY_S3_AP_PREFIX)

push_ap_file_dag = push_file_subdag('ap', get_ap_file_paths)

unzip_ap_files = unzip_files_operator('ap', get_expected_ap_file_name, 'ap/')

move_ap_deid_files = move_files_operator('ap', 'deid', 'ap/', '^deid', 'ap/deid/')

move_ap_transaction_files = move_files_operator('ap', 'transaction', 'ap/', '^(?!deid)', 'ap/transaction/')

rename_ap_deid_files = rename_files_operator('ap', 'deid', 'ap/deid/', 'ap')

rename_ap_transaction_files = rename_files_operator('ap', 'transaction', 'ap/transaction/', 'ap')

decrypt_ap_transaction_files_dag = decrypt_transaction_files_subdag('ap', get_ap_transaction_tmp_dir,
        get_ap_encrypted_decrypted_file_paths)

split_push_ap_transaction_files_dag = split_push_transaction_files_subdag('ap', get_ap_transaction_tmp_dir,
        get_ap_transaction_files_paths)

queue_up_ap_for_matching_dag = queue_up_for_matching_subdag('ap', get_ap_deid_file_paths)

validate_ses_file_dag = validate_file_subdag('ses', get_expected_ses_file_name, get_expected_ses_file_regex,
        MINIMUM_SES_FILE_SIZE, ABILITY_S3_SES_PREFIX, SES_FILE_DESCRIPTION)

fetch_ses_file_dag = fetch_file_subdag('ses', get_expected_ses_file_name, ABILITY_S3_SES_PREFIX)

push_ses_file_dag = push_file_subdag('ses', get_ses_file_paths)

unzip_ses_files = unzip_files_operator('ses', get_expected_ses_file_name, 'ses/')

move_ses_deid_files = move_files_operator('ses', 'deid', 'ses/', '^deid', 'ses/deid/')

move_ses_transaction_files = move_files_operator('ses', 'transaction', 'ses/', '^(?!deid)', 'ses/transaction/')

rename_ses_deid_files = rename_files_operator('ses', 'deid', 'ses/deid/', 'ses')

rename_ses_transaction_files = rename_files_operator('ses', 'transaction', 'ses/transaction/', 'ses')

decrypt_ses_transaction_files_dag = decrypt_transaction_files_subdag('ses', get_ses_transaction_tmp_dir,
        get_ses_encrypted_decrypted_file_paths)

split_push_ses_transaction_files_dag = split_push_transaction_files_subdag('ses', get_ses_transaction_tmp_dir,
        get_ses_transaction_files_paths)

queue_up_ses_for_matching_dag = queue_up_for_matching_subdag('ses', get_ses_deid_file_paths)

validate_ease_file_dag = validate_file_subdag('ease', get_expected_ease_file_name, get_expected_ease_file_regex,
        MINIMUM_EASE_FILE_SIZE, ABILITY_S3_EASE_PREFIX, EASE_FILE_DESCRIPTION)

fetch_ease_file_dag = fetch_file_subdag('ease', get_expected_ease_file_name, ABILITY_S3_EASE_PREFIX)

push_ease_file_dag = push_file_subdag('ease', get_ease_file_paths)

unzip_ease_files = unzip_files_operator('ease', get_expected_ease_file_name, 'ease/')

move_ease_deid_files = move_files_operator('ease', 'deid', 'ease/', '^deid', 'ease/deid/')

move_ease_transaction_files = move_files_operator('ease', 'transaction', 'ease/', '^(?!deid)', 'ease/transaction/')

rename_ease_deid_files = rename_files_operator('ease', 'deid', 'ease/deid/', 'ease')

rename_ease_transaction_files = rename_files_operator('ease', 'transaction', 'ease/transaction/', 'ease')

decrypt_ease_transaction_files_dag = decrypt_transaction_files_subdag('ease', get_ease_transaction_tmp_dir,
        get_ease_encrypted_decrypted_file_paths)

split_push_ease_transaction_files_dag = split_push_transaction_files_subdag('ease', get_ease_transaction_tmp_dir,
        get_ease_transaction_files_paths)

queue_up_ease_for_matching_dag = queue_up_for_matching_subdag('ease', get_ease_deid_file_paths)

detect_move_normalize_dag = SubDagOperator(
    subdag=detect_move_normalize.detect_move_normalize(
        DAG_NAME,
        'detect_move_normalize',
        default_args['start_date'],
        mdag.schedule_interval,
        {
            'expected_matching_files_func'   : get_expected_matching_files,
            'file_date_func'                 : get_file_date,
            'incoming_path'                  : HV_S3_TRANSACTION_PREFIX,
            'normalization_routine_directory': '/home/airflow/airflow/dags/providers/ability/',
            'normalization_routine_script'   : '/home/airflow/airflow/dags/providers/ability/rsNormalizeAbilityDaily.py',
            'parquet_dates_func'             : get_parquet_dates,
            's3_text_path_prefix'            : S3_TEXT_ABILITY_PREFIX,
            's3_parquet_path_prefix'         : S3_PARQUET_ABILITY_PREFIX,
            's3_payload_loc_url'             : S3_PAYLOAD_LOC_ABILITY_URL,
            'vendor_description'             : 'Ability DX',
            'vendor_uuid'                    : '10d4caa3-056e-42c7-aab9-401ca375fee1',
            'feed_data_type'                 : 'medical-old'
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

fetch_ap_file_dag.set_upstream(validate_ap_file_dag)
push_ap_file_dag.set_upstream(fetch_ap_file_dag)
unzip_ap_files.set_upstream(push_ap_file_dag)
move_ap_deid_files.set_upstream(unzip_ap_files)
rename_ap_deid_files.set_upstream(move_ap_deid_files)
queue_up_ap_for_matching_dag.set_upstream(rename_ap_deid_files)
move_ap_transaction_files.set_upstream(unzip_ap_files)
rename_ap_transaction_files.set_upstream(move_ap_transaction_files)
decrypt_ap_transaction_files_dag.set_upstream(rename_ap_transaction_files)
split_push_ap_transaction_files_dag.set_upstream(decrypt_ap_transaction_files_dag)

fetch_ses_file_dag.set_upstream(validate_ses_file_dag)
push_ses_file_dag.set_upstream(fetch_ses_file_dag)
unzip_ses_files.set_upstream(push_ses_file_dag)
move_ses_deid_files.set_upstream(unzip_ses_files)
rename_ses_deid_files.set_upstream(move_ses_deid_files)
queue_up_ses_for_matching_dag.set_upstream(rename_ses_deid_files)
move_ses_transaction_files.set_upstream(unzip_ses_files)
rename_ses_transaction_files.set_upstream(move_ses_transaction_files)
decrypt_ses_transaction_files_dag.set_upstream(rename_ses_transaction_files)
split_push_ses_transaction_files_dag.set_upstream(decrypt_ses_transaction_files_dag)

fetch_ease_file_dag.set_upstream(validate_ease_file_dag)
push_ease_file_dag.set_upstream(fetch_ease_file_dag)
unzip_ease_files.set_upstream(push_ease_file_dag)
move_ease_deid_files.set_upstream(unzip_ease_files)
rename_ease_deid_files.set_upstream(move_ease_deid_files)
queue_up_ease_for_matching_dag.set_upstream(rename_ease_deid_files)
move_ease_transaction_files.set_upstream(unzip_ease_files)
rename_ease_transaction_files.set_upstream(move_ease_transaction_files)
decrypt_ease_transaction_files_dag.set_upstream(rename_ease_transaction_files)
split_push_ease_transaction_files_dag.set_upstream(decrypt_ease_transaction_files_dag)

clean_up_tmp_dir_dag.set_upstream([
    split_push_ap_transaction_files_dag,
    split_push_ses_transaction_files_dag,
    split_push_ease_transaction_files_dag,
    queue_up_ap_for_matching_dag,
    queue_up_ses_for_matching_dag,
    queue_up_ease_for_matching_dag
])

detect_move_normalize_dag.set_upstream([
    split_push_ap_transaction_files_dag,
    split_push_ses_transaction_files_dag,
    split_push_ease_transaction_files_dag,
    queue_up_ap_for_matching_dag,
    queue_up_ses_for_matching_dag,
    queue_up_ease_for_matching_dag
])
