from airflow import DAG
from airflow.models import Variable
from airflow.operators import BashOperator, PythonOperator, DummyOperator, BranchPythonOperator, SlackAPIOperator, SubDagOperator
from datetime import datetime, timedelta
from subprocess import check_output, check_call, STDOUT
from json import loads as json_loads
import logging
import os
import pysftp
import re
import sys

if sys.modules.get('subdags.emdeon_validate_fetch_file'):
    del sys.modules['subdags.emdeon_validate_fetch_file']
from subdags.emdeon_validate_fetch_file import emdeon_validate_fetch_file

# Applies to all files
TMP_PATH='/tmp/webmd/pharmacyclaims/'
TMP_PATH_PARTS='/tmp/webmd/pharmacyclaims/parts/'
DAG_NAME='emdeon_rx_pre_matching_pipeline'
DATATYPE='pharmacyclaims'
DAILY_NUM_FILES=3

# Transaction file
TRANSACTION_FILE_DESCRIPTION='WebMD RX transaction file'
S3_TRANSACTION_SPLIT_PATH='s3://salusv/incoming/pharmacyclaims/emdeon/'
S3_TRANSACTION_RAW_PATH='s3://healthverity/incoming/pharmacyclaims/emdeon/transactions/'
TRANSACTION_FILE_NAME_TEMPLATE='{}_RX_DEID_CF_ON.dat.gz'
TRANSACTION_DAG_NAME='validate_fetch_transaction_file'
MINIMUM_TRANSACTION_FILE_SIZE=500

# Transaction MFT file
TRANSACTION_MFT_FILE_DESCRIPTION='WebMD RX transaction mft file'
S3_TRANSACTION_MFT_RAW_PATH='s3://healthverity/incoming/pharmacyclaims/emdeon/transactions/'
TRANSACTION_MFT_FILE_NAME_TEMPLATE='{}_RX_DEID_CF_ON.dat.mft'
TRANSACTION_MFT_DAG_NAME='validate_fetch_transaction_mft_file'
MINIMUM_TRANSACTION_MFT_FILE_SIZE=15

# Deid file
DEID_FILE_DESCRIPTION='WebMD RX deid file'
S3_DEID_RAW_PATH='s3://healthverity/incoming/pharmacyclaims/emdeon/deid/'
DEID_FILE_NAME_TEMPLATE='{}_RX_Ident_CF_ON_PHI_Hash_HV_Encrypt.dat.gz'
DEID_DAG_NAME='validate_fetch_deid_file'
MINIMUM_DEID_FILE_SIZE=500

def do_unzip_file(ds, **kwargs):
    file_list = os.listdir(TMP_PATH)
    file_path = TMP_PATH + file_list[0]
    check_call(['gzip', '-d', '-k', '-f', file_path])

def do_split_file(ds, **kwargs):
    file_list = os.listdir(TMP_PATH)
    file_name = filter(lambda x: not re.search('.gz$', x), file_list)[0]
    file_path = TMP_PATH + file_name
    check_call(['mkdir', '-p', TMP_PATH_PARTS])
    check_call(['split', '-n', 'l/20', file_path, '{}{}.'.format(TMP_PATH_PARTS, file_name)])

def do_zip_part_files(ds, **kwargs):
    file_list = os.listdir(TMP_PATH_PARTS)
    for file_name in file_list:
        check_call(['lbzip2', '{}{}'.format(TMP_PATH_PARTS, file_name)])

def do_push_splits_to_s3(ds, **kwargs):
    file_list = os.listdir(TMP_PATH)
    file_name = filter(lambda x: x.find('.gz') == (len(x) - 3), file_list)[0]
    date = '{}/{}/{}'.format(file_name[0:4], file_name[4:6], file_name[6:8])
    env = os.environ
    env["AWS_ACCESS_KEY_ID"] = Variable.get('AWS_ACCESS_KEY_ID')
    env["AWS_SECRET_ACCESS_KEY"] = Variable.get('AWS_SECRET_ACCESS_KEY')
    check_call(['aws', 's3', 'cp', '--recursive', TMP_PATH_PARTS, "{}{}/".format(S3_TRANSACTION_SPLIT_PATH, date)], env=env)

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2016, 10, 1, 12),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2)
}

mdag = DAG(
    dag_id=DAG_NAME,
    schedule_interval="@once",
    default_args=default_args
)

validate_fetch_transaction_config = {
    'tmp_path' : TMP_PATH,
    's3_raw_path' : S3_TRANSACTION_RAW_PATH,
    'file_name_template' : TRANSACTION_FILE_NAME_TEMPLATE,
    'datatype' : DATATYPE,
    'daily_num_files' : DAILY_NUM_FILES,
    'minimum_file_size' : MINIMUM_TRANSACTION_FILE_SIZE,
    'file_description' : TRANSACTION_FILE_DESCRIPTION
}

validate_fetch_transaction_file_dag = SubDagOperator(
    subdag=emdeon_validate_fetch_file(DAG_NAME, TRANSACTION_DAG_NAME, default_args['start_date'], mdag.schedule_interval, validate_fetch_transaction_config),
    task_id=TRANSACTION_DAG_NAME,
    dag=mdag
)

validate_fetch_transaction_mft_config = {
    'tmp_path' : TMP_PATH,
    's3_raw_path' : S3_TRANSACTION_MFT_RAW_PATH,
    'file_name_template' : TRANSACTION_MFT_FILE_NAME_TEMPLATE,
    'datatype' : DATATYPE,
    'daily_num_files' : DAILY_NUM_FILES,
    'minimum_file_size' : MINIMUM_TRANSACTION_MFT_FILE_SIZE,
    'file_description' : TRANSACTION_MFT_FILE_DESCRIPTION
}

validate_fetch_transaction_mft_file_dag = SubDagOperator(
    subdag=emdeon_validate_fetch_file(DAG_NAME, TRANSACTION_MFT_DAG_NAME, default_args['start_date'], mdag.schedule_interval, validate_fetch_transaction_mft_config),
    task_id=TRANSACTION_MFT_DAG_NAME,
    trigger_rule='all_done',
    dag=mdag
)

validate_fetch_deid_config = {
    'tmp_path' : TMP_PATH,
    's3_raw_path' : S3_DEID_RAW_PATH,
    'file_name_template' : DEID_FILE_NAME_TEMPLATE,
    'datatype' : DATATYPE,
    'daily_num_files' : DAILY_NUM_FILES,
    'minimum_file_size' : MINIMUM_DEID_FILE_SIZE,
    'file_description' : DEID_FILE_DESCRIPTION
}

validate_fetch_deid_file_dag = SubDagOperator(
    subdag=emdeon_validate_fetch_file(DAG_NAME, DEID_DAG_NAME, default_args['start_date'], mdag.schedule_interval, validate_fetch_deid_config),
    task_id=DEID_DAG_NAME,
    trigger_rule='all_done',
    dag=mdag
)

unzip_file = PythonOperator(
    task_id='unzip_file',
    provide_context=True,
    python_callable=do_unzip_file,
    dag=mdag
)

split_file = PythonOperator(
    task_id='split_file',
    provide_context=True,
    python_callable=do_split_file,
    dag=mdag
)

zip_part_files = PythonOperator(
    task_id='zip_part_files',
    provide_context=True,
    python_callable=do_zip_part_files,
    dag=mdag
)

push_splits_to_s3 = PythonOperator(
    task_id='push_splits_to_s3',
    provide_context=True,
    python_callable=do_push_splits_to_s3,
    dag=mdag
)

unzip_file.set_upstream(validate_fetch_transaction_file_dag)
split_file.set_upstream(unzip_file)
zip_part_files.set_upstream(split_file)
push_splits_to_s3.set_upstream(zip_part_files)
validate_fetch_transaction_mft_file_dag.set_upstream(push_splits_to_s3)
validate_fetch_deid_file_dag.set_upstream(validate_fetch_transaction_mft_file_dag)
