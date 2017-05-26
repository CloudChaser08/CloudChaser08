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

import subdags.emdeon_validate_fetch_file
import common.HVDAG as HVDAG

for m in [subdags.emdeon_validate_fetch_file, HVDAG]:
    reload(m)

from subdags.emdeon_validate_fetch_file import emdeon_validate_fetch_file

# Applies to all files
TMP_PATH_TEMPLATE='/tmp/webmd/era/{}/'
TMP_PATH_PARTS_TEMPLATE='/tmp/webmd/era/{}/parts/'
DAG_NAME='emdeon_era_pre_matching_pipeline'
DATATYPE='era'

# Transaction file
TRANSACTION_FILE_DESCRIPTION='WebMD ERA transaction file'
S3_TRANSACTION_RAW_PATH='s3://healthverity/incoming/era/emdeon/transactions/'
TRANSACTION_FILE_NAME_TEMPLATE='{}_AF_ERA_CF_ON_CS_deid.dat.gz'
TRANSACTION_DAG_NAME='validate_fetch_transaction_file'
MINIMUM_TRANSACTION_FILE_SIZE=500

# Transaction MFT file
TRANSACTION_MFT_FILE_DESCRIPTION='WebMD ERA transaction mft file'
S3_TRANSACTION_MFT_RAW_PATH='s3://healthverity/incoming/era/emdeon/transactions/'
TRANSACTION_MFT_FILE_NAME_TEMPLATE='{}_AF_ERA_CF_ON_CS_deid.dat.mft'
TRANSACTION_MFT_DAG_NAME='validate_fetch_transaction_mft_file'
MINIMUM_TRANSACTION_MFT_FILE_SIZE=15

def do_unzip_file(ds, **kwargs):
    tmp_path = TMP_PATH_TEMPLATE.format(kwargs['ds_nodash'])
    file_path = tmp_path + TRANSACTION_FILE_NAME_TEMPLATE.format(kwargs['ds_yesterday_nodash'])
    check_call(['gzip', '-d', '-k', '-f', file_path])

def do_split_file(ds, **kwargs):
    tmp_path = TMP_PATH_TEMPLATE.format(kwargs['ds_nodash'])
    tmp_path_parts = TMP_PATH_PARTS_TEMPLATE.format(kwargs['ds_nodash'])
    file_list = os.listdir(tmp_path)
    file_name = filter(lambda x: not re.search('.gz$', x), file_list)[0]
    file_path = tmp_path + file_name
    check_call(['mkdir', '-p', tmp_path_parts])
    check_call(['split', '-n', 'l/20', file_path, '{}{}.'.format(tmp_path_parts, file_name)])

def do_zip_part_files(ds, **kwargs):
    tmp_path_parts = TMP_PATH_PARTS_TEMPLATE.format(kwargs['ds_nodash'])
    file_list = os.listdir(tmp_path_parts)
    for file_name in file_list:
        check_call(['lbzip2', '{}{}'.format(tmp_path_parts, file_name)])

def do_push_splits_to_s3(ds, **kwargs):
    tmp_path = TMP_PATH_TEMPLATE.format(kwargs['ds_nodash'])
    tmp_path_parts = TMP_PATH_PARTS_TEMPLATE.format(kwargs['ds_nodash'])
    file_list = os.listdir(tmp_path)
    file_name = filter(lambda x: x.find('.gz') == (len(x) - 3), file_list)[0]
    date = '{}/{}/{}'.format(file_name[0:4], file_name[4:6], file_name[6:8])
    check_call(['aws', 's3', 'cp', '--sse', 'AES256', '--recursive', tmp_path_parts, "{}{}/".format(S3_TRANSACTION_SPLIT_PATH, date)])

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2016, 12, 1, 12),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2)
}

mdag = HVDAG.HVDAG(
    dag_id=DAG_NAME,
    schedule_interval="0 12 * * *",
    default_args=default_args
)

validate_fetch_transaction_config = {
    'tmp_path_template' : TMP_PATH_TEMPLATE,
    's3_raw_path' : S3_TRANSACTION_RAW_PATH,
    'file_name_template' : TRANSACTION_FILE_NAME_TEMPLATE,
    'datatype' : DATATYPE,
    'minimum_file_size' : MINIMUM_TRANSACTION_FILE_SIZE,
    'file_description' : TRANSACTION_FILE_DESCRIPTION
}

validate_fetch_transaction_file_dag = SubDagOperator(
    subdag=emdeon_validate_fetch_file(DAG_NAME, TRANSACTION_DAG_NAME, default_args['start_date'], mdag.schedule_interval, validate_fetch_transaction_config),
    task_id=TRANSACTION_DAG_NAME,
    retries=0,
    dag=mdag
)

validate_fetch_transaction_mft_config = {
    'tmp_path_template' : TMP_PATH_TEMPLATE,
    's3_raw_path' : S3_TRANSACTION_MFT_RAW_PATH,
    'file_name_template' : TRANSACTION_MFT_FILE_NAME_TEMPLATE,
    'datatype' : DATATYPE,
    'minimum_file_size' : MINIMUM_TRANSACTION_MFT_FILE_SIZE,
    'file_description' : TRANSACTION_MFT_FILE_DESCRIPTION
}

validate_fetch_transaction_mft_file_dag = SubDagOperator(
    subdag=emdeon_validate_fetch_file(DAG_NAME, TRANSACTION_MFT_DAG_NAME, default_args['start_date'], mdag.schedule_interval, validate_fetch_transaction_mft_config),
    task_id=TRANSACTION_MFT_DAG_NAME,
    trigger_rule='all_done',
    retries=0,
    dag=mdag
)

#unzip_file = PythonOperator(
#    task_id='unzip_file',
#    provide_context=True,
#    python_callable=do_unzip_file,
#    dag=mdag
#)
#
#split_file = PythonOperator(
#    task_id='split_file',
#    provide_context=True,
#    python_callable=do_split_file,
#    dag=mdag
#)
#
#zip_part_files = PythonOperator(
#    task_id='zip_part_files',
#    provide_context=True,
#    python_callable=do_zip_part_files,
#    dag=mdag
#)
#
#push_splits_to_s3 = PythonOperator(
#    task_id='push_splits_to_s3',
#    provide_context=True,
#    python_callable=do_push_splits_to_s3,
#    dag=mdag
#)

clean_up_workspace = BashOperator(
    task_id='clean_up_workspace',
    bash_command='rm -rf {};'.format(TMP_PATH_TEMPLATE.format('{{ ds_nodash }}')),
    trigger_rule='all_done',
    dag=mdag
)

#unzip_file.set_upstream(validate_fetch_transaction_file_dag)
#split_file.set_upstream(unzip_file)
#zip_part_files.set_upstream(split_file)
#push_splits_to_s3.set_upstream(zip_part_files)

# We don't do anything with the ERA files yet
# Just make sure they are in the right locations
validate_fetch_transaction_mft_file_dag.set_downstream(clean_up_workspace)
validate_fetch_transaction_file_dag.set_downstream(clean_up_workspace)
