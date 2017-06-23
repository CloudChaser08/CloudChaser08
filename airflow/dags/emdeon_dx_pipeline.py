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

import common.HVDAG as HVDAG
import subdags.emdeon_validate_fetch_file

for m in [subdags.emdeon_validate_fetch_file, HVDAG]:
    reload(m)

from subdags.emdeon_validate_fetch_file import emdeon_validate_fetch_file

# Applies to all files
TMP_PATH_TEMPLATE='/tmp/webmd/medicalclaims/{}/'
TMP_PATH_PARTS_TEMPLATE='/tmp/webmd/medicalclaims/{}/parts/'
DAG_NAME='emdeon_dx_pre_matching_pipeline'
DATATYPE='medicalclaims'

# Transaction file
TRANSACTION_FILE_DESCRIPTION='WebMD DX transaction file'
S3_TRANSACTION_SPLIT_PATH='s3://salusv/incoming/medicalclaims/emdeon/'
S3_TRANSACTION_RAW_PATH='s3://healthverity/incoming/medicalclaims/emdeon/transactions/'
TRANSACTION_FILE_NAME_TEMPLATE='{}_Claims_US_CF_D_deid.dat.gz'
TRANSACTION_DAG_NAME='validate_fetch_transaction_file'
MINIMUM_TRANSACTION_FILE_SIZE=500

# Transaction MFT file
TRANSACTION_MFT_FILE_DESCRIPTION='WebMD DX transaction mft file'
S3_TRANSACTION_MFT_RAW_PATH='s3://healthverity/incoming/medicalclaims/emdeon/transactions/'
TRANSACTION_MFT_FILE_NAME_TEMPLATE='{}_Claims_US_CF_D_deid.dat.mft'
TRANSACTION_MFT_DAG_NAME='validate_fetch_transaction_mft_file'
MINIMUM_TRANSACTION_MFT_FILE_SIZE=15

# Deid file
DEID_FILE_DESCRIPTION='WebMD DX deid file'
S3_DEID_RAW_PATH='s3://healthverity/incoming/medicalclaims/emdeon/deid/'
DEID_FILE_NAME_TEMPLATE='{}_Claims_US_CF_Hash_File_HV_Encrypt.dat.gz'
DEID_DAG_NAME='validate_fetch_deid_file'
MINIMUM_DEID_FILE_SIZE=500

def do_unzip_file(ds, **kwargs):
    tmp_path = TMP_PATH_TEMPLATE.format(kwargs['ds_nodash'])
    file_path = tmp_path + TRANSACTION_FILE_NAME_TEMPLATE.format(kwargs['yesterday_ds_nodash'])
    check_call(['gzip', '-d', '-k', '-f', file_path])

def do_split_file(ds, **kwargs):
    tmp_path = TMP_PATH_TEMPLATE.format(kwargs['ds_nodash'])
    tmp_path_parts = TMP_PATH_PARTS_TEMPLATE.format(kwargs['ds_nodash'])
    file_name = TRANSACTION_FILE_NAME_TEMPLATE.replace('.gz', '').format(kwargs['yesterday_ds_nodash'])
    file_path = tmp_path + file_name
    check_call(['mkdir', '-p', tmp_path_parts])
    check_call(['split', '-n', 'l/20', file_path, '{}{}.'.format(tmp_path_parts, file_name)])

def do_zip_part_files(ds, **kwargs):
    tmp_path = TMP_PATH_TEMPLATE.format(kwargs['ds_nodash'])
    tmp_path_parts = TMP_PATH_PARTS_TEMPLATE.format(kwargs['ds_nodash'])

    file_list = os.listdir(tmp_path_parts)
    for file_name in file_list:
        check_call(['lbzip2', '{}{}'.format(tmp_path_parts, file_name)])

def do_push_splits_to_s3(ds, **kwargs):
    tmp_path = TMP_PATH_TEMPLATE.format(kwargs['ds_nodash'])
    tmp_path_parts = TMP_PATH_PARTS_TEMPLATE.format(kwargs['ds_nodash'])

    file_list = os.listdir(tmp_path_parts)
    file_name = file_list[0]
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

validate_fetch_deid_config = {
    'tmp_path_template' : TMP_PATH_TEMPLATE,
    's3_raw_path' : S3_DEID_RAW_PATH,
    'file_name_template' : DEID_FILE_NAME_TEMPLATE,
    'datatype' : DATATYPE,
    'minimum_file_size' : MINIMUM_DEID_FILE_SIZE,
    'file_description' : DEID_FILE_DESCRIPTION
}

validate_fetch_deid_file_dag = SubDagOperator(
    subdag=emdeon_validate_fetch_file(DAG_NAME, DEID_DAG_NAME, default_args['start_date'], mdag.schedule_interval, validate_fetch_deid_config),
    task_id=DEID_DAG_NAME,
    trigger_rule='all_done',
    retries=0,
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

queue_up_for_matching = BashOperator(
    task_id='queue_up_for_matching',
    bash_command='/home/airflow/airflow/dags/resources/push_file_to_s3_batchless.sh {}{}'.format(
                 S3_DEID_RAW_PATH, (DEID_FILE_NAME_TEMPLATE.format('{{ yesterday_ds_nodash }}')) + 
                 ' {{ params.sequence_num }} {{ params.matching_engine_env }} {{ params.priority }}'),
    params={'sequence_num' : 0,
            'matching_engine_env' : 'prod-matching-engine',
            'priority' : 'priority3'},
    env={'AWS_ACCESS_KEY_ID' : Variable.get('AWS_ACCESS_KEY_ID_MATCH_PUSHER'),
         'AWS_SECRET_ACCESS_KEY' : Variable.get('AWS_SECRET_ACCESS_KEY_MATCH_PUSHER')},
    dag=mdag
)

def insert_file_date_func(template):
    def out(ds, kwargs):
        return template.format(
            kwargs['ds_yesterday'][0:4],
            kwargs['ds_yesterday'][5:7],
            kwargs['ds_yesterday'][8:10]
        )
    return out

def expected_matching_files_func(ds, kwargs):
    deid_file_name = DEID_FILE_NAME_TEMPLATE.format(kwargs['yesterday_ds_nodash'])
    return [deid_file_name.replace('.gz', '')]


#
# Post-Matching
#
detect_move_normalize_dag = SubDagOperator(
    subdag=detect_move_normalize.detect_move_normalize(
        DAG_NAME,
        'detect_move_normalize',
        default_args['start_date'],
        mdag.schedule_interval,
        {
            'expected_matching_files_func'      : expected_matching_files_func,
            'file_date_func'                    : insert_file_date_func(
                '{}/{}/{}'
            ),
            's3_payload_loc_url'                : S3_PAYLOAD_LOC,
            'vendor_uuid'                       : '86396771-0345-4d67-83b3-7e22fded9e1d',
            'pyspark_normalization_script_name' : '/home/hadoop/spark/providers/emdeon/medicalclaims/sparkNormalizeEmdeonDX.py',
            'pyspark_normalization_args_func'   : lambda ds, k: [
                '--date', insert_file_date_func('{}-{}-{}')(ds, k)
            ],
            'pyspark'                           : True
        }
    ),
    task_id='detect_move_normalize',
    dag=mdag
)

sql_template = """
    ALTER TABLE medicalclaims_new ADD PARTITION (part_provider='emdeon', part_best_date='{0}-{1}')
    LOCATION 's3a://salusv/warehouse/parquet/medicalclaims/2017-02-24/part_provider=emdeon/part_best_date={0}-{1}/'
"""

update_analytics_db = SubDagOperator(
    subdag=update_analytics_db.update_analytics_db(
        DAG_NAME,
        'update_analytics_db_new',
        default_args['start_date'],
        mdag.schedule_interval,
        {
            'sql_command_func' : lambda ds, k: insert_file_date(sql_template)(ds, k) \
                if insert_file_date('{}-{}-{}')(ds, k).find('-01') == 7 else ''
        }
    ),
    task_id='update_analytics_db',
    dag=mdag
)

clean_up_workspace = BashOperator(
    task_id='clean_up_workspace',
    bash_command='rm -rf {};'.format(TMP_PATH_TEMPLATE.format('{{ ds_nodash }}')),
    trigger_rule='all_done',
    dag=mdag
)

unzip_file.set_upstream(validate_fetch_transaction_file_dag)
split_file.set_upstream(unzip_file)
zip_part_files.set_upstream(split_file)
push_splits_to_s3.set_upstream(zip_part_files)
push_splits_to_s3.set_downstream(trigger_post_matching_dag)
validate_fetch_transaction_mft_file_dag.set_downstream(trigger_post_matching_dag)
queue_up_for_matching.set_upstream(validate_fetch_deid_file_dag)
queue_up_for_matching.set_downstream(trigger_post_matching_dag)
detect_move_normalize_dag.set_upstream([push_splits_to_s3, validate_fetch_transaction_mft_file_dag, queue_up_for_matching])
update_analytics_db.set_upstream(detect_move_normalize_dag)
clean_up_workspace.set_upstream([push_splits_to_s3, validate_fetch_transaction_mft_file_dag, queue_up_for_matching])
