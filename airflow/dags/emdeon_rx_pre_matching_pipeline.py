from airflow.models import Variable
from airflow.operators import *
from datetime import datetime, timedelta
from subprocess import check_output, check_call
import os

import subdags.split_push_files as split_push_files
import subdags.s3_validate_file as s3_validate_file

import common.HVDAG as HVDAG

import util.date_utils as date_utils

for m in [split_push_files, HVDAG, s3_validate_file, date_utils]:
    reload(m)

# Applies to all files
TMP_PATH_TEMPLATE='/tmp/webmd/pharmacyclaims/{}{}{}/'
TMP_PATH_PARTS_TEMPLATE='/tmp/webmd/pharmacyclaims/{}{}{}/parts/'
DAG_NAME='emdeon_rx_pre_matching_pipeline'
DATATYPE='pharmacyclaims'

# Transaction file
TRANSACTION_FILE_DESCRIPTION='WebMD RX transaction file'
S3_TRANSACTION_SPLIT_PATH='s3://salusv/incoming/pharmacyclaims/emdeon/'
S3_TRANSACTION_RAW_PATH='s3://healthverity/incoming/pharmacyclaims/emdeon/transactions/'
TRANSACTION_FILE_NAME_TEMPLATE='{}{}{}_RX_DEID_CF_ON.dat.gz'
TRANSACTION_DAG_NAME='validate_fetch_transaction_file'
MINIMUM_TRANSACTION_FILE_SIZE=500

# Transaction MFT file
TRANSACTION_MFT_FILE_DESCRIPTION='WebMD RX transaction mft file'
S3_TRANSACTION_MFT_RAW_PATH='s3://healthverity/incoming/pharmacyclaims/emdeon/transactions/'
TRANSACTION_MFT_FILE_NAME_TEMPLATE='{}{}{}_RX_DEID_CF_ON.dat.mft'
TRANSACTION_MFT_DAG_NAME='validate_fetch_transaction_mft_file'
MINIMUM_TRANSACTION_MFT_FILE_SIZE=15

# Deid file
DEID_FILE_DESCRIPTION='WebMD RX deid file'
S3_DEID_RAW_PATH='s3://healthverity/incoming/pharmacyclaims/emdeon/deid/'
DEID_FILE_NAME_TEMPLATE='{}{}{}_RX_Ident_CF_ON_PHI_Hash_HV_Encrypt.dat.gz'
DEID_DAG_NAME='validate_fetch_deid_file'
MINIMUM_DEID_FILE_SIZE=500

EMDEON_RX_DAY_OFFSET = -1

get_file_name_pattern = date_utils.generate_insert_regex_into_template_function(TRANSACTION_FILE_NAME_TEMPLATE)

tmp_path = date_utils.generate_insert_date_into_template_function(TMP_PATH_TEMPLATE)
tmp_path_parts = date_utils.generate_insert_date_into_template_function(TMP_PATH_PARTS_TEMPLATE)

def do_unzip_file(ds, **kwargs):
    file_path = tmp_path(ds, kwargs) + date_utils.insert_date_into_template(
        TRANSACTION_FILE_NAME_TEMPLATE,
        kwargs,
        day_offset = EMDEON_RX_DAY_OFFSET
    )
    check_call(['gzip', '-d', '-k', '-f', file_path])

def do_split_file(ds, **kwargs):
    file_name = date_utils.insert_date_into_template(
        TRANSACTION_FILE_NAME_TEMPLATE.replace('.gz', ''),
        kwargs,
        day_offset = EMDEON_RX_DAY_OFFSET
    )
    file_path = tmp_path(ds, kwargs) + file_name
    check_call(['mkdir', '-p', tmp_path_parts(ds, kwargs)])
    check_call(['split', '-n', 'l/20', file_path, '{}{}.'.format(tmp_path_parts(ds, kwargs), file_name)])

def do_zip_part_files(ds, **kwargs):
    file_list = os.listdir(tmp_path_parts(ds, kwargs))
    for file_name in file_list:
        check_call(['lbzip2', '{}{}'.format(tmp_path_parts(ds, kwargs), file_name)])

def do_push_splits_to_s3(ds, **kwargs):
    file_list = os.listdir(tmp_path_parts(ds, kwargs))
    file_name = file_list[0]
    date = '{}/{}/{}'.format(file_name[0:4], file_name[4:6], file_name[6:8])
    check_call(['aws', 's3', 'cp', '--sse', 'AES256', '--recursive', tmp_path_parts(ds, kwargs), "{}{}/".format(S3_TRANSACTION_SPLIT_PATH, date)])

def do_trigger_post_matching_dag(context, dag_run_obj):
    file_dir = tmp_path(context['ds'], context)
    transaction_file_name = date_utils.insert_date_into_template(TRANSACTION_FILE_NAME_TEMPLATE,
        context,
        day_offset = EMDEON_RX_DAY_OFFSET
    )
    deid_file_name = date_utils.insert_date_into_template(DEID_FILE_NAME_TEMPLATE,
        context,
        day_offset = EMDEON_RX_DAY_OFFSET
    )
    row_count = check_output(['zgrep', '-c', '^', file_dir + transaction_file_name])
    dag_run_obj.payload = {
            "deid_filename": deid_file_name.replace('.gz', ''),
            "row_count": str(row_count),
            "ds_yesterday": context['yesterday_ds']
        }
    return dag_run_obj

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

def generate_transaction_file_validation_dag(
        task_id, file_name_template, s3_path, minimum_file_size
):
    return SubDagOperator(
        subdag=s3_validate_file.s3_validate_file(
            DAG_NAME,
            'validate_' + task_id + '_file',
            default_args['start_date'],
            mdag.schedule_interval,
            {
                'expected_file_name_func' : date_utils.generate_insert_date_into_template_function(
                    file_name_template,
                    day_offset = EMDEON_RX_DAY_OFFSET
                ),
                'file_name_pattern_func'  : date_utils.generate_insert_regex_into_template_function(
                    file_name_template
                ),
                'minimum_file_size'       : minimum_file_size,
                's3_prefix'               : '/'.join(s3_path.split('/')[3:]),
                's3_bucket'               : s3_path.split('/')[2],
                'file_description'        : 'Emdeon ' + task_id + ' file'
            }
        ),
        task_id='validate_' + task_id + '_file',
        dag=mdag
    )


validate_transaction_file = generate_transaction_file_validation_dag(
    'transaction', TRANSACTION_FILE_NAME_TEMPLATE, S3_TRANSACTION_RAW_PATH, MINIMUM_TRANSACTION_FILE_SIZE
)

validate_transaction_mft_file = generate_transaction_file_validation_dag(
    'transaction_mft', TRANSACTION_MFT_FILE_NAME_TEMPLATE, S3_TRANSACTION_MFT_RAW_PATH, MINIMUM_TRANSACTION_MFT_FILE_SIZE
)

validate_deid_file = generate_transaction_file_validation_dag(
    'deid', DEID_FILE_NAME_TEMPLATE, S3_DEID_RAW_PATH, MINIMUM_DEID_FILE_SIZE
)


def generate_fetch_dag(
        task_id, s3_path_template, file_name_template
):
    return SubDagOperator(
        subdag=s3_fetch_file.s3_fetch_file(
            DAG_NAME,
            'fetch_' + task_id + '_file',
            default_args['start_date'],
            mdag.schedule_interval,
            {
                'tmp_path_template'      : TMP_PATH_TEMPLATE + task_id + '/',
                'expected_file_name_func': date_utils.generate_insert_date_into_template_function(
                    file_name_template,
                    day_offset = EMDEON_RX_DAY_OFFSET
                ),
                's3_prefix'              : s3_path_template,
                's3_bucket'              : 'salusv' if airflow_env == 'test' else 'healthverity'
            }
        ),
        task_id='fetch_' + task_id + '_file',
        dag=mdag
    )


fetch_transaction_file = generate_fetch_dag(
    'transaction', '/'.join(S3_TRANSACTION_RAW_PATH.split('/')[3:]), TRANSACTION_FILE_NAME_TEMPLATE
)
fetch_link_file = generate_fetch_dag(
    'link', '/'.join(S3_LINK_RAW_PATH.split('/')[3:]), LINK_FILE_NAME_TEMPLATE
)


unzip_file = PythonOperator(
    task_id='unzip_file',
    provide_context=True,
    python_callable=do_unzip_file,
    dag=mdag
)

log_file_volume = PythonOperator(
    task_id='log_file_volume',
    provide_context=True,
    python_callable=split_push_files.do_log_file_volume(
        DAG_NAME,
        get_file_name_pattern,
        lambda ds, k: [
            tmp_path(TMP_PATH_TEMPLATE, k)
            + date_utils.insert_date_into_template(
                TRANSACTION_FILE_NAME_TEMPLATE,
                k,
                day_offset = EMDEON_RX_DAY_OFFSET
            )
        ]
    ),
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
                     S3_DEID_RAW_PATH, DEID_FILE_NAME_TEMPLATE.format('{{ yesterday_ds_nodash }}','','')) +
                 ' {{ params.sequence_num }} {{ params.matching_engine_env }} {{ params.priority }}',
    params={'sequence_num' : 0,
            'matching_engine_env' : 'prod-matching-engine',
            'priority' : 'priority3'},
    env={'AWS_ACCESS_KEY_ID' : Variable.get('AWS_ACCESS_KEY_ID_MATCH_PUSHER'),
         'AWS_SECRET_ACCESS_KEY' : Variable.get('AWS_SECRET_ACCESS_KEY_MATCH_PUSHER')},
    dag=mdag
)

trigger_post_matching_dag = TriggerDagRunOperator(
    task_id='trigger_post_matching_dag',
    trigger_dag_id='emdeon_rx_post_matching_pipeline',
    python_callable=do_trigger_post_matching_dag,
    dag=mdag
)

clean_up_workspace = BashOperator(
    task_id='clean_up_workspace',
    bash_command='rm -rf {};'.format(TMP_PATH_TEMPLATE.format('{{ ds_nodash }}','','')),
    trigger_rule='all_done',
    dag=mdag
)

fetch_transaction_file.set_upstream(validate_transaction_file)
unzip_file.set_upstream(fetch_transaction_file)
log_file_volume.set_upstream(unzip_file)
split_file.set_upstream(unzip_file)
zip_part_files.set_upstream(split_file)
push_splits_to_s3.set_upstream(zip_part_files)
push_splits_to_s3.set_downstream(trigger_post_matching_dag)
validate_transaction_mft_file.set_downstream(trigger_post_matching_dag)
queue_up_for_matching.set_upstream(validate_deid_file)
queue_up_for_matching.set_downstream(trigger_post_matching_dag)
clean_up_workspace.set_upstream([trigger_post_matching_dag, log_file_volume])
