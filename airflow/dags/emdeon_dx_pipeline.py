from airflow.models import Variable
from airflow.operators import *
from datetime import datetime, timedelta
from subprocess import check_call
import os

import common.HVDAG as HVDAG
import subdags.detect_move_normalize as detect_move_normalize
import subdags.split_push_files as split_push_files
import subdags.update_analytics_db as update_analytics_db
import subdags.s3_validate_file as s3_validate_file
import subdags.s3_fetch_file as s3_fetch_file
import util.date_utils as date_utils

for m in [
        HVDAG, detect_move_normalize, split_push_files, s3_validate_file,
        s3_fetch_file, update_analytics_db, date_utils
]:
    reload(m)

# Applies to all files
TMP_PATH_TEMPLATE='/tmp/webmd/medicalclaims/{}{}{}/'
TMP_PATH_PARTS_TEMPLATE='/tmp/webmd/medicalclaims/{}{}{}/parts/'
DAG_NAME='emdeon_dx_pipeline'
DATATYPE='medicalclaims'

# Transaction file
TRANSACTION_FILE_DESCRIPTION='WebMD DX transaction file'
S3_TRANSACTION_SPLIT_PATH='s3://salusv/incoming/medicalclaims/emdeon/'
S3_TRANSACTION_RAW_PATH='s3://healthverity/incoming/medicalclaims/emdeon/transactions/'
TRANSACTION_FILE_NAME_TEMPLATE='{}{}{}_Claims_US_CF_D_deid.dat.gz'
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
DEID_FILE_NAME_TEMPLATE='{}{}{}_Claims_US_CF_Hash_File_HV_Encrypt.dat.gz'
DEID_DAG_NAME='validate_fetch_deid_file'
MINIMUM_DEID_FILE_SIZE=500

S3_PAYLOAD_LOC='s3://salusv/matching/payload/medicalclaims/emdeon/'

EMDEON_DX_DAY_OFFSET = -1

get_tmp_path = date_utils.generate_insert_date_into_template_function(TMP_PATH_TEMPLATE)
get_tmp_path_parts = date_utils.generate_insert_date_into_template_function(TMP_PATH_PARTS_TEMPLATE)


def do_unzip_file(ds, **kwargs):
    file_path = get_tmp_path(ds,kwargs) + date_utils.insert_date_into_template(
        TRANSACTION_FILE_NAME_TEMPLATE,
        kwargs,
        day_offset = EMDEON_DX_DAY_OFFSET
    )
    check_call(['gzip', '-d', '-k', '-f', file_path])

def do_split_file(ds, **kwargs):
    file_name = date_utils.insert_date_into_template(
        TRANSACTION_FILE_NAME_TEMPLATE.replace('.gz', ''),
        kwargs,
        day_offset = EMDEON_DX_DAY_OFFSET
    )
    file_path = get_tmp_path(ds,kwargs) + file_name
    check_call(['mkdir', '-p', get_tmp_path_parts(ds,kwargs)])
    check_call(['split', '-n', 'l/20', file_path,
        '{}{}.'.format(get_tmp_path_parts(ds,kwargs), file_name)
    ])

def do_zip_part_files(ds, **kwargs):
    file_list = os.listdir(get_tmp_path_parts(ds, kwargs))
    for file_name in file_list:
        check_call(['lbzip2', '{}{}'.format(get_tmp_path_parts(ds,kwargs), file_name)])

def do_push_splits_to_s3(ds, **kwargs):
    file_list = os.listdir(get_tmp_path_parts(ds,kwargs))
    file_name = file_list[0]
    date = '{}/{}/{}'.format(file_name[0:4], file_name[4:6], file_name[6:8])
    check_call(
        ['aws', 's3', 'cp', '--sse', 'AES256', '--recursive',
            get_tmp_path_parts(ds,kwargs),
            "{}{}/".format(S3_TRANSACTION_SPLIT_PATH, date)
        ]
    )

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2017, 6, 30, 12),
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
                    kwargs,
                    day_offset = EMDEON_DX_DAY_OFFSET
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

fetch_transaction_file = SubDagOperator(
    subdag=s3_fetch_file.s3_fetch_file(
        DAG_NAME,
        'fetch_transaction_file',
        default_args['start_date'],
        mdag.schedule_interval,
        {
            'tmp_path_template'      : TMP_PATH_TEMPLATE,
            'expected_file_name_func': generate_insert_date_into_template_function(
                TRANSACTION_FILE_NAME_TEMPLATE
            ),
            's3_prefix'              : '/'.join(S3_TRANSACTION_RAW_PATH.split('/')[3:]),
            's3_bucket'              : S3_TRANSACTION_RAW_PATH.split('/')[2],
        }
    ),
    task_id='fetch_transaction_file',
    dag=mdag
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
        date_utils.generate_insert_regex_into_template_function(
            TRANSACTION_FILE_NAME_TEMPLATE
        ),
        lambda ds, k: [
            get_tmp_path(ds, k)
            + date_utils.insert_date_into_template(
                TRANSACTION_FILE_NAME_TEMPLATE,
                k,
                day_offset = EMDEON_DX_DAY_OFFSET
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
                 S3_DEID_RAW_PATH, DEID_FILE_NAME_TEMPLATE.format('{{ yesterday_ds_nodash }}','','') +
                 ' {{ params.sequence_num }} {{ params.matching_engine_env }} {{ params.priority }}'),
    params={'sequence_num' : 0,
            'matching_engine_env' : 'prod-matching-engine',
            'priority' : 'priority3'},
    env={'AWS_ACCESS_KEY_ID' : Variable.get('AWS_ACCESS_KEY_ID_MATCH_PUSHER'),
         'AWS_SECRET_ACCESS_KEY' : Variable.get('AWS_SECRET_ACCESS_KEY_MATCH_PUSHER')},
    dag=mdag
)

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
            'expected_matching_files_func'      : lambda ds, k: [
                date_utils.insert_date_into_template(
                    DEID_FILE_NAME_TEMPLATE.replace('.gz',''),
                    k,
                    day_offset = EMDEON_DX_DAY_OFFSET
                )
            ],
            'file_date_func'                    : date_utils.generate_insert_date_into_template_function(
                '{}/{}/{}',
                day_offset = EMDEON_DX_DAY_OFFSET
            ),
            's3_payload_loc_url'                : S3_PAYLOAD_LOC,
            'vendor_uuid'                       : '86396771-0345-4d67-83b3-7e22fded9e1d',
            'pyspark_normalization_script_name' : '/home/hadoop/spark/providers/emdeon/medicalclaims/sparkNormalizeEmdeonDX.py',
            'pyspark_normalization_args_func'   : lambda ds, k: [
                '--date', date_utils.insert_date_into_template('{}-{}-{}', k, day_offset = EMDEON_DX_DAY_OFFSET)
            ],
            'pyspark'                           : True
        }
    ),
    task_id='detect_move_normalize',
    dag=mdag
)

sql_template = """
    MSCK REPAIR TABLE medicalclaims_new
"""

update_analytics_db = SubDagOperator(
    subdag=update_analytics_db.update_analytics_db(
        DAG_NAME,
        'update_analytics_db',
        default_args['start_date'],
        mdag.schedule_interval,
        {
            'sql_command_func' : lambda ds, k: sql_template
        }
    ),
    task_id='update_analytics_db',
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
queue_up_for_matching.set_upstream(validate_deid_file)
detect_move_normalize_dag.set_upstream([push_splits_to_s3, validate_transaction_mft_file, queue_up_for_matching])
update_analytics_db.set_upstream(detect_move_normalize_dag)
clean_up_workspace.set_upstream([push_splits_to_s3, validate_transaction_mft_file, queue_up_for_matching, log_file_volume])
