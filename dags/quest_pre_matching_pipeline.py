from airflow import DAG
from airflow.models import Variable
from airflow.operators import BashOperator, PythonOperator
from datetime import datetime, timedelta
import sys
import util.file_utils as file_utils

if sys.modules.get('subdags.emdeon_validate_fetch_file'):
    del sys.modules['subdags.emdeon_validate_fetch_file']

# Applies to all files
TMP_PATH_TEMPLATE = '/tmp/quest/labtests/{}/'
DAG_NAME = 'quest pre_matching_pipeline'
DATATYPE = 'labtests'

# Applies to all transaction files
S3_TRANSACTION_RAW_PATH = 's3://healthverity/incoming/quest/'

# Transaction Trunk file
TRANSACTION_TRUNK_TMP_PATH_TEMPLATE = TMP_PATH_TEMPLATE + 'raw/trunk/'
TRANSACTION_TRUNK_TMP_PATH_PARTS_TEMPLATE = TMP_PATH_TEMPLATE + 'parts/trunk/'
TRANSACTION_TRUNK_S3_SPLIT_PATH = 's3://salusv/incoming/labtests/quest/trunk/'
TRANSACTION_TRUNK_FILE_DESCRIPTION = 'Quest transaction trunk file'
TRANSACTION_TRUNK_FILE_NAME_TEMPLATE = 'HealthVerity_{}_1_PlainTxt.txt.zip'
TRANSACTION_TRUNK_DAG_NAME = 'validate_fetch_transaction_trunk_file'
MINIMUM_TRANSACTION_FILE_SIZE = 500

# Transaction ADDON file
TRANSACTION_ADDON_TMP_PATH_TEMPLATE = TMP_PATH_TEMPLATE + 'raw/addon/'
TRANSACTION_ADDON_TMP_PATH_PARTS_TEMPLATE = TMP_PATH_TEMPLATE + 'parts/addon/'
TRANSACTION_ADDON_S3_SPLIT_PATH = 's3://salusv/incoming/labtests/quest/addon/'
TRANSACTION_ADDON_FILE_DESCRIPTION = 'Quest transaction addon file'
TRANSACTION_ADDON_FILE_NAME_TEMPLATE = 'HealthVerity_{}_2.gz.zip'
TRANSACTION_ADDON_DAG_NAME = 'validate_fetch_transaction_addon_file'
MINIMUM_TRANSACTION_ADDON_FILE_SIZE = 15

# Deid file
DEID_FILE_DESCRIPTION = 'Quest deid file'
S3_DEID_RAW_PATH = 's3://healthverity/incoming/labtests/quest/'
DEID_FILE_NAME_TEMPLATE = 'HealthVerity {}_1_DeID.txt.zip'
DEID_DAG_NAME = 'validate_fetch_deid_file'
MINIMUM_DEID_FILE_SIZE = 500

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2016, 12, 1, 12),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2)
}

mdag = DAG(
    dag_id=DAG_NAME,
    schedule_interval="0 12 * * *",
    default_args=default_args
)


def unzip_step(tmp_path):
    return PythonOperator(
        task_id='unzip_file_' + tmp_path.substring(
            0, len(tmp_path)-1
        ).split('/').last,
        provide_context=True,
        python_callable=file_utils.unzip(tmp_path),
        dag=mdag
    )
unzip_trunk = unzip_step(TRANSACTION_TRUNK_TMP_PATH_TEMPLATE)
unzip_addon = unzip_step(TRANSACTION_ADDON_TMP_PATH_TEMPLATE)

decrypt_trunk = PythonOperator(
    task_id='decrypt_file',
    provide_context=True,
    python_callable=file_utils.decrypt(
        Variable.get('AWS_ACCESS_KEY_ID'),
        Variable.get('AWS_SECRET_ACCESS_KEY'),
        TRANSACTION_TRUNK_TMP_PATH_TEMPLATE
    ),
    dag=mdag
)


def gunzip_step(tmp_path):
    return PythonOperator(
        task_id='gunzip_file_' + tmp_path.substring(
            0, len(tmp_path)-1
        ).split('/').last,
        provide_context=True,
        python_callable=file_utils.gunzip(tmp_path),
        dag=mdag
    )
gunzip_trunk = gunzip_step(TRANSACTION_TRUNK_TMP_PATH_TEMPLATE)
gunzip_addon = gunzip_step(TRANSACTION_ADDON_TMP_PATH_TEMPLATE)


def split_step(tmp_path, tmp_parts_path):
    return PythonOperator(
        task_id='split_file_' + tmp_path.substring(
            0, len(tmp_path)-1
        ).split('/').last,
        provide_context=True,
        python_callable=file_utils.split_file(tmp_path, tmp_parts_path),
        dag=mdag
    )
split_trunk = split_step(
    TRANSACTION_TRUNK_TMP_PATH_TEMPLATE,
    TRANSACTION_TRUNK_TMP_PATH_PARTS_TEMPLATE
)
split_addon = split_step(
    TRANSACTION_ADDON_TMP_PATH_TEMPLATE,
    TRANSACTION_ADDON_TMP_PATH_PARTS_TEMPLATE
)


def bzip_parts_step(tmp_parts_path):
    return PythonOperator(
        task_id='bzip_part_files_' + tmp_parts_path.substring(
            0, len(tmp_parts_path)-1
        ).split('/').last,
        provide_context=True,
        python_callable=file_utils.bzip_part_files(tmp_parts_path),
        dag=mdag
    )
bzip_parts_trunk = bzip_parts_step(TRANSACTION_TRUNK_TMP_PATH_PARTS_TEMPLATE)
bzip_parts_addon = bzip_parts_step(TRANSACTION_ADDON_TMP_PATH_PARTS_TEMPLATE)


def push_splits_to_s3_step(tmp_path, tmp_parts_path, s3_path):
    PythonOperator(
        task_id='push_splits_to_s3_' + tmp_path.substring(
            0, len(tmp_path)-1
        ).split('/').last,
        provide_context=True,
        python_callable=file_utils.push_splits_to_s3(
            tmp_path, tmp_parts_path, s3_path,
            Variable.get('AWS_ACCESS_KEY_ID'),
            Variable.get('AWS_SECRET_ACCESS_KEY')
        ),
        dag=mdag
    )
push_splits_to_s3_trunk = push_splits_to_s3_step(
    TRANSACTION_TRUNK_TMP_PATH_TEMPLATE,
    TRANSACTION_TRUNK_TMP_PATH_PARTS_TEMPLATE,
    TRANSACTION_TRUNK_S3_SPLIT_PATH
)
push_splits_to_s3_addon = push_splits_to_s3_step(
    TRANSACTION_ADDON_TMP_PATH_TEMPLATE,
    TRANSACTION_ADDON_TMP_PATH_PARTS_TEMPLATE,
    TRANSACTION_ADDON_S3_SPLIT_PATH
)


clean_up_workspace = BashOperator(
    task_id='clean_up_workspace',
    bash_command='rm -rf {};'.format(
        TMP_PATH_TEMPLATE.format('{{ ds_nodash }}')
    ),
    trigger_rule='all_done',
    dag=mdag
)

# trunk
decrypt_trunk.set_upstream(unzip_trunk)
gunzip_trunk.set_upstream(decrypt_trunk)
split_trunk.set_upstream(gunzip_trunk)
bzip_parts_trunk.set_upstream(split_trunk)
push_splits_to_s3_trunk.set_upstream(bzip_parts_trunk)

# addon
gunzip_addon.set_upstream(unzip_addon)
split_addon.set_upstream(gunzip_addon)
bzip_parts_addon.set_upstream(split_addon)
push_splits_to_s3_addon.set_upstream(bzip_parts_addon)

# all
clean_up_workspace.set_upstream(push_splits_to_s3_addon)
