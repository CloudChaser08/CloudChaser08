from airflow import DAG
from airflow.models import Variable
from airflow.operators import BashOperator, PythonOperator
from datetime import datetime, timedelta
import sys
import re

if sys.modules.get('util.file_utils'):
    del sys.modules['util.file_utils']
import util.file_utils as file_utils

# Applies to all files
TMP_PATH_TEMPLATE = '/tmp/quest/labtests/{}/'
DAG_NAME = 'quest_pre_matching_pipeline'
DATATYPE = 'labtests'

# Applies to all transaction files
S3_TRANSACTION_RAW_PATH = 's3://healthverity/incoming/quest/'

# Transaction Addon file
TRANSACTION_ADDON_TMP_PATH_TEMPLATE = TMP_PATH_TEMPLATE + 'raw/addon/'
TRANSACTION_ADDON_TMP_PATH_PARTS_TEMPLATE = TMP_PATH_TEMPLATE + 'parts/addon/'
TRANSACTION_ADDON_S3_SPLIT_PATH = 's3://salusv/incoming/labtests/quest/{}/{}/{}/addon/'
TRANSACTION_ADDON_FILE_DESCRIPTION = 'Quest transaction addon file'
TRANSACTION_ADDON_FILE_NAME_TEMPLATE = 'HealthVerity_{}_1_PlainTxt.txt.zip'
TRANSACTION_ADDON_DAG_NAME = 'validate_fetch_transaction_addon_file'
MINIMUM_TRANSACTION_FILE_SIZE = 500

# Transaction Trunk file
TRANSACTION_TRUNK_TMP_PATH_TEMPLATE = TMP_PATH_TEMPLATE + 'raw/trunk/'
TRANSACTION_TRUNK_TMP_PATH_PARTS_TEMPLATE = TMP_PATH_TEMPLATE + 'parts/trunk/'
TRANSACTION_TRUNK_S3_SPLIT_PATH = 's3://salusv/incoming/labtests/quest/{}/{}/{}/trunk/'
TRANSACTION_TRUNK_FILE_DESCRIPTION = 'Quest transaction trunk file'
TRANSACTION_TRUNK_FILE_NAME_TEMPLATE = 'HealthVerity_{}_2.gz.zip'
TRANSACTION_TRUNK_DAG_NAME = 'validate_fetch_transaction_trunk_file'
MINIMUM_TRANSACTION_TRUNK_FILE_SIZE = 15

# Deid file
DEID_FILE_DESCRIPTION = 'Quest deid file'
S3_DEID_RAW_PATH = 's3://healthverity/incoming/labtests/quest/'
DEID_FILE_NAME_TEMPLATE = 'HealthVerity_{}_1_DeID.txt.zip'
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
    schedule_interval="0 13 * * *",
    default_args=default_args
)

yesterday = datetime.today() - timedelta(1)
two_days_ago = yesterday - timedelta(1)
formatted_date = two_days_ago.strftime('%Y%m%d') + yesterday.strftime('%m%d')


def fetch_step(task_id, s3_path, local_path):
    return PythonOperator(
        task_id='fetch_' + task_id,
        provide_context=True,
        python_callable=file_utils.fetch_file_from_s3(
            Variable.get("AWS_ACCESS_KEY_ID"),
            Variable.get("AWS_SECRET_ACCESS_KEY"),
            s3_path,
            local_path
        ),
        dag=mdag
    )
fetch_addon = fetch_step(
    "addon",
    "{}{}".format(S3_TRANSACTION_RAW_PATH, TRANSACTION_ADDON_FILE_NAME_TEMPLATE.format(formatted_date)),
    TRANSACTION_ADDON_TMP_PATH_TEMPLATE.format(formatted_date)
)
fetch_trunk = fetch_step(
    "trunk",
    "{}{}".format(S3_TRANSACTION_RAW_PATH, TRANSACTION_TRUNK_FILE_NAME_TEMPLATE.format(formatted_date)), 
    TRANSACTION_TRUNK_TMP_PATH_TEMPLATE.format(formatted_date)
)


def unzip_step(task_id, tmp_path):
    return PythonOperator(
        task_id='unzip_file_' + task_id,
        provide_context=True,
        python_callable=file_utils.unzip(formatted_date, tmp_path),
        dag=mdag
    )
unzip_addon = unzip_step("addon", TRANSACTION_ADDON_TMP_PATH_TEMPLATE)
unzip_trunk = unzip_step("trunk", TRANSACTION_TRUNK_TMP_PATH_TEMPLATE)

decrypt_addon = PythonOperator(
    task_id='decrypt_file',
    provide_context=True,
    python_callable=file_utils.decrypt(
        Variable.get('AWS_ACCESS_KEY_ID'),
        Variable.get('AWS_SECRET_ACCESS_KEY'),
        formatted_date,
        TRANSACTION_ADDON_TMP_PATH_TEMPLATE
    ),
    dag=mdag
)


def gunzip_step(task_id, tmp_path):
    return PythonOperator(
        task_id='gunzip_file_' + task_id,
        provide_context=True,
        python_callable=file_utils.gunzip(formatted_date, tmp_path),
        dag=mdag
    )
gunzip_addon = gunzip_step("addon", TRANSACTION_ADDON_TMP_PATH_TEMPLATE)
gunzip_trunk = gunzip_step("trunk", TRANSACTION_TRUNK_TMP_PATH_TEMPLATE)


def split_step(task_id, tmp_path, tmp_parts_path):
    return PythonOperator(
        task_id='split_file_' + task_id,
        provide_context=True,
        python_callable=file_utils.split(formatted_date, tmp_path, tmp_parts_path),
        dag=mdag
    )
split_addon = split_step(
    "addon",
    TRANSACTION_ADDON_TMP_PATH_TEMPLATE,
    TRANSACTION_ADDON_TMP_PATH_PARTS_TEMPLATE
)
split_trunk = split_step(
    "trunk",
    TRANSACTION_TRUNK_TMP_PATH_TEMPLATE,
    TRANSACTION_TRUNK_TMP_PATH_PARTS_TEMPLATE
)


def bzip_parts_step(task_id, tmp_parts_path):
    return PythonOperator(
        task_id='bzip_part_files_' + task_id,
        provide_context=True,
        python_callable=file_utils.bzip_part_files(formatted_date, tmp_parts_path),
        dag=mdag
    )
bzip_parts_addon = bzip_parts_step("addon", TRANSACTION_ADDON_TMP_PATH_PARTS_TEMPLATE)
bzip_parts_trunk = bzip_parts_step("trunk", TRANSACTION_TRUNK_TMP_PATH_PARTS_TEMPLATE)


def push_splits_to_s3_step(task_id, tmp_path, tmp_parts_path, s3_path):
    return PythonOperator(
        task_id='push_splits_to_s3_' + task_id,
        provide_context=True,
        python_callable=file_utils.push_splits_to_s3(
            tmp_parts_path, s3_path,
            Variable.get('AWS_ACCESS_KEY_ID'),
            Variable.get('AWS_SECRET_ACCESS_KEY')
        ),
        dag=mdag
    )
push_splits_to_s3_addon = push_splits_to_s3_step(
    "addon",
    TRANSACTION_ADDON_TMP_PATH_TEMPLATE,
    TRANSACTION_ADDON_TMP_PATH_PARTS_TEMPLATE,
    TRANSACTION_ADDON_S3_SPLIT_PATH.format(
        re.sub('[^0-9]', '', formatted_date)[0:4],
        re.sub('[^0-9]', '', formatted_date)[4:6],
        re.sub('[^0-9]', '', formatted_date)[6:8]
    )
)
push_splits_to_s3_trunk = push_splits_to_s3_step(
    "trunk",
    TRANSACTION_TRUNK_TMP_PATH_TEMPLATE,
    TRANSACTION_TRUNK_TMP_PATH_PARTS_TEMPLATE,
    TRANSACTION_TRUNK_S3_SPLIT_PATH.format(
        re.sub('[^0-9]', '', formatted_date)[0:4],
        re.sub('[^0-9]', '', formatted_date)[4:6],
        re.sub('[^0-9]', '', formatted_date)[6:8]
    )
)


clean_up_workspace = BashOperator(
    task_id='clean_up_workspace',
    bash_command='rm -rf {};'.format(
        TMP_PATH_TEMPLATE.format(formatted_date)
    ),
    trigger_rule='all_done',
    dag=mdag
)

# addon
unzip_addon.set_upstream(fetch_addon)
decrypt_addon.set_upstream(unzip_addon)
gunzip_addon.set_upstream(decrypt_addon)
split_addon.set_upstream(gunzip_addon)
bzip_parts_addon.set_upstream(split_addon)
push_splits_to_s3_addon.set_upstream(bzip_parts_addon)

# trunk
fetch_trunk.set_upstream(push_splits_to_s3_addon)
unzip_trunk.set_upstream(fetch_trunk)
gunzip_trunk.set_upstream(unzip_trunk)
split_trunk.set_upstream(gunzip_trunk)
bzip_parts_trunk.set_upstream(split_trunk)
push_splits_to_s3_trunk.set_upstream(bzip_parts_trunk)

# all
clean_up_workspace.set_upstream(push_splits_to_s3_trunk)
