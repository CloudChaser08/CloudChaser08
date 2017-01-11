from airflow import DAG
from airflow.models import Variable
from airflow.operators import PythonOperator
import airflow.hooks.S3_hook
from datetime import datetime, timedelta
import os
import logging
import sys
import re
from subprocess import check_call, Popen
import time

if sys.modules.get('config'):
    del sys.modules['config']
import config

if sys.modules.get('util.file_utils'):
    del sys.modules['util.file_utils']
import util.file_utils as file_utils

# Applies to all files
TMP_PATH_TEMPLATE = '/tmp/quest/labtests/{}/'
DAG_NAME = 'quest_pre_matching_pipeline'
DATATYPE = 'labtests'

# Applies to all transaction files
S3_TRANSACTION_RAW_PATH = 's3://healthverity/incoming/quest/'

# Decryptor Config
TMP_DECRYPTOR_PATH_TEMPLATE = TMP_PATH_TEMPLATE + 'decrypt/'

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
DEID_FILE_NAME_TEMPLATE = 'HealthVerity_{}_1_DeID.txt.zip'
DEID_DAG_NAME = 'validate_fetch_deid_file'
MINIMUM_DEID_FILE_SIZE = 500

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2017, 02, 01, 12),
    'depends_on_past': False
    # 'retries': 3,
    # 'retry_delay': timedelta(minutes=2)
}

mdag = DAG(
    dag_id=DAG_NAME,
    schedule_interval="0 13 * * *",
    default_args=default_args
)

env = file_utils.get_s3_env(
    Variable.get('AWS_ACCESS_KEY_ID'),
    Variable.get('AWS_SECRET_ACCESS_KEY')
)


def get_formatted_date(kwargs):
    return (
        datetime.strptime(
            kwargs['yesterday_ds_nodash'], '%Y%m%d'
        ) - timedelta(days=1)
    ).strftime('%Y%m%d') + kwargs['yesterday_ds_nodash'][4:8]

#
# Pre-Matching
#
def fetch_step(task_id, s3_path_template, local_path_template):
    def execute(ds, **kwargs):
        file_utils.fetch_file_from_s3(
            Variable.get("AWS_ACCESS_KEY_ID"),
            Variable.get("AWS_SECRET_ACCESS_KEY"),
            s3_path_template.format(get_formatted_date(kwargs)),
            local_path_template.format(get_formatted_date(kwargs))
        )
    return PythonOperator(
        task_id='fetch_' + task_id,
        provide_context=True,
        python_callable=execute,
        dag=mdag
    )
# fetch_addon = fetch_step(
#     "addon",
#     "{}{}".format(
#         S3_TRANSACTION_RAW_PATH,
#         TRANSACTION_ADDON_FILE_NAME_TEMPLATE
#     ),
#     TRANSACTION_ADDON_TMP_PATH_TEMPLATE
# )
# fetch_trunk = fetch_step(
#     "trunk",
#     '{}{}'.format(
#         S3_TRANSACTION_RAW_PATH,
#         TRANSACTION_TRUNK_FILE_NAME_TEMPLATE
#     ),
#     TRANSACTION_TRUNK_TMP_PATH_TEMPLATE
# )


def unzip_step(task_id, tmp_path_template):
    def execute(ds, **kwargs):
        file_utils.unzip(
            tmp_path_template.format(
                get_formatted_date(kwargs)
            )
        )
    return PythonOperator(
        task_id='unzip_file_' + task_id,
        provide_context=True,
        python_callable=execute,
        dag=mdag
    )
# unzip_addon = unzip_step(
#     "addon", TRANSACTION_ADDON_TMP_PATH_TEMPLATE
# )
# unzip_trunk = unzip_step(
#     "trunk", TRANSACTION_TRUNK_TMP_PATH_TEMPLATE
# )


def decrypt_step(task_id, tmp_path_template):
    def execute(ds, **kwargs):
        file_utils.decrypt(
            Variable.get('AWS_ACCESS_KEY_ID'),
            Variable.get('AWS_SECRET_ACCESS_KEY'),
            TMP_DECRYPTOR_PATH_TEMPLATE.format(
                get_formatted_date(kwargs)
            ),
            TRANSACTION_ADDON_TMP_PATH_TEMPLATE.format(
                get_formatted_date(kwargs)
            )
        )
    return PythonOperator(
        task_id='decrypt_file_' + task_id,
        provide_context=True,
        python_callable=execute,
        dag=mdag
    )
# decrypt_addon = decrypt_step("addon", TRANSACTION_ADDON_TMP_PATH_TEMPLATE)


def gunzip_step(task_id, tmp_path_template):
    def execute(ds, **kwargs):
        file_utils.gunzip(
            tmp_path_template.format(
                get_formatted_date(kwargs)
            )
        )
    return PythonOperator(
        task_id='gunzip_file_' + task_id,
        provide_context=True,
        python_callable=execute,
        dag=mdag
    )
# gunzip_addon = gunzip_step(
#     "addon", TRANSACTION_ADDON_TMP_PATH_TEMPLATE
# )
# gunzip_trunk = gunzip_step(
#     "trunk", TRANSACTION_TRUNK_TMP_PATH_TEMPLATE
# )


def split_step(task_id, tmp_path_template, tmp_parts_path_template):
    def execute(ds, **kwargs):
        file_utils.split(
            tmp_path_template.format(
                get_formatted_date(kwargs)
            ),
            tmp_parts_path_template.format(
                get_formatted_date(kwargs)
            )
        )
    return PythonOperator(
        task_id='split_file_' + task_id,
        provide_context=True,
        python_callable=execute,
        dag=mdag
    )
# split_addon = split_step(
#     "addon",
#     TRANSACTION_ADDON_TMP_PATH_TEMPLATE,
#     TRANSACTION_ADDON_TMP_PATH_PARTS_TEMPLATE
# )
# split_trunk = split_step(
#     "trunk",
#     TRANSACTION_TRUNK_TMP_PATH_TEMPLATE,
#     TRANSACTION_TRUNK_TMP_PATH_PARTS_TEMPLATE
# )


def bzip_parts_step(task_id, tmp_parts_path_template):
    def execute(ds, **kwargs):
        file_utils.bzip_part_files(
            tmp_parts_path_template.format(
                get_formatted_date(kwargs)
            )
        )
    return PythonOperator(
        task_id='bzip_part_files_' + task_id,
        provide_context=True,
        python_callable=execute,
        dag=mdag
    )
# bzip_parts_addon = bzip_parts_step(
#     "addon", TRANSACTION_ADDON_TMP_PATH_PARTS_TEMPLATE
# )
# bzip_parts_trunk = bzip_parts_step(
#     "trunk", TRANSACTION_TRUNK_TMP_PATH_PARTS_TEMPLATE
# )


def push_splits_to_s3_step(task_id, tmp_parts_path, s3_path):
    def execute(ds, **kwargs):
        formatted_date = get_formatted_date(kwargs)
        file_utils.push_splits_to_s3(
            tmp_parts_path.format(formatted_date),
            s3_path.format(
                re.sub('[^0-9]', '', formatted_date)[0:4],
                re.sub('[^0-9]', '', formatted_date)[4:6],
                re.sub('[^0-9]', '', formatted_date)[6:8]
            ),
            Variable.get('AWS_ACCESS_KEY_ID'),
            Variable.get('AWS_SECRET_ACCESS_KEY')
        )
    return PythonOperator(
        task_id='push_splits_to_s3_' + task_id,
        provide_context=True,
        python_callable=execute,
        dag=mdag
    )
# push_splits_to_s3_addon = push_splits_to_s3_step(
#     "addon",
#     TRANSACTION_ADDON_TMP_PATH_PARTS_TEMPLATE,
#     TRANSACTION_ADDON_S3_SPLIT_PATH
# )
# push_splits_to_s3_trunk = push_splits_to_s3_step(
#     "trunk",
#     TRANSACTION_TRUNK_TMP_PATH_PARTS_TEMPLATE,
#     TRANSACTION_TRUNK_S3_SPLIT_PATH
# )


def queue_up_for_matching_step(seq_num, engine_env, priority):
    def execute(ds, **kwargs):
        deid_file = '{}{}'.format(
            S3_TRANSACTION_RAW_PATH, DEID_FILE_NAME_TEMPLATE.format(
                get_formatted_date(kwargs)
            ))
        check_call([
            '/home/airflow/airflow/dags/resources/push_file_to_s3.sh',
            deid_file, seq_num, engine_env, priority
        ], env=file_utils.get_s3_env(
            Variable.get('AWS_ACCESS_KEY_ID_MATCH_PUSHER'),
            Variable.get('AWS_SECRET_ACCESS_KEY_MATCH_PUSHER')
        ))
    return PythonOperator(
        task_id='queue_up_for_matching',
        provide_context=True,
        python_callable=execute,
        dag=mdag
    )
# queue_up_for_matching = queue_up_for_matching_step(
#     '0', 'prod-matching-engine', 'priority3'
# )


def clean_up_workspace_step():
    def execute(ds, **kwargs):
        check_call([
            'rm', '-rf', TMP_PATH_TEMPLATE.format(get_formatted_date(kwargs))
        ])
    return PythonOperator(
        task_id='clean_up_workspace',
        provide_context=True,
        python_callable=execute,
        trigger_rule='all_done',
        dag=mdag
    )
# clean_up_workspace = clean_up_workspace_step()

#
# Post-Matching
#
S3_PAYLOAD_BUCKET = 'salusv'
S3_PAYLOAD_KEY = 'matching/prod/payload/1b3f553d-7db8-43f3-8bb0-6e0b327320d9/'
S3_PAYLOAD_DEST = 's3://salusv/matching/payload/labtests/quest/'


def detect_matching_done_step():
    def execute(ds, **kwargs):
        hook = airflow.hooks.S3_hook.S3Hook(s3_conn_id='my_conn_s3')
        row_count = int(kwargs['dag_run'].conf['row_count'])
        chunk_start = row_count / 1000000 * 1000000
        deid_filename = kwargs['dag_run'].conf['deid_filename']
        template = '{}{}'
        if row_count >= 1000000:
            template += '{}-{}'
        template += '*'
        s3_key = template.format(
            S3_PAYLOAD_BUCKET + '/' + S3_PAYLOAD_KEY,
            deid_filename, chunk_start, row_count
        )
        logging.info('Poking for key : {}'.format(s3_key))
        while not hook.check_for_wildcard_key(s3_key, None):
            time.sleep(60)
    return PythonOperator(
        task_id='detect_matching_done',
        provide_context=True,
        python_callable=execute,
        execution_timeout=timedelta(hours=6),
        dag=mdag
    )
detect_matching_done = detect_matching_done_step()


def move_matching_payload_step():
    def execute(ds, **kwargs):
        hook = airflow.hooks.S3_hook.S3Hook(s3_conn_id='my_conn_s3')
        deid_filename = kwargs['dag_run'].conf['deid_filename']
        for payload_file in hook.list_keys(
                S3_PAYLOAD_BUCKET, S3_PAYLOAD_KEY + '/' + deid_filename
        ):
            date = '{}/{}/{}'.format(
                deid_filename[0:4], deid_filename[4:6], deid_filename[6:8]
            )
            check_call([
                'aws', 's3', 'cp',
                's3://' + S3_PAYLOAD_BUCKET + '/' + payload_file,
                S3_PAYLOAD_DEST + date + '/' + payload_file.split('/')[-1]
            ], env=env)
    return PythonOperator(
        task_id='move_matching_payload',
        provide_context=True,
        python_callable=execute,
        dag=mdag
    )
move_matching_payload = move_matching_payload_step()

#
# Normalization
#
CLUSTER_ID_TEMPLATE = 'quest-norm-{}'
RS_HOST_TEMPLATE = config.REDSHIFT_HOST_URL_TEMPLATE.format(
    CLUSTER_ID_TEMPLATE
)
RS_NUM_NODES = 5


def create_redshift_cluster_step():
    def execute(ds, **kwargs):
        check_call([
            '/home/airflow/airflow/dags/resources/redshift.py', 'create',
            '--identifier',
            CLUSTER_ID_TEMPLATE.format(get_formatted_date(kwargs)),
            '--num_nodes', 2
        ], env=env)
    return PythonOperator(
        task_id='create-redshift-cluster',
        provide_context=True,
        python_callable=execute,
        dag=mdag
    )
# create_redshift_cluster = create_redshift_cluster_step()


def normalize_step():
    def execute(ds, **kwargs):
        hook = airflow.hooks.S3_hook.S3Hook(s3_conn_id='my_conn_s3')
        file_date = (
            datetime.strptime(
                kwargs['yesterday_ds'], '%Y-%m-%d'
            ) - timedelta(days=1)
        ).strftime('%Y/%m/%d')
        s3_key = hook.list_keys(
            'salusv', 'incoming/medicalclaims/quest/{}'.format(file_date)
        )[0]
        setid = s3_key.split('/')[-1].replace('.bz2', '')[0:-3]
        s3_credentials = 'aws_access_key_id={};' + \
                         'aws_secret_access_key={}'.format(
                             Variable.get('AWS_ACCESS_KEY_ID'),
                             Variable.get('AWS_SECRET_ACCESS_KEY')
                         )
        command = [
            '/home/airflow/airflow/dags/providers/quest/rsNormalizeQuest.py',
            '--date', file_date, '--setid', setid,
            '--s3_credentials', s3_credentials
        ]
        env = dict(os.environ)
        env['PGHOST'] = RS_HOST_TEMPLATE.format(get_formatted_date(kwargs))
        env['PGUSER'] = config.REDSHIFT_USER
        env['PGDATABASE'] = config.REDSHIFT_DATABASE
        env['PGPORT'] = config.REDSHIFT_PORT
        env['PGPASSWORD'] = Variable.get('rs_norm_password')
        cwd = '/home/airflow/airflow/dags/providers/quest/'
        p = Popen(command, env=env, cwd=cwd)
        p.wait()
    return PythonOperator(
        task_id='normalize',
        provide_context=True,
        python_callable=execute,
        dag=mdag
    )
# normalize = normalize_step()


def delete_redshift_cluster_step():
    def execute(ds, **kwargs):
        check_call([
            '/home/airflow/airflow/dags/resources/redshift.py', 'delete',
            '--identifier',
            CLUSTER_ID_TEMPLATE.format(get_formatted_date(kwargs))
        ], env=env)
    return PythonOperator(
        task_id='delete-redshift-cluster',
        provide_context=True,
        python_callable=execute,
        dag=mdag
    )
# delete_redshift_cluster = delete_redshift_cluster_step()

# # addon
# unzip_addon.set_upstream(fetch_addon)
# decrypt_addon.set_upstream(unzip_addon)
# gunzip_addon.set_upstream(decrypt_addon)
# split_addon.set_upstream(gunzip_addon)
# bzip_parts_addon.set_upstream(split_addon)
# push_splits_to_s3_addon.set_upstream(bzip_parts_addon)

# # trunk
# fetch_trunk.set_upstream(push_splits_to_s3_addon)
# unzip_trunk.set_upstream(fetch_trunk)
# gunzip_trunk.set_upstream(unzip_trunk)
# split_trunk.set_upstream(gunzip_trunk)
# bzip_parts_trunk.set_upstream(split_trunk)
# push_splits_to_s3_trunk.set_upstream(bzip_parts_trunk)

# # queue and cleanup
# queue_up_for_matching.set_upstream(push_splits_to_s3_trunk)
# clean_up_workspace.set_upstream(queue_up_for_matching)

# post-matching
# detect_matching_done.set_upstream(clean_up_workspace)
move_matching_payload.set_upstream(detect_matching_done)

# normalization
# create_redshift_cluster.set_upstream(move_matching_payload)
# normalize.set_upstream(create_redshift_cluster)
# delete_redshift_cluster.set_upstream(normalize)
