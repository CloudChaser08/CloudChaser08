from airflow import DAG
from airflow.models import Variable
from airflow.operators import PythonOperator

from datetime import datetime, timedelta
import logging
import sys
from subprocess import check_call, check_output
import time

if sys.modules.get('config'):
    del sys.modules['config']
import config

if sys.modules.get('util.file_utils'):
    del sys.modules['util.file_utils']
import util.file_utils as file_utils

if sys.modules.get('util.aws_utils'):
    del sys.modules['util.aws_utils']
import util.aws_utils as aws_utils

# Applies to all files
TMP_PATH_TEMPLATE = '/tmp/quest/labtests/{}/'
DAG_NAME = 'quest_pre_matching_pipeline'
DATATYPE = 'labtests'

# Applies to all transaction files
S3_TRANSACTION_RAW_PATH = 's3://healthverity/incoming/quest/'
S3_TRANSACTION_PROCESSED_PATH_TEMPLATE = 's3://salusv/incoming/labtests/quest/{}/{}/{}/'

# Decryptor Config
TMP_DECRYPTOR_PATH_TEMPLATE = TMP_PATH_TEMPLATE + 'decrypt/'

# Transaction Addon file
TRANSACTION_ADDON_TMP_PATH_TEMPLATE = TMP_PATH_TEMPLATE + 'raw/addon/'
TRANSACTION_ADDON_TMP_PATH_PARTS_TEMPLATE = TMP_PATH_TEMPLATE + 'parts/addon/'
TRANSACTION_ADDON_S3_SPLIT_PATH = S3_TRANSACTION_PROCESSED_PATH_TEMPLATE + 'addon/'
TRANSACTION_ADDON_FILE_DESCRIPTION = 'Quest transaction addon file'
TRANSACTION_ADDON_FILE_NAME_TEMPLATE = 'HealthVerity_{}_1_PlainTxt.txt.zip'
TRANSACTION_ADDON_DAG_NAME = 'validate_fetch_transaction_addon_file'
MINIMUM_TRANSACTION_FILE_SIZE = 500

# Transaction Trunk file
TRANSACTION_TRUNK_TMP_PATH_TEMPLATE = TMP_PATH_TEMPLATE + 'raw/trunk/'
TRANSACTION_TRUNK_TMP_PATH_PARTS_TEMPLATE = TMP_PATH_TEMPLATE + 'parts/trunk/'
TRANSACTION_TRUNK_S3_SPLIT_PATH = S3_TRANSACTION_PROCESSED_PATH_TEMPLATE + 'trunk/'
TRANSACTION_TRUNK_FILE_DESCRIPTION = 'Quest transaction trunk file'
TRANSACTION_TRUNK_UNZIPPED_FILE_NAME_TEMPLATE = 'HealthVerity_{}_2'
TRANSACTION_TRUNK_FILE_NAME_TEMPLATE = 'HealthVerity_{}_2.gz.zip'
TRANSACTION_TRUNK_DAG_NAME = 'validate_fetch_transaction_trunk_file'
MINIMUM_TRANSACTION_TRUNK_FILE_SIZE = 15

# Deid file
DEID_FILE_DESCRIPTION = 'Quest deid file'
DEID_FILE_NAME_TEMPLATE = 'HealthVerity_{}_1_DeID.txt.zip'
DEID_UNZIPPED_FILE_NAME_TEMPLATE = 'HealthVerity_{}_1_DeID.txt'
DEID_DAG_NAME = 'validate_fetch_deid_file'
MINIMUM_DEID_FILE_SIZE = 500

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2017, 05, 11, 12),
    'depends_on_past': True,
    'retries': 3,
    'retry_delay': timedelta(minutes=2)
}

mdag = DAG(
    dag_id=DAG_NAME,
    schedule_interval="0 13 * * *",
    default_args=default_args
)

env = aws_utils.get_aws_env()

row_count = 0


def get_formatted_date(kwargs):
    return kwargs['yesterday_ds_nodash'] + kwargs['ds_nodash'][4:8]


def insert_current_date(template, kwargs):
    return template.format(
        kwargs['ds_nodash'][0:4],
        kwargs['ds_nodash'][4:6],
        kwargs['ds_nodash'][6:8]
    )

#
# Pre-Matching
#
def fetch_step(task_id, s3_path_template, local_path_template):
    def execute(ds, **kwargs):
        aws_utils.fetch_file_from_s3(
            s3_path_template.format(get_formatted_date(kwargs)),
            local_path_template.format(get_formatted_date(kwargs))
        )
    return PythonOperator(
        task_id='fetch_' + task_id,
        provide_context=True,
        python_callable=execute,
        dag=mdag
    )
fetch_addon = fetch_step(
    "addon",
    "{}{}".format(
        S3_TRANSACTION_RAW_PATH,
        TRANSACTION_ADDON_FILE_NAME_TEMPLATE
    ),
    TRANSACTION_ADDON_TMP_PATH_TEMPLATE
)
fetch_trunk = fetch_step(
    "trunk",
    '{}{}'.format(
        S3_TRANSACTION_RAW_PATH,
        TRANSACTION_TRUNK_FILE_NAME_TEMPLATE
    ),
    TRANSACTION_TRUNK_TMP_PATH_TEMPLATE
)


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
unzip_addon = unzip_step(
    "addon", TRANSACTION_ADDON_TMP_PATH_TEMPLATE
)
unzip_trunk = unzip_step(
    "trunk", TRANSACTION_TRUNK_TMP_PATH_TEMPLATE
)


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
decrypt_addon = decrypt_step("addon", TRANSACTION_ADDON_TMP_PATH_TEMPLATE)


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
gunzip_addon = gunzip_step(
    "addon", TRANSACTION_ADDON_TMP_PATH_TEMPLATE
)
gunzip_trunk = gunzip_step(
    "trunk", TRANSACTION_TRUNK_TMP_PATH_TEMPLATE
)


def set_row_count_step():
    def execute(ds, **kwargs):
        global row_count
        row_count = check_output([
            'wc', '-l',
            TRANSACTION_TRUNK_TMP_PATH_TEMPLATE.format(
                get_formatted_date(kwargs)
            ) + TRANSACTION_TRUNK_UNZIPPED_FILE_NAME_TEMPLATE.format(
                get_formatted_date(kwargs)
            )
        ])
    return PythonOperator(
        task_id='set_row_count',
        provide_context=True,
        python_callable=execute,
        dag=mdag
    )
set_row_count = set_row_count_step()


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
bzip_parts_addon = bzip_parts_step(
    "addon", TRANSACTION_ADDON_TMP_PATH_PARTS_TEMPLATE
)
bzip_parts_trunk = bzip_parts_step(
    "trunk", TRANSACTION_TRUNK_TMP_PATH_PARTS_TEMPLATE
)


def push_splits_to_s3_step(task_id, tmp_parts_path, s3_path):
    def execute(ds, **kwargs):
        formatted_date = get_formatted_date(kwargs)
        dest_date = kwargs['ds_nodash']
        aws_utils.push_local_dir_to_s3(
            tmp_parts_path.format(formatted_date),
            s3_path.format(
                dest_date[0:4],
                dest_date[4:6],
                dest_date[6:8]
            )
        )
    return PythonOperator(
        task_id='push_splits_to_s3_' + task_id,
        provide_context=True,
        python_callable=execute,
        dag=mdag
    )
push_splits_to_s3_addon = push_splits_to_s3_step(
    "addon",
    TRANSACTION_ADDON_TMP_PATH_PARTS_TEMPLATE,
    TRANSACTION_ADDON_S3_SPLIT_PATH
)
push_splits_to_s3_trunk = push_splits_to_s3_step(
    "trunk",
    TRANSACTION_TRUNK_TMP_PATH_PARTS_TEMPLATE,
    TRANSACTION_TRUNK_S3_SPLIT_PATH
)


def clean_up_workspace_step(task_id, template):
    def execute(ds, **kwargs):
        check_call([
            'rm', '-rf', template.format(get_formatted_date(kwargs))
        ])
    return PythonOperator(
        task_id='clean_up_workspace_' + task_id,
        provide_context=True,
        python_callable=execute,
        trigger_rule='all_done',
        dag=mdag
    )
clean_up_workspace_addon = clean_up_workspace_step(
    "addon", TRANSACTION_ADDON_TMP_PATH_TEMPLATE
)
clean_up_workspace_trunk = clean_up_workspace_step(
    "trunk", TRANSACTION_TRUNK_TMP_PATH_TEMPLATE
)
clean_up_workspace_addon_parts = clean_up_workspace_step(
    "addon_parts", TRANSACTION_ADDON_TMP_PATH_PARTS_TEMPLATE
)
clean_up_workspace_trunk_parts = clean_up_workspace_step(
    "trunk_parts", TRANSACTION_TRUNK_TMP_PATH_PARTS_TEMPLATE
)
clean_up_workspace = clean_up_workspace_step("all", TMP_PATH_TEMPLATE)


def queue_up_for_matching_step(seq_num, engine_env, priority):
    def execute(ds, **kwargs):
        deid_file = '{}{}'.format(
            S3_TRANSACTION_RAW_PATH, DEID_FILE_NAME_TEMPLATE.format(
                get_formatted_date(kwargs)
            ))
        check_call([
            '/home/airflow/airflow/dags/resources/push_file_to_s3.sh',
            deid_file, seq_num, engine_env, priority
        ], env=aws_utils.get_aws_env("_MATCH_PUSHER"))
    return PythonOperator(
        task_id='queue_up_for_matching',
        provide_context=True,
        python_callable=execute,
        dag=mdag
    )
queue_up_for_matching = queue_up_for_matching_step(
    '0', 'prod-matching-engine', 'priority3'
)


#
# Post-Matching
#
S3_PAYLOAD_LOCATION_BUCKET = 'salusv'
S3_PAYLOAD_LOCATION_KEY = 'matching/prod/payload/1b3f553d-7db8-43f3-8bb0-6e0b327320d9/'
S3_PAYLOAD_LOCATION = 's3://' + S3_PAYLOAD_LOCATION_BUCKET + '/' + S3_PAYLOAD_LOCATION_KEY
S3_PAYLOAD_DEST = 's3://salusv/matching/payload/labtests/quest/'


def detect_matching_done_step():
    def execute(ds, **kwargs):
        global row_count
        chunk_start = row_count / 1000000 * 1000000
        template = '{}{}'
        if row_count >= 1000000:
            template += '{}-{}'
        template += '*'
        s3_key = template.format(
            S3_PAYLOAD_LOCATION,
            DEID_UNZIPPED_FILE_NAME_TEMPLATE.format(
                get_formatted_date(kwargs)
            ), chunk_start, row_count
        )
        logging.info('Poking for key : {}'.format(s3_key))
        while not aws_utils.s3_key_exists(s3_key):
            print("Looking for: " + s3_key)
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
        dest_date = '{}/{}/{}'.format(
            kwargs['ds_nodash'][0:4],
            kwargs['ds_nodash'][4:6],
            kwargs['ds_nodash'][6:8]
        )
        for payload_file in aws_utils.list_s3_bucket(
                S3_PAYLOAD_LOCATION +
                DEID_UNZIPPED_FILE_NAME_TEMPLATE.format(
                    get_formatted_date(kwargs)
                )
        ):
            check_call([
                'aws', 's3', 'cp',
                's3://' + S3_PAYLOAD_LOCATION_BUCKET + '/' + payload_file,
                S3_PAYLOAD_DEST + dest_date + '/' + payload_file.split('/')[-1]
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
RS_CLUSTER_ID_TEMPLATE = 'quest-norm-{}'
RS_NUM_NODES = '5'


def create_redshift_cluster_step():
    def execute(ds, **kwargs):
        aws_utils.create_redshift_cluster(
            RS_CLUSTER_ID_TEMPLATE.format(get_formatted_date(kwargs)),
            RS_NUM_NODES
        )
    return PythonOperator(
        task_id='create-redshift-cluster',
        provide_context=True,
        python_callable=execute,
        dag=mdag
    )
create_redshift_cluster = create_redshift_cluster_step()


def normalize_step():
    def execute(ds, **kwargs):
        path = insert_current_date(
            S3_TRANSACTION_PROCESSED_PATH_TEMPLATE, kwargs
        )
        curdate = insert_current_date(
            '{}/{}/{}', kwargs
        )
        setid = aws_utils.list_s3_bucket(path)[0]  \
                         .split('/')[-1]           \
                         .replace('.bz2', '')[0:-3]
        command = [
            '/home/airflow/airflow/dags/providers/quest/rsNormalizeQuest.py',
            '--date', curdate, '--setid', setid,
            '--s3_credentials', aws_utils.get_rs_s3_credentials_str()
        ]
        cwd = '/home/airflow/airflow/dags/providers/quest/'
        aws_utils.run_rs_query_file(
            # RS_CLUSTER_ID_TEMPLATE.format(get_formatted_date(kwargs)),
            RS_CLUSTER_ID_TEMPLATE.format('201701110112'),
            command, cwd
        )
    return PythonOperator(
        task_id='normalize',
        provide_context=True,
        python_callable=execute,
        dag=mdag
    )
normalize = normalize_step()


def delete_redshift_cluster_step():
    def execute(ds, **kwargs):
        check_call([
            '/home/airflow/airflow/dags/resources/redshift.py', 'delete',
            '--identifier',
            RS_CLUSTER_ID_TEMPLATE.format(get_formatted_date(kwargs))
        ], env=env)
    return PythonOperator(
        task_id='delete-redshift-cluster',
        provide_context=True,
        python_callable=execute,
        dag=mdag
    )
delete_redshift_cluster = delete_redshift_cluster_step()

#
# Parquet
#
EMR_CLUSTER_ID_TEMPLATE = 'quest-parquet-{}'
EMR_NUM_NODES = 5
EMR_NODE_TYPE = 'c4.xlarge'
EMR_EBS_VOLUME_SIZE = 0


def create_emr_cluster_step():
    def execute(ds, **kwargs):
        aws_utils.create_emr_cluster(
            EMR_CLUSTER_ID_TEMPLATE.format(get_formatted_date(kwargs)),
            EMR_NUM_NODES, EMR_NODE_TYPE, 0
        )
    return PythonOperator(
        task_id='create-emr-cluster',
        provide_context=True,
        python_callable=execute,
        dag=mdag
    )
# create_emr_cluster = create_emr_cluster_step()

# addon
unzip_addon.set_upstream(fetch_addon)
decrypt_addon.set_upstream(unzip_addon)
gunzip_addon.set_upstream(decrypt_addon)
split_addon.set_upstream(gunzip_addon)
clean_up_workspace_addon.set_upstream(split_addon)
bzip_parts_addon.set_upstream(clean_up_workspace_addon)
push_splits_to_s3_addon.set_upstream(bzip_parts_addon)
clean_up_workspace_addon_parts.set_upstream(push_splits_to_s3_addon)

# trunk
unzip_trunk.set_upstream(fetch_trunk)
gunzip_trunk.set_upstream(unzip_trunk)
set_row_count.set_upstream(gunzip_trunk)
split_trunk.set_upstream(set_row_count)
clean_up_workspace_trunk.set_upstream(split_trunk)
bzip_parts_trunk.set_upstream(clean_up_workspace_trunk)
push_splits_to_s3_trunk.set_upstream(bzip_parts_trunk)
clean_up_workspace_trunk_parts.set_upstream(push_splits_to_s3_trunk)

# queue and cleanup
clean_up_workspace.set_upstream(
    [clean_up_workspace_trunk_parts, clean_up_workspace_addon_parts]
)
queue_up_for_matching.set_upstream(clean_up_workspace)


# post-matching
detect_matching_done.set_upstream(queue_up_for_matching)
move_matching_payload.set_upstream(detect_matching_done)

# normalization
create_redshift_cluster.set_upstream(move_matching_payload)
normalize.set_upstream(create_redshift_cluster)
delete_redshift_cluster.set_upstream(normalize)

# parquet
