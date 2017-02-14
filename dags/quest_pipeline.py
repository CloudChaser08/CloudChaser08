from airflow import DAG
from airflow.models import Variable
from airflow.operators import PythonOperator, SubDagOperator
from datetime import datetime, timedelta
import sys
from subprocess import check_call
import time

modules = [
    'util.s3_utils',
    'util.emr_utils',
    'util.redshift_utils'
]

subdags = [
    'subdags.s3_validate_file',
    'subdags.s3_fetch_file',
    'subdags.decrypt_file',
    'subdags.split_push_file',
    'subdags.queue_up_for_matching'
]
for subdag in subdags:
    if sys.modules.get(subdag):
        del sys.modules[subdag]
for module in modules:
    if sys.modules.get(module):
        del sys.modules[module]

from subdags.s3_validate_file import s3_validate_file
from subdags.s3_fetch_file import s3_fetch_file
from subdags.decrypt_file import decrypt_file
from subdags.split_push_file import split_push_file
from subdags.queue_up_for_matching import queue_up_for_matching

import util.s3_utils as s3_utils
import util.emr_utils as emr_utils
import util.redshift_utils as redshift_utils

# Applies to all files
TMP_PATH_TEMPLATE = '/tmp/quest/labtests/{}/'
DAG_NAME = 'quest_pipeline'

# Applies to all transaction files
S3_TRANSACTION_RAW_PATH = 's3://healthverity/incoming/quest/'
S3_TRANSACTION_PROCESSED_PATH_TEMPLATE = 's3://salusv/incoming/labtests/quest/{}/{}/{}/'

# Transaction Addon file
TRANSACTION_ADDON_TMP_PATH_TEMPLATE = TMP_PATH_TEMPLATE + 'raw/addon/'
TRANSACTION_ADDON_S3_SPLIT_PATH = S3_TRANSACTION_PROCESSED_PATH_TEMPLATE + 'addon/'
TRANSACTION_ADDON_FILE_DESCRIPTION = 'Quest transaction addon file'
TRANSACTION_ADDON_FILE_NAME_TEMPLATE = 'HealthVerity_{}_1_PlainTxt.txt.zip'
TRANSACTION_ADDON_DAG_NAME = 'validate_fetch_transaction_addon_file'
MINIMUM_TRANSACTION_FILE_SIZE = 500

# Transaction Trunk file
TRANSACTION_TRUNK_TMP_PATH_TEMPLATE = TMP_PATH_TEMPLATE + 'raw/trunk/'
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
MINIMUM_DEID_FILE_SIZE = 500

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2017, 01, 25, 12),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2)
}

mdag = DAG(
    dag_id=DAG_NAME,
    schedule_interval="0 12 * * *" if Variable.get(
        "AIRFLOW_ENV", default_var=''
    ).find('prod') != -1 else None,
    default_args=default_args
)


def get_formatted_date(kwargs):
    return kwargs['yesterday_ds_nodash'] + kwargs['ds_nodash'][4:8]


def insert_current_date(template, kwargs):
    return template.format(
        kwargs['yesterday_ds_nodash'][0:4],
        kwargs['yesterday_ds_nodash'][4:6],
        kwargs['yesterday_ds_nodash'][6:8]
    )


def generate_transaction_file_validation_dag(
        task_id, path_template, minimum_file_size
):
    return SubDagOperator(
        subdag=s3_validate_file(
            DAG_NAME,
            'validate_' + task_id + '_file',
            default_args['start_date'],
            mdag.schedule_interval,
            {
                'expected_file_name_func': lambda k: path_template.format(
                    get_formatted_date(k)
                ),
                'file_name_pattern_func': lambda k: path_template.format(
                    '\d{10}'
                ),
                'minimum_file_size': minimum_file_size,
                's3_prefix': S3_TRANSACTION_RAW_PATH,
                'file_description': 'Quest ' + task_id + 'file'
            }
        ),
        task_id='validate_' + task_id + '_file',
        dag=mdag
    )
validate_addon = generate_transaction_file_validation_dag(
    'addon', S3_TRANSACTION_RAW_PATH + TRANSACTION_ADDON_FILE_NAME_TEMPLATE,
    1000000
)
validate_trunk = generate_transaction_file_validation_dag(
    'trunk', S3_TRANSACTION_RAW_PATH + TRANSACTION_TRUNK_FILE_NAME_TEMPLATE,
    10000000
)
validate_deid = generate_transaction_file_validation_dag(
    'deid', S3_TRANSACTION_RAW_PATH + DEID_FILE_NAME_TEMPLATE,
    10000000
)


def generate_fetch_dag(
        task_id, s3_path_template, local_path_template, file_name_template
):
    return SubDagOperator(
        subdag=s3_fetch_file(
            DAG_NAME,
            'fetch_' + task_id + '_file',
            default_args['start_date'],
            mdag.schedule_interval,
            {
                'tmp_path_template': local_path_template,
                'expected_file_name_func': lambda k: file_name_template.format(
                    get_formatted_date(k)
                ),
                's3_prefix': s3_path_template
            }
        ),
        task_id='fetch_' + task_id + '_file',
        dag=mdag
    )
fetch_addon = generate_fetch_dag(
    "addon",
    S3_TRANSACTION_RAW_PATH,
    TRANSACTION_ADDON_TMP_PATH_TEMPLATE,
    TRANSACTION_ADDON_FILE_NAME_TEMPLATE
)
fetch_trunk = generate_fetch_dag(
    "trunk",
    S3_TRANSACTION_RAW_PATH,
    TRANSACTION_TRUNK_TMP_PATH_TEMPLATE,
    TRANSACTION_TRUNK_FILE_NAME_TEMPLATE
)


def unzip_step(task_id, tmp_path_template):
    def execute(ds, **kwargs):
        check_call([
            'unzip', tmp_path_template.format(
                kwargs['ds_nodash']
            )
        ])
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


decrypt_addon = SubDagOperator(
    subdag=decrypt_file(
        DAG_NAME,
        'decrypt_addon_file',
        default_args['start_date'],
        mdag.schedule_interval,
        {
            'tmp_path_template': TMP_PATH_TEMPLATE,
            'encrypted_file_name_func': lambda k: TRANSACTION_ADDON_TMP_PATH_TEMPLATE.format(
                get_formatted_date(k)
            ),
            'decrypted_file_name_func': lambda k: TRANSACTION_ADDON_TMP_PATH_TEMPLATE.format(
                get_formatted_date(k)
            ) + '.gz'
        }
    ),
    task_id='decrypt_addon_file',
    dag=mdag
)


def gunzip_step(task_id, tmp_path_template):
    def execute(ds, **kwargs):
        check_call([
            'gzip', '-d', tmp_path_template.format(
                kwargs['ds_nodash']
            )
        ])

    return PythonOperator(
        task_id='gunzip_file_' + task_id,
        provide_context=True,
        python_callable=execute,
        dag=mdag
    )
gunzip_trunk = gunzip_step(
    "trunk", TRANSACTION_TRUNK_TMP_PATH_TEMPLATE
)


def split_step(task_id, tmp_name_template, s3_destination, num_splits):
    return SubDagOperator(
        subdag=split_push_file(
            DAG_NAME,
            'split_' + task_id + '_file',
            default_args['start_date'],
            mdag.schedule_interval,
            {
                'tmp_path_template': TMP_PATH_TEMPLATE,
                'source_file_name_func': lambda k: tmp_name_template.format(
                    get_formatted_date(k)
                ),
                's3_destination_prefix': s3_destination,
                'num_splits': num_splits
            }
        ),
        task_id='split_' + task_id + '_file',
        dag=mdag
    )
split_addon = split_step(
    "addon", TRANSACTION_ADDON_FILE_NAME_TEMPLATE,
    TRANSACTION_ADDON_S3_SPLIT_PATH, 20
)
split_trunk = split_step(
    "trunk", TRANSACTION_TRUNK_FILE_NAME_TEMPLATE,
    TRANSACTION_TRUNK_S3_SPLIT_PATH, 20
)


def clean_up_workspace_step(task_id, template):
    def execute(ds, **kwargs):
        check_call([
            'rm', '-rf', template.format(kwargs['ds_nodash'])
        ])
    return PythonOperator(
        task_id='clean_up_workspace_' + task_id,
        provide_context=True,
        python_callable=execute,
        trigger_rule='all_done',
        dag=mdag
    )
clean_up_workspace = clean_up_workspace_step("all", TMP_PATH_TEMPLATE)

queue_up_for_matching = SubDagOperator(
    subdag=queue_up_for_matching(
        DAG_NAME,
        'queue_up_for_matching',
        default_args['start_date'],
        mdag.schedule_interval,
        {
            'expected_file_name_func': lambda k: DEID_FILE_NAME_TEMPLATE.format(
                get_formatted_date(k)
            ),
            's3_prefix': S3_TRANSACTION_RAW_PATH
        }
    ),
    task_id='queue_up_for_matching',
    dag=mdag
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
        while not filter(
                lambda k: get_formatted_date(kwargs) in k and 'DONE' in k,
                aws_utils.list_s3_bucket(S3_PAYLOAD_LOCATION)
        ):
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
        for payload_file in s3_utils.list_s3_bucket(
                S3_PAYLOAD_LOCATION +
                DEID_UNZIPPED_FILE_NAME_TEMPLATE.format(
                    get_formatted_date(kwargs)
                )
        ):
            s3_utils.copy_file(
                payload_file,
                insert_current_date(
                    S3_PAYLOAD_DEST + '{}/{}/{}/' + payload_file.split('/')[-1],
                    kwargs
                )
            )
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
        redshift_utils.create_redshift_cluster(
            RS_CLUSTER_ID_TEMPLATE.format(get_formatted_date(kwargs)),
            RS_NUM_NODES
        )
    return PythonOperator(
        task_id='create_redshift_cluster',
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
        period = 'hist' if curdate < '2016/09/01' else 'current'
        setid = s3_utils.list_s3_bucket(path)[0]  \
                        .split('/')[-1]          \
                        .replace('.bz2', '')[0:-3]
        command = [
            '/home/airflow/airflow/dags/providers/quest/norm/rsNormalizeQuest.py',
            '--date', curdate, '--setid', setid, '--period', period,
            '--s3_credentials', redshift_utils.get_rs_s3_credentials_str()
        ]
        cwd = '/home/airflow/airflow/dags/providers/quest/'
        redshift_utils.run_rs_query_file(
            RS_CLUSTER_ID_TEMPLATE.format(get_formatted_date(kwargs)),
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
        redshift_utils.delete_redshift_cluster(
            RS_CLUSTER_ID_TEMPLATE.format(get_formatted_date(kwargs))
        )


    return PythonOperator(
        task_id='delete_redshift_cluster',
        provide_context=True,
        python_callable=execute,
        dag=mdag
    )
delete_redshift_cluster = delete_redshift_cluster_step()

#
# Parquet
#
EMR_CLUSTER_ID_TEMPLATE = 'quest-parquet-{}'
EMR_NUM_NODES = "5"
EMR_NODE_TYPE = 'c4.xlarge'
EMR_EBS_VOLUME_SIZE = 0
PARQUET_SOURCE_TEMPLATE = "s3a://salusv/warehouse/text/labtests/quest/{}/{}/{}/"
PARQUET_DESTINATION_TEMPLATE = "s3://salusv/warehouse/parquet/labtests/quest/{}/{}/{}/"


def create_emr_cluster_step():
    def execute(ds, **kwargs):
        emr_utils.create_emr_cluster(
            EMR_CLUSTER_ID_TEMPLATE.format(get_formatted_date(kwargs)),
            EMR_NUM_NODES, EMR_NODE_TYPE, EMR_EBS_VOLUME_SIZE
        )
    return PythonOperator(
        task_id='create_emr_cluster',
        provide_context=True,
        python_callable=execute,
        dag=mdag
    )
create_emr_cluster = create_emr_cluster_step()


def transform_to_parquet_step():
    def execute(ds, **kwargs):
        emr_utils.transform_to_parquet(
            EMR_CLUSTER_ID_TEMPLATE.format(get_formatted_date(kwargs)),
            insert_current_date(PARQUET_SOURCE_TEMPLATE, kwargs),
            insert_current_date(PARQUET_DESTINATION_TEMPLATE, kwargs),
            "lab"
        )
    return PythonOperator(
        task_id='parquet',
        provide_context=True,
        python_callable=execute,
        dag=mdag
    )
transform_to_parquet = transform_to_parquet_step()


def delete_emr_cluster_step():
    def execute(ds, **kwargs):
        emr_utils.delete_emr_cluster(
            EMR_CLUSTER_ID_TEMPLATE.format(get_formatted_date(kwargs))
        )
    return PythonOperator(
        task_id='delete_emr_cluster',
        provide_context=True,
        python_callable=execute,
        dag=mdag
    )
delete_emr_cluster = delete_emr_cluster_step()

# addon
fetch_addon.set_upstream(validate_addon)
unzip_addon.set_upstream(fetch_addon)
decrypt_addon.set_upstream(unzip_addon)
split_addon.set_upstream(decrypt_addon)

# trunk
fetch_trunk.set_upstream(validate_trunk)
unzip_trunk.set_upstream(fetch_trunk)
gunzip_trunk.set_upstream(unzip_trunk)
split_trunk.set_upstream(gunzip_trunk)

# cleanup
clean_up_workspace.set_upstream(
    [split_trunk, split_addon]
)

# matching
queue_up_for_matching.set_upstream(validate_deid)

# post-matching
detect_matching_done.set_upstream(queue_up_for_matching)
move_matching_payload.set_upstream(detect_matching_done)

# normalization
create_redshift_cluster.set_upstream(detect_matching_done)
normalize.set_upstream(
    [
        create_redshift_cluster, move_matching_payload,
        split_addon, split_trunk
    ]
)
delete_redshift_cluster.set_upstream(normalize)

# parquet
create_emr_cluster.set_upstream(normalize)
transform_to_parquet.set_upstream(create_emr_cluster)
delete_emr_cluster.set_upstream(transform_to_parquet)
