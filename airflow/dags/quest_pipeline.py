from airflow import DAG
from airflow.models import Variable
from airflow.operators import PythonOperator, SubDagOperator
from datetime import datetime, timedelta
from subprocess import check_call
import time

# hv-specific modules
import subdags.s3_validate_file as s3_validate_file
import subdags.s3_fetch_file as s3_fetch_file
import subdags.decrypt_file as decrypt_file
import subdags.split_push_file as split_push_file
import subdags.queue_up_for_matching as queue_up_for_matching

import util.s3_utils as s3_utils
import util.emr_utils as emr_utils
import util.redshift_utils as redshift_utils

reload(s3_validate_file)
reload(s3_fetch_file)
reload(decrypt_file)
reload(split_push_file)
reload(queue_up_for_matching)
reload(s3_utils)
reload(emr_utils)
reload(redshift_utils)

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
TRANSACTION_ADDON_UNZIPPED_FILE_NAME_TEMPLATE = 'HealthVerity_{}_1_PlainTxt.txt'
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
    'start_date': datetime(2017, 1, 25, 12),
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


def get_formatted_date(ds, kwargs):
    return kwargs['yesterday_ds_nodash'] + kwargs['ds_nodash'][4:8]


def insert_formatted_date_function(template):
    def out(ds, kwargs):
        return template.format(get_formatted_date(ds, kwargs))
    return out


def insert_todays_date_function(template):
    def out(ds, kwargs):
        return template.format(kwargs['ds_nodash'])
    return out


def insert_formatted_regex_function(template):
    def out(ds, kwargs):
        return template.format('\d{12}')
    return out


def insert_current_date_function(template):
    def out(ds, kwargs):
        return template.format(
            kwargs['yesterday_ds_nodash'][0:4],
            kwargs['yesterday_ds_nodash'][4:6],
            kwargs['yesterday_ds_nodash'][6:8]
        )
    return out


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
        subdag=s3_validate_file.s3_validate_file(
            DAG_NAME,
            'validate_' + task_id + '_file',
            default_args['start_date'],
            mdag.schedule_interval,
            {
                'expected_file_name_func': insert_formatted_date_function(
                    path_template
                ),
                'file_name_pattern_func': insert_formatted_regex_function(
                    path_template
                ),
                'minimum_file_size': minimum_file_size,
                's3_prefix': '/'.join(S3_TRANSACTION_RAW_PATH.split('/')[3:]),
                'file_description': 'Quest ' + task_id + 'file'
            }
        ),
        task_id='validate_' + task_id + '_file',
        dag=mdag
    )


validate_addon = generate_transaction_file_validation_dag(
    'addon', TRANSACTION_ADDON_FILE_NAME_TEMPLATE,
    1000000
)
validate_trunk = generate_transaction_file_validation_dag(
    'trunk', TRANSACTION_TRUNK_FILE_NAME_TEMPLATE,
    10000000
)
validate_deid = generate_transaction_file_validation_dag(
    'deid', DEID_FILE_NAME_TEMPLATE,
    10000000
)


def generate_fetch_dag(
        task_id, s3_path_template, local_path_template, file_name_template
):
    return SubDagOperator(
        subdag=s3_fetch_file.s3_fetch_file(
            DAG_NAME,
            'fetch_' + task_id + '_file',
            default_args['start_date'],
            mdag.schedule_interval,
            {
                'tmp_path_template': local_path_template,
                'expected_file_name_func': insert_formatted_date_function(
                    file_name_template
                ),
                's3_prefix': s3_path_template
            }
        ),
        task_id='fetch_' + task_id + '_file',
        dag=mdag
    )


fetch_addon = generate_fetch_dag(
    "addon",
    '/'.join(S3_TRANSACTION_RAW_PATH.split('/')[3:]),
    TRANSACTION_ADDON_TMP_PATH_TEMPLATE,
    TRANSACTION_ADDON_FILE_NAME_TEMPLATE
)
fetch_trunk = generate_fetch_dag(
    "trunk",
    '/'.join(S3_TRANSACTION_RAW_PATH.split('/')[3:]),
    TRANSACTION_TRUNK_TMP_PATH_TEMPLATE,
    TRANSACTION_TRUNK_FILE_NAME_TEMPLATE
)


def unzip_step(task_id, tmp_path_template, filename_template):
    def execute(ds, **kwargs):
        check_call([
            'unzip', '-o',
            insert_todays_date_function(tmp_path_template)(ds, kwargs)
            + filename_template.format(get_formatted_date(ds, kwargs)),
            '-d', insert_todays_date_function(tmp_path_template)(ds, kwargs)
        ])
    return PythonOperator(
        task_id='unzip_' + task_id + '_file',
        provide_context=True,
        python_callable=execute,
        dag=mdag
    )


unzip_addon = unzip_step(
    "addon", TRANSACTION_ADDON_TMP_PATH_TEMPLATE,
    TRANSACTION_ADDON_FILE_NAME_TEMPLATE
)
unzip_trunk = unzip_step(
    "trunk", TRANSACTION_TRUNK_TMP_PATH_TEMPLATE,
    TRANSACTION_TRUNK_FILE_NAME_TEMPLATE
)


decrypt_addon = SubDagOperator(
    subdag=decrypt_file.decrypt_file(
        DAG_NAME,
        'decrypt_addon_file',
        default_args['start_date'],
        mdag.schedule_interval,
        {
            'tmp_path_template': TRANSACTION_ADDON_TMP_PATH_TEMPLATE,
            'encrypted_file_name_func': insert_formatted_date_function(
                TRANSACTION_ADDON_UNZIPPED_FILE_NAME_TEMPLATE
            ),
            'decrypted_file_name_func': insert_formatted_date_function(
                TRANSACTION_ADDON_UNZIPPED_FILE_NAME_TEMPLATE + '.gz'
            )
        }
    ),
    task_id='decrypt_addon_file',
    dag=mdag
)


def gunzip_step(task_id, tmp_path_template, tmp_file_template):
    def execute(ds, **kwargs):
        check_call([
            'gzip', '-df', tmp_path_template.format(kwargs['ds_nodash'])
            + tmp_file_template.format(get_formatted_date(ds, kwargs))
        ])

    return PythonOperator(
        task_id='gunzip_' + task_id + '_file',
        provide_context=True,
        python_callable=execute,
        dag=mdag
    )


gunzip_trunk = gunzip_step(
    "trunk", TRANSACTION_TRUNK_TMP_PATH_TEMPLATE,
    TRANSACTION_TRUNK_UNZIPPED_FILE_NAME_TEMPLATE
)


def split_step(task_id, tmp_path_template, tmp_name_template, s3_destination, num_splits):
    return SubDagOperator(
        subdag=split_push_file.split_push_file(
            DAG_NAME,
            'split_' + task_id + '_file',
            default_args['start_date'],
            mdag.schedule_interval,
            {
                'tmp_path_template': tmp_path_template,
                'source_file_name_func': insert_formatted_date_function(
                    tmp_name_template
                ),
                's3_dest_path_func': insert_current_date_function(
                    s3_destination
                ),
                'num_splits': num_splits
            }
        ),
        task_id='split_' + task_id + '_file',
        dag=mdag
    )


split_addon = split_step(
    "addon", TRANSACTION_ADDON_TMP_PATH_TEMPLATE,
    TRANSACTION_ADDON_UNZIPPED_FILE_NAME_TEMPLATE,
    TRANSACTION_ADDON_S3_SPLIT_PATH, 20
)
split_trunk = split_step(
    "trunk", TRANSACTION_TRUNK_TMP_PATH_TEMPLATE,
    TRANSACTION_TRUNK_UNZIPPED_FILE_NAME_TEMPLATE,
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
    subdag=queue_up_for_matching.queue_up_for_matching(
        DAG_NAME,
        'queue_up_for_matching',
        default_args['start_date'],
        mdag.schedule_interval,
        {
            'expected_file_name_func': insert_formatted_date_function(
                DEID_FILE_NAME_TEMPLATE
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
                lambda k: get_formatted_date(ds, kwargs) in k and 'DONE' in k,
                s3_utils.list_s3_bucket(S3_PAYLOAD_LOCATION)
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
                    get_formatted_date(ds, kwargs)
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
# Normalization/Parquet
#
EMR_CLUSTER_ID_TEMPLATE = 'quest-norm-{}'
EMR_NUM_NODES = "5"
EMR_NODE_TYPE = 'c4.xlarge'
EMR_EBS_VOLUME_SIZE = 0


def create_emr_cluster_step():
    def execute(ds, **kwargs):
        emr_utils.create_emr_cluster(
            EMR_CLUSTER_ID_TEMPLATE.format(get_formatted_date(ds, kwargs)),
            EMR_NUM_NODES, EMR_NODE_TYPE, EMR_EBS_VOLUME_SIZE
        )
    return PythonOperator(
        task_id='create_emr_cluster',
        provide_context=True,
        python_callable=execute,
        dag=mdag
    )


create_emr_cluster = create_emr_cluster_step()

TEXT_WAREHOUSE = "s3a://salusv/warehouse/text/labtests/2017-02-16/part_provider=quest/"
PARQUET_WAREHOUSE = "s3://salusv/warehouse/parquet/labtests/2017-02-16/part_provider=quest/"


def normalize_step():
    def execute(ds, **kwargs):
        emr_utils.normalize(
            EMR_CLUSTER_ID_TEMPLATE.format(get_formatted_date(ds, kwargs)),
            '/home/hadoop/spark/providers/quest/sparkNormalizeQuest.py',
            ['--date', insert_current_date('{}-{}-{}', kwargs)],
            TEXT_WAREHOUSE, PARQUET_WAREHOUSE, 'lab'
        )

    return PythonOperator(
        task_id='normalize',
        provide_context=True,
        python_callable=execute,
        dag=mdag
    )


normalize = normalize_step()


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
create_emr_cluster.set_upstream(detect_matching_done)
normalize.set_upstream(
    [
        create_emr_cluster, move_matching_payload,
        split_addon, split_trunk
    ]
)

# shutdown
delete_emr_cluster.set_upstream(normalize)
