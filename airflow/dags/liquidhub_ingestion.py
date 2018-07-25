from airflow.models import Variable
from airflow.operators import PythonOperator, SubDagOperator, DummyOperator
from datetime import datetime, timedelta
import os
import re

# hv-specific modules
import common.HVDAG as HVDAG
import subdags.s3_fetch_file as s3_fetch_file
import subdags.decrypt_files as decrypt_files
import subdags.queue_up_for_matching as queue_up_for_matching
import subdags.clean_up_tmp_dir as clean_up_tmp_dir

import util.decompression as decompression
import util.compression as compression
import util.date_utils as date_utils
import util.s3_utils as s3_utils

for m in [s3_fetch_file, decrypt_files, queue_up_for_matching,
          clean_up_tmp_dir, decompression, HVDAG, date_utils]:
    reload(m)

# Applies to all files
TMP_PATH_TEMPLATE = '/tmp/liquidhub/custom/{}{}{}/'
DAG_NAME = 'liquidhub_ingestion_pipeline'

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2018, 7, 25),
    'depends_on_past': True,
    'retries': 3,
    'retry_delay': timedelta(minutes=2)
}

mdag = HVDAG.HVDAG(
    dag_id=DAG_NAME,
    schedule_interval='5,20,35,50 * * * *',
    default_args=default_args
)

# Applies to all transaction files
if HVDAG.HVDAG.airflow_env == 'test':
    S3_TRANSACTION_RAW_URL = 's3://salusv/testing/dewey/airflow/e2e/lhv2/custom/raw/'
    S3_TRANSACTION_PROCESSED_URL_TEMPLATE = 's3://salusv/testing/dewey/airflow/e2e/lhv2/custom/out/{}/{{}}/{{}}/{{}}/'
    S3_PAYLOAD_DEST = 's3://salusv/testing/dewey/airflow/e2e/lhv2/custom/payload/'
    S3_NORMALIZED_FILE_URL_TEMPLATE = 's3://salusv/testing/dewey/airflow/e2e/lhv2/custom/spark-out/{}/{{}}/{{}}/{{}}/'
else:
    S3_INCOMING_LOCATION = 's3://healthverity/incoming/lh_amgen_hv/'
    S3_TRANSACTION_PROCESSED_URL_TEMPLATE = 's3://salusv/incoming/custom/lhv2/{}/'

# File naming patterns
FILE_V1T1_TEMPLATE = 'LHV1_{}_PatDemo_{}_v{}'
FILE_V1T2_TEMPLATE = 'LHV1_{}_{}_{}_v{}'
FILE_V2T1_TEMPLATE = 'LHV2_{}_PatDemo_{}_v{}'
FILE_V2T2_TEMPLATE = 'LHV2_{}_{}_{}_v{}'

# Transaction files
TRANSACTION_EXTENSION = '_plainoutput.txt'
TRANSACTION_TMP_PATH_TEMPLATE = TMP_PATH_TEMPLATE + 'raw/'

# Deid files
DEID_EXTENSION = '_output.txt'

get_tmp_dir = date_utils.generate_insert_date_into_template_function(
    TRANSACTION_TMP_PATH_TEMPLATE
)

# Determine groups (deid + transaction file pairs) that are ready for processing
def do_get_groups_ready(**kwargs):
    received_groups = []
    received_files  = set(s3_utils.list_s3_bucket_files(S3_INCOMING_LOCATION))
    for f in received_files:
        if re.match(FILE_V1T1_TEMPLATE.format('.*', '\d{8}', '\d+') + DEID_EXTENSION, f) or             \
                re.match(FILE_V1T2_TEMPLATE.format('.*', '.*', '\d{8}', '\d+') + DEID_EXTENSION, f) or  \
                re.match(FILE_V2T1_TEMPLATE.format('.*', '\d{8}', '\d+') + DEID_EXTENSION, f) or        \
                re.match(FILE_V2T2_TEMPLATE.format('.*', '.*', '\d{8}', '\d+') + DEID_EXTENSION, f):

            group = f.replace(DEID_EXTENSION, '').replace(TRANSACTION_EXTENSION, '')
            received_groups.append(group)

    processed_groups = []
    for f in s3_utils.list_s3_bucket_files(S3_TRANSACTION_PROCESSED_URL_TEMPLATE.format('')[:-1]):
        group = f.split('/')[-1].split(DEID_EXTENSION)[0].split(TRANSACTION_EXTENSION)[0]
        processed_groups.append(group)

    # processed_files are in the format <gid>/<filename>
    new_groups = set(received_groups).difference(set(processed_groups))
    groups_ready = set()
    for g in new_groups:
        if (g + DEID_EXTENSION) in received_files and (g + TRANSACTION_EXTENSION) in received_files:
            groups_ready.add(g)

    kwargs['ti'].xcom_push(key = 'groups_ready', value = groups_ready)

get_groups_ready = PythonOperator(
    task_id = 'get_groups_ready',
    python_callable = do_get_groups_ready,
    provide_context = True,
    dag = mdag
)

def get_transaction_files_regex(ds, kwargs):
    groups = get_transaction_file_names(ds, kwargs)
    return '(' + '|'.join(groups) + ')'

def get_transaction_file_names(ds, kwargs):
    groups_ready = kwargs['ti'].xcom_pull(dag_id = DAG_NAME, task_ids = 'get_groups_ready', key = 'groups_ready')
    return [g + TRANSACTION_EXTENSION for g in groups_ready]

def get_transaction_file_paths(ds, kwargs):
    return [get_tmp_dir(ds, kwargs) + f for f in get_transaction_file_names(ds, kwargs)]

def get_encrypted_decrypted_file_paths(ds, kwargs):
    encrypted_file_paths = get_transaction_file_paths(ds, kwargs)
    return [
        [encrypted_file_path, encrypted_file_path + '.gz']
        for encrypted_file_path in encrypted_file_paths
    ]

def get_deid_file_urls(ds, kwargs):
    groups_ready = kwargs['ti'].xcom_pull(dag_id = DAG_NAME, task_ids = 'get_groups_ready', key = 'groups_ready')
    return [S3_INCOMING_LOCATION + g + DEID_EXTENSION for g in groups_ready]

fetch_transaction = SubDagOperator(
    subdag=s3_fetch_file.s3_fetch_file(
        DAG_NAME,
        'fetch_transaction_files',
        default_args['start_date'],
        mdag.schedule_interval,
        {
            'tmp_path_template'      : TRANSACTION_TMP_PATH_TEMPLATE,
            'expected_file_name_func': get_transaction_files_regex,
            's3_prefix'              : '/'.join(S3_INCOMING_LOCATION.split('/')[3:]),
            's3_bucket'              : 'salusv' if HVDAG.HVDAG.airflow_env == 'test' else 'healthverity',
            'regex_name_match'       : True,
            'multi_match'            : True
        }
    ),
    task_id='fetch_transaction_files',
    dag=mdag
)

decrypt_transaction = SubDagOperator(
    subdag=decrypt_files.decrypt_files(
        DAG_NAME,
        'decrypt_transaction_files',
        default_args['start_date'],
        mdag.schedule_interval,
        {
            'tmp_dir_func'                        : get_tmp_dir,
            'encrypted_decrypted_file_paths_func' : get_encrypted_decrypted_file_paths
        }
    ),
    task_id='decrypt_transaction_files',
    dag=mdag
)

def do_compress_push(ds, **kwargs):
    groups_ready = kwargs['ti'].xcom_pull(dag_id = DAG_NAME, task_ids = 'get_groups_ready', key = 'groups_ready')
    for g in groups_ready:
        f = get_tmp_dir(ds, kwargs) + g + TRANSACTION_EXTENSION
        compression.compress_bzip2_file(f)
        s3_utils.copy_file(f + '.bz2', S3_TRANSACTION_PROCESSED_URL_TEMPLATE.format(g))

compress_push = PythonOperator(
    provide_context=True,
    task_id='compress_push',
    python_callable=do_compress_push,
    dag=mdag
)

clean_up_workspace = SubDagOperator(
    subdag=clean_up_tmp_dir.clean_up_tmp_dir(
        DAG_NAME,
        'clean_up_workspace',
        default_args['start_date'],
        mdag.schedule_interval,
        {
            'tmp_path_template': TMP_PATH_TEMPLATE
        }
    ),
    task_id='clean_up_workspace',
    dag=mdag
)

queue_up_for_matching = SubDagOperator(
    subdag=queue_up_for_matching.queue_up_for_matching(
        DAG_NAME,
        'queue_up_for_matching',
        default_args['start_date'],
        mdag.schedule_interval,
        {
            'source_files_func' : get_deid_file_urls,
            'priority'          : 'priority1'
        }
    ),
    task_id='queue_up_for_matching',
    dag=mdag
)

def do_trigger_deliveries(**kwargs):
    groups_ready = kwargs['ti'].xcom_pull(dag_id = DAG_NAME, task_ids = 'get_groups_ready', key = 'groups_ready')

    for gid in groups_ready:
        def do_trigger_delivery_dag(context, dag_run_obj):
            dag_run_obj.run_id += '_' + gid
            dag_run_obj.payload = {
                    "group_id": gid
                }
            return dag_run_obj

        trigger_delivery_dag = TriggerDagRunOperator(
            task_id='trigger_delivery_dag_' + gid,
            trigger_dag_id='liquidhub_delivery_pipeline',
            python_callable=do_trigger_delivery_dag,
            dag=mdag
        )

        trigger_delivery_dag.execute(kwargs)

trigger_deliveries = PythonOperator(
    task_id = 'trigger_deliveries',
    python_callable = do_trigger_deliveries,
    provide_context = True,
    dag = mdag
)

if HVDAG.HVDAG.airflow_env != 'prod':
    for t in ['queue_up_for_matching']:
        del mdag.task_dict[t]
        globals()[t] = DummyOperator(
            task_id = t,
            dag = mdag
        )

fetch_transaction.set_upstream(get_groups_ready)
decrypt_transaction.set_upstream(fetch_transaction)
compress_push.set_upstream(decrypt_transaction)
queue_up_for_matching.set_upstream(get_groups_ready)
trigger_deliveries.set_upstream([queue_up_for_matching, compress_push])
clean_up_workspace.set_upstream(compress_push)
