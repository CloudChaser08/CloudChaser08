from airflow.operators import *
from datetime import datetime, timedelta

# hv-specific modules
import common.HVDAG as HVDAG
import subdags.decrypt_files as decrypt_files
import subdags.queue_up_for_matching as queue_up_for_matching

import util.s3_utils as s3_utils

for m in [decrypt_files, queue_up_for_matching, HVDAG, s3_utils]:
    reload(m)

# Applies to all files
TMP_PATH_TEMPLATE = '/tmp/humana/hv000468/{}/'
DAG_NAME = 'humana_hv000468_ingestion'

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2018, 4, 26, 12),
    'depends_on_past': True,
    'retries': 3,
    'retry_delay': timedelta(minutes=2)
}

mdag = HVDAG.HVDAG(
    dag_id=DAG_NAME,
    schedule_interval="*/15 * * * *",
    default_args=default_args
)

# Applies to all transaction files
if HVDAG.HVDAG.airflow_env == 'test':
    S3_TRANSACTION_RAW_URL = 's3://salusv/testing/dewey/airflow/e2e/humana/hv000468/raw/'
    S3_TRANSACTION_PROCESSED_URL_TEMPLATE = 's3://salusv/testing/dewey/airflow/e2e/humana/hv000468/out/{}/'
    S3_PAYLOAD_DEST = 's3://salusv/testing/dewey/airflow/e2e/humana/hv000468/payload/'
else:
    S3_TRANSACTION_RAW_URL = 's3://healthverity/incoming/humana/'
    S3_TRANSACTION_PROCESSED_URL_TEMPLATE = 's3://salusv/data_requests/humana/hv000468/{}/'
    S3_PAYLOAD_DEST = 's3://salusv/matching/payload/custom/humana/hv000468/{}/'

# Transaction Addon file
TRANSACTION_TMP_PATH_TEMPLATE = TMP_PATH_TEMPLATE + 'raw/'
TRANSACTION_FILE_DESCRIPTION = 'Humana hv000468 file'
TRANSACTION_FILE_NAME_TEMPLATE = 'record_data_{}'

# Deid file
DEID_FILE_DESCRIPTION = 'Humana hv000468 deid file'
DEID_FILE_NAME_TEMPLATE = 'deid_data_{}'

# Determine groups that are ready for processing
def do_get_groups_ready():
    received_files  = s3_utils.list_s3_bucket_files(S3_TRANSACTION_RAW_URL)
    processed_files = s3_utils.list_s3_bucket_files(S3_TRANSACTION_PROCESSED_URL_TEMPLATE)
    new_files = set(received_files).remove(set(processed_files))
    groups_ready = set()
    for f in new_files:
        gid = f.split('_')
        if TRANSACTION_FILE_NAME_TEMPLATE.format(f.split('_')[2]) in new_files \
                and DEID_FILE_NAME_TEMPLATE.format(f.split('_')[2]) in new_files:
            groups_ready.add(gid)

    kwargs['ti'].xcom_push(key = 'groups_ready', value = groups_ready)

get_groups_ready = PythonOperator(
    task_id = 'get_groups_ready',
    python_callable = do_get_groups_ready,
    provide_context = True,
    dag = mdag
)

def get_tmp_dir(ds, kwargs):
    return TRANSACTION_TMP_PATH_TEMPLATE.format(kwargs['ts_nodash'])

# Fetch, decrypt, push up transactions file
create_tmp_dir = BashOperator(
    task_id='create_tmp_dir',
    bash_command='mkdir -p {};'.format(TRANSACTION_TMP_PATH_TEMPLATE.format('{{ ts_nodash }}',)),
    dag=dag
)

def do_fetch_transaction_files(ds, **kwargs):
    group_ready = kwargs['ti'].xcom_pull(dag_id = DAG_NAME, task_ids = 'persist_groups_ready', key = 'groups_ready')
    for gid in groups_ready:
        s3_utils.fetch_file_from_s3(
            S3_TRANSACTION_RAW_URL + TRANSACTION_FILE_NAME_TEMPLATE.format(gid),
            get_tmp_dir(ds, kwargs) + TRANSACTION_FILE_NAME_TEMPLATE.format(gid)
        )

fetch_transaction_files = PythonOperator(
    task_id = 'fetch_transaction_files',
    python_callable = do_fetch_transaction_files,
    provide_context = True,
    dag = mdag
)

def encrypted_decrypted_file_paths_function(ds, kwargs):
    group_ready = kwargs['ti'].xcom_pull(dag_id = DAG_NAME, task_ids = 'persist_groups_ready', key = 'groups_ready')
    pairs = []
    for gid in groups_ready:
        fn = get_tmp_dir(ds, kwargs) + TRANSACTION_FILE_NAME_TEMPLATE.format(gid)
        pairs.append([fn, fn + '.gz'])

    return pairs

decrypt_transactions = SubDagOperator(
    subdag=decrypt_files.decrypt_files(
        DAG_NAME,
        'decrypt_transaction_files',
        default_args['start_date'],
        mdag.schedule_interval,
        {
            'tmp_dir_func'                        : get_tmp_dir,
            'encrypted_decrypted_file_paths_func' : encrypted_decrypted_file_paths_function
        }
    ),
    task_id='decrypt_transaction_files',
    dag=mdag
)

def do_push_transaction_files(ds, **kwargs):
    groups_ready = kwargs['ti'].xcom_pull(dag_id = DAG_NAME, task_ids = 'persist_groups_ready', key = 'groups_ready')

    for gid in groups_ready:
        fn = TRANSACTION_FILE_NAME_TEMPLATE.format(gid)
        s3_utils.copy_file(get_tmp_dir(ds, kwargs) + fn + '.gz',
            S3_TRANSACTION_PROCESSED_URL_TEMPLATE.format(gid) + fn)

if HVDAG.HVDAG.airflow_env != 'test':
    push_transaction_files = PythonOperator(
        task_id = 'push_transaction_files',
        python_callable = do_push_transaction_files,
        provide_context = True,
        dag = mdag
    )
else:
    push_transaction_files = DummyOperator(
        task_id = 'push_transaction_files',
        dag = mdag
    )

# Queue up DeID file for matching
def get_deid_file_urls(ds, kwargs):
    return [S3_TRANSACTION_RAW_URL + DEID_FILE_NAME_TEMPLATE.format(gid) for gid in groups_ready]

if HVDAG.HVDAG.airflow_env != 'test':
    queue_up_for_matching = SubDagOperator(
        subdag=queue_up_for_matching.queue_up_for_matching(
            DAG_NAME,
            'queue_up_for_matching',
            default_args['start_date'],
            mdag.schedule_interval,
            {
                'source_files_func' : get_deid_file_urls
            }
        ),
        task_id='queue_up_for_matching',
        dag=mdag
    )
else:
    queue_up_for_matching = DummyOperator(
        task_id='queue_up_for_matching',
        dag=mdag
    )

def do_trigger_deliveries(**kwargs):
    group_ready = kwargs['ti'].xcom_pull(dag_id = DAG_NAME, task_ids = 'persist_groups_ready', key = 'groups_ready')

    for gid in groups_ready:
        def do_trigger_delivery_dag(context, dag_run_obj):
            dag_run.run_id += '_' + gid
            dag_run_obj.payload = {
                    "group_id": gid
                }
            return dag_run_obj

        trigger_delivery_dag = TriggerDagRunOperator(
            task_id='trigger_delivery_dag_' + gid,
            trigger_dag_id='humana_hv000468_delivery',
            python_callable=do_trigger_delivery_dag,
            dag=mdag
        )

        trigger_deliver_dag.execute(**kwargs)

trigger_deliveries = PythonOperator(
    task_id = 'trigger_deliveries',
    python_callable = do_trigger_deliveries,
    provide_context = True,
    dag = mdag
)

clean_up_workspace = BashOperator(
    task_id='clean_up_workspace',
    bash_command='rm -rf {};'.format(TMP_PATH_TEMPLATE.format('{{ ts_nodash }}')),
    trigger_rule='all_done',
    dag=mdag
)

# Dependencies
create_tmp_dir.set_upstream(get_groups_ready)
fetch_transaction_files.set_upstream(create_tmp_dir)
decrypt_transactions.set_upstream(fetch_transaction_files)
push_transaction_files.set_upstream(decrypt_transactions)
clean_up_workspace.set_upstream(push_transaction_files)

queue_up_for_matching.set_upstream(get_groups_ready)

trigger_deliveries.set_upstream([push_transaction_files, queue_up_for_matching])
