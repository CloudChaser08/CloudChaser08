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
    'start_date': datetime(2018, 4, 23, 12),
    'depends_on_past': True if HVDAG.HVDAG.airflow_env == 'prod' else False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2)
}

mdag = HVDAG.HVDAG(
    dag_id=DAG_NAME,
    schedule_interval="*/15 * * * *" if HVDAG.HVDAG.airflow_env == 'prod' else None,
    default_args=default_args
)

# Applies to all transaction files
if HVDAG.HVDAG.airflow_env == 'test':
    S3_TRANSACTION_RAW_URL = 's3://salusv/testing/dewey/airflow/e2e/humana/hv000468/raw/'
    S3_TRANSACTION_PROCESSED_URL_TEMPLATE = 's3://salusv/testing/dewey/airflow/e2e/humana/hv000468/processed/{}/'
else:
    S3_TRANSACTION_RAW_URL = 's3://healthverity/incoming/humana/'
    S3_TRANSACTION_PROCESSED_URL_TEMPLATE = 's3://salusv/data_requests/humana/hv000468/{}/'

# Deid file
DEID_FILE_DESCRIPTION = 'Humana hv000468 deid file'
DEID_FILE_NAME_TEMPLATE = 'deid_data_{}'

# Determine groups that are ready for processing
def do_get_groups_ready(**kwargs):
    received_files  = s3_utils.list_s3_bucket_files(S3_TRANSACTION_RAW_URL)
    processed_files = s3_utils.list_s3_bucket_files(S3_TRANSACTION_PROCESSED_URL_TEMPLATE.format('')[:-1])
    # processed_files are in the format <gid>/<filename>
    processed_files = [f.split('/')[-1] for f in processed_files]
    new_files = set(received_files).difference(set(processed_files))
    groups_ready = set()
    for f in new_files:
        gid = f.split('_')[-1]
        groups_ready.add(gid)

    kwargs['ti'].xcom_push(key = 'groups_ready', value = groups_ready)

get_groups_ready = PythonOperator(
    task_id = 'get_groups_ready',
    python_callable = do_get_groups_ready,
    provide_context = True,
    dag = mdag
)

# copy deid file (to keep track of ingested files)
def do_copy_deid_files(ds, **kwargs):
    groups_ready = kwargs['ti'].xcom_pull(dag_id = DAG_NAME, task_ids = 'get_groups_ready', key = 'groups_ready')
    for gid in groups_ready:
        fn = DEID_FILE_NAME_TEMPLATE.format(gid)
        s3_utils.copy_file(
            S3_TRANSACTION_RAW_URL + fn,
            S3_TRANSACTION_PROCESSED_URL_TEMPLATE.format(gid) + fn
        )

copy_deid_files = PythonOperator(
    task_id = 'copy_deid_files',
    python_callable = do_copy_deid_files,
    provide_context = True,
    dag = mdag
)

# Queue up DeID file for matching
def get_deid_file_urls(ds, kwargs):
    return [S3_TRANSACTION_RAW_URL + DEID_FILE_NAME_TEMPLATE.format(gid) for gid in groups_ready]

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
            trigger_dag_id='humana_hv000468_delivery',
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

clean_up_workspace = BashOperator(
    task_id='clean_up_workspace',
    bash_command='rm -rf {};'.format(TMP_PATH_TEMPLATE.format('{{ ts_nodash }}')),
    trigger_rule='all_done',
    dag=mdag
)

if HVDAG.HVDAG.airflow_env == 'test':
    for t in ['copy_deid_files', 'queue_up_for_matching']:
        del mdag.task_dict[t]
        globals()[t] = DummyOperator(
            task_id = t,
            dag = mdag
        )

# Dependencies
copy_deid_files.set_upstream(get_groups_ready)
queue_up_for_matching.set_upstream(copy_deid_files)
trigger_deliveries.set_upstream(queue_up_for_matching)
