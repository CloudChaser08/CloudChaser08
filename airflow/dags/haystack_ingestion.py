from airflow.models import Variable
from airflow.operators import *
from datetime import datetime, timedelta
import os
import re
import json
import subprocess

# hv-specific modules
import common.HVDAG as HVDAG
import subdags.split_push_files as split_push_files
import subdags.decrypt_files as decrypt_files
import subdags.queue_up_for_matching as queue_up_for_matching
import subdags.clean_up_tmp_dir as clean_up_tmp_dir
import subdags.s3_push_files as s3_push_files

import util.date_utils as date_utils
import util.s3_utils as s3_utils

for m in [split_push_files, decrypt_files, queue_up_for_matching,
          clean_up_tmp_dir, HVDAG, date_utils, s3_push_files]:
    reload(m)

# Applies to all files
TMP_PATH_TEMPLATE = '/tmp/haystack/custom/{}{}{}/'
DAG_NAME = 'haystack_ingestion_pipeline'

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2018, 8, 15),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2)
}

mdag = HVDAG.HVDAG(
    dag_id=DAG_NAME,
    schedule_interval='6,21,36,51 * * * *',
    default_args=default_args
)

# Applies to all transaction files
if HVDAG.HVDAG.airflow_env == 'test':
    S3_TRANSACTION_RAW_URL = 's3://salusv/testing/dewey/airflow/e2e/lhv2/custom/raw/'
    S3_TRANSACTION_PROCESSED_URL_TEMPLATE = 's3://salusv/testing/dewey/airflow/e2e/lhv2/custom/out/{}/{{}}/{{}}/{{}}/'
    S3_PAYLOAD_DEST = 's3://salusv/testing/dewey/airflow/e2e/lhv2/custom/payload/'
    S3_NORMALIZED_FILE_URL_TEMPLATE = 's3://salusv/testing/dewey/airflow/e2e/lhv2/custom/spark-out/{}/{{}}/{{}}/{{}}/'
else:
    S3_INCOMING_LOCATION = 's3://haystack-deid-test/outgoing/{}-{}-{}/'
    S3_INGESTION_URL = 's3://healthverity/incoming/haystack/'
    S3_TRANSACTION_URL_TEMPLATE = 's3://salusv/incoming/custom/haystack/testing/{}/'

# File naming patterns
FILE_TEMPLATE = '{}' # Just a timestamp

# Transaction files
TRANSACTION_PREFIX = 'record-'
TRANSACTION_TMP_PATH_TEMPLATE = TMP_PATH_TEMPLATE + 'raw/'

# Deid files
DEID_PREFIX = 'deid-'

get_tmp_dir = date_utils.generate_insert_date_into_template_function(
    TRANSACTION_TMP_PATH_TEMPLATE
)

def get_haystack_aws_env():
    ext_id_d = json.loads(subprocess.check_output(['aws', 'ssm', 'get-parameters', '--names', 'HealthVerityHaystackMavenAlnylamTest.extid', '--with-decryption']))
    ext_id = ext_id_d['Parameters'][0]['Value']
    assumer_creds_d = json.loads(subprocess.check_output(['aws', 'ssm', 'get-parameters', '--names', 'haystack-maven-s3-credentials', '--with-decryption']))
    assumer_creds = json.loads(assumer_creds_d['Parameters'][0]['Value'])
    assumer_env = dict(os.environ)
    assumer_env['AWS_ACCESS_KEY_ID']     = assumer_creds['aws_access_key_id']
    assumer_env['AWS_SECRET_ACCESS_KEY'] = assumer_creds['aws_secret_access_key']
    
    assumed_role_creds_d = json.loads(subprocess.check_output(['aws', 'sts', 'assume-role', '--role-arn', 'arn:aws:iam::278511714598:role/haystack-storage-deid-test-DeIdPartnerAccessRole-1FI81DT0QTRC7', '--role-session-name', 'de-id-pickup', '--external-id', ext_id], env=assumer_env))

    assumed_env = dict(os.environ)
    assumed_env['AWS_ACCESS_KEY_ID']     = assumed_role_creds_d['Credentials']['AccessKeyId']
    assumed_env['AWS_SECRET_ACCESS_KEY'] = assumed_role_creds_d['Credentials']['SecretAccessKey']
    assumed_env['AWS_SESSION_TOKEN']     = assumed_role_creds_d['Credentials']['SessionToken']

    return assumed_env


# Determine groups (deid + transaction file pairs) that are ready for processing
def do_get_groups_ready(**kwargs):
    received_groups = []
    env = get_haystack_aws_env()
    s3_incoming_loc1 = date_utils.insert_date_into_template(S3_INCOMING_LOCATION, kwargs)
    s3_incoming_loc2 = date_utils.insert_date_into_template(S3_INCOMING_LOCATION, kwargs, day_offset=-1)

    try:
        received_files  = [f.split(' ')[-1] for f in subprocess.check_output(['aws', 's3', 'ls', s3_incoming_loc1], env=env).split('\n')[:-1]]
    except:
        received_files  = []

    try:
        #received_files += [f.split(' ')[-1] for f in subprocess.check_output(['aws', 's3', 'ls', s3_incoming_loc2], env=env).split('\n')[:-1]]
        pass
    except:
        pass

    for f in received_files:
        if re.match(DEID_PREFIX + FILE_TEMPLATE.format('[0-9a-f-]*'), f):
            group = f.replace(DEID_PREFIX, '')
            received_groups.append(group)

    processed_groups = []
    for f in s3_utils.list_s3_bucket_files(S3_INGESTION_URL):
        group = f.split('/')[-1].split(DEID_PREFIX)[-1].split(TRANSACTION_PREFIX)[-1]
        processed_groups.append(group)

    # processed_files are in the format <gid>/<filename>
    new_groups = set(received_groups).difference(set(processed_groups))
    groups_ready = set()
    for g in new_groups:
        if (DEID_PREFIX + g) in received_files and (TRANSACTION_PREFIX + g) in received_files:
            groups_ready.add(g)

    kwargs['ti'].xcom_push(key = 'groups_ready', value = groups_ready)

get_groups_ready = PythonOperator(
    task_id = 'get_groups_ready',
    python_callable = do_get_groups_ready,
    provide_context = True,
    dag = mdag
)

def do_check_any_groups_ready(ds, **kwargs):
    groups_ready = kwargs['ti'].xcom_pull(dag_id = DAG_NAME, task_ids = 'get_groups_ready', key = 'groups_ready')
    if groups_ready:
        return True

    return False

check_any_groups_ready = ShortCircuitOperator(
    task_id='check_any_groups_ready',
    provide_context=True,
    python_callable=do_check_any_groups_ready,
    retries=0,
    dag=mdag
)

def get_transaction_file_names(ds, kwargs):
    groups_ready = kwargs['ti'].xcom_pull(dag_id = DAG_NAME, task_ids = 'get_groups_ready', key = 'groups_ready')
    return [TRANSACTION_PREFIX + g for g in groups_ready]

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
    return [get_tmp_dir(ds, kwargs) + DEID_PREFIX + g for g in groups_ready]

def do_fetch_files(ds, **kwargs):
    groups_ready = kwargs['ti'].xcom_pull(dag_id = DAG_NAME, task_ids = 'get_groups_ready', key = 'groups_ready')
    env = get_haystack_aws_env()
    tmp_dir = get_tmp_dir(ds, kwargs)
    subprocess.check_call(['mkdir', '-p', tmp_dir])
    s3_incoming_loc1 = date_utils.insert_date_into_template(S3_INCOMING_LOCATION, kwargs, day_offset=0)
    for g in groups_ready:
        subprocess.check_call(['aws', 's3', 'cp', s3_incoming_loc1 + DEID_PREFIX + g, tmp_dir], env=env)
        subprocess.check_call(['aws', 's3', 'cp', s3_incoming_loc1 + TRANSACTION_PREFIX + g, tmp_dir], env=env)

fetch_files = PythonOperator(
    provide_context=True,
    task_id='fetch_files',
    python_callable=do_fetch_files,
    dag=mdag
)

push_raw_files = SubDagOperator(
    subdag=s3_push_files.s3_push_files(
        DAG_NAME,
        'push_raw_files',
        default_args['start_date'],
        mdag.schedule_interval,
        {
            'file_paths_func'       : lambda ds, k: get_deid_file_urls(ds, k) + get_transaction_file_paths(ds, k),
            's3_prefix_func'        : lambda ds, k: '/'.join(S3_INGESTION_URL.split('/')[3:]),
            's3_bucket'             : S3_INGESTION_URL.split('/')[2],
        }
    ),
    task_id='push_raw_files',
    dag=mdag
)

decrypt_transaction_files = SubDagOperator(
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

split_push_transactions = SubDagOperator(
    subdag=split_push_files.split_push_files(
        DAG_NAME,
        'split_push_transaction_files',
        default_args['start_date'],
        mdag.schedule_interval,
        {
            'tmp_dir_func'             : get_tmp_dir,
            'file_paths_to_split_func' : get_transaction_file_paths,
            'file_name_pattern_func'   : lambda ds, k: TRANSACTION_PREFIX + '[0-9a-f-]*',
            's3_prefix_func'           : lambda ds, k: S3_TRANSACTION_URL_TEMPLATE.format(k['file_to_push'].split('.')[0].replace(TRANSACTION_PREFIX, '')),
            'num_splits'               : 1
        }
    ),
    task_id='split_push_transaction_files',
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
            'priority'          : 'priority1',
            'write_lock'        : True
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
            trigger_dag_id='haystack_delivery_pipeline',
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

check_any_groups_ready.set_upstream(get_groups_ready)
fetch_files.set_upstream(check_any_groups_ready)
push_raw_files.set_upstream(fetch_files)
decrypt_transaction_files.set_upstream(push_raw_files)
split_push_transactions.set_upstream(decrypt_transaction_files)
queue_up_for_matching.set_upstream(fetch_files)
trigger_deliveries.set_upstream([queue_up_for_matching, split_push_transactions])
clean_up_workspace.set_upstream([queue_up_for_matching, split_push_transactions, push_raw_files])
