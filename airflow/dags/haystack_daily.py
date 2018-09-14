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
import subdags.detect_move_normalize as detect_move_normalize
import subdags.clean_up_tmp_dir as clean_up_tmp_dir
import subdags.s3_push_files as s3_push_files

import util.date_utils as date_utils
import util.s3_utils as s3_utils

for m in [split_push_files, decrypt_files, queue_up_for_matching,
          detect_move_normalize, clean_up_tmp_dir, HVDAG,
          date_utils, s3_push_files]:
    reload(m)

# Applies to all files
TMP_PATH_TEMPLATE = '/tmp/haystack/custom/{}{}{}/'
DAG_NAME = 'haystack_daily'

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2018, 9, 13, 4),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2)
}

mdag = HVDAG.HVDAG(
    dag_id=DAG_NAME,
    schedule_interval='0 4 * * *',
    default_args=default_args
)

# Applies to all transaction files
if HVDAG.HVDAG.airflow_env == 'test':
    S3_INCOMING_LOCATION = 's3://salusv/testing/dewey/airflow/e2e/haystack/custom/outgoing/{}-{}-{}/'
    S3_INGESTION_URL = 's3://salusv/testing/dewey/airflow/e2e/haystack/custom/ingestion/'
    S3_TRANSACTION_URL_TEMPLATE = 's3://salusv/testing/dewey/airflow/e2e/haystack/custom/testing/'
    S3_PAYLOAD_DEST = 's3://salusv/testing/dewey/airflow/e2e/haystack/custom/payload/'
    S3_NORMALIZED_FILE_URL_TEMPLATE = 's3://salusv/testing/dewey/airflow/e2e/haystack/custom/spark-out/{}/'
    S3_DELIVERY_LOCATION = None
else:
    S3_INCOMING_LOCATION = 's3://haystack-deid-test/outgoing/{}-{}-{}/'
    S3_INGESTION_URL = 's3://healthverity/incoming/haystack/'
    S3_TRANSACTION_URL_TEMPLATE = 's3://salusv/incoming/custom/haystack/testing/{}/{}/{}/'
    S3_PAYLOAD_DEST = 's3://salusv/matching/payload/custom/haystack/testing/'
    S3_NORMALIZED_FILE_URL_TEMPLATE = 's3://salusv/deliverable/haystack/testing/{}/'
    S3_DELIVERY_LOCATION = 's3://haystack-deid-test/incoming/alnylam/'

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
    s3_incoming_loc2 = date_utils.insert_date_into_template(S3_INCOMING_LOCATION, kwargs, day_offset=1)

    try:
        received_files  = [s3_incoming_loc1 + f.split(' ')[-1] for f in subprocess.check_output(['aws', 's3', 'ls', s3_incoming_loc1], env=env).split('\n')[:-1]]
    except:
        received_files  = []

    try:
        received_files += [s3_incoming_loc2 + f.split(' ')[-1] for f in subprocess.check_output(['aws', 's3', 'ls', s3_incoming_loc2], env=env).split('\n')[:-1]]
    except:
        pass

    transaction_files = {}
    deid_files = {}
    for f in received_files:
        filename = f.split('/')[-1]
        if re.match(DEID_PREFIX + FILE_TEMPLATE.format('[0-9a-f-]*'), filename):
            group = filename.replace(DEID_PREFIX, '')
            received_groups.append(group)
            deid_files[group] = f
        if re.match(TRANSACTION_PREFIX + FILE_TEMPLATE.format('[0-9a-f-]*'), filename):
            group = filename.replace(TRANSACTION_PREFIX, '')
            received_groups.append(group)
            transaction_files[group] = f

    processed_groups = []
    for f in s3_utils.list_s3_bucket_files(S3_INGESTION_URL):
        group = f.split('/')[-1].split(DEID_PREFIX)[-1].split(TRANSACTION_PREFIX)[-1]
        processed_groups.append(group)

    # processed_files are in the format <gid>/<filename>
    new_groups = set(received_groups).difference(set(processed_groups))
    groups_ready = []
    for g in new_groups:
        if g in transaction_files and g in deid_files:
            groups_ready.append({
                'group_id'          : g,
                'transaction_file'  : transaction_files[g],
                'deid_file'         : deid_files[g]
            })

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
    return [f['transaction_file'].split('/')[-1] for f in groups_ready]

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
    deid_files = [g['deid_file'].split('/')[-1] for g in groups_ready]
    return [get_tmp_dir(ds, kwargs) + d for d in deid_files]

def get_deid_file_names(ds, kwargs):
    groups_ready = kwargs['ti'].xcom_pull(dag_id = DAG_NAME, task_ids = 'get_groups_ready', key = 'groups_ready')
    return [g['deid_file'].split('/')[-1] for g in groups_ready]

def do_fetch_files(ds, **kwargs):
    groups_ready = kwargs['ti'].xcom_pull(dag_id = DAG_NAME, task_ids = 'get_groups_ready', key = 'groups_ready')
    env = get_haystack_aws_env()
    tmp_dir = get_tmp_dir(ds, kwargs)
    subprocess.check_call(['mkdir', '-p', tmp_dir])
    for g in groups_ready:
        subprocess.check_call(['aws', 's3', 'cp', g['deid_file'], tmp_dir], env=env)
        subprocess.check_call(['aws', 's3', 'cp', g['transaction_file'], tmp_dir], env=env)

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
            's3_prefix_func'           : date_utils.generate_insert_date_into_template_function(S3_TRANSACTION_URL_TEMPLATE),
            'num_splits'               : 1
        }
    ),
    task_id='split_push_transaction_files',
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

#
# Post-Matching
#
def norm_args(ds, k):
    base = ['--date', ds]
    if HVDAG.HVDAG.airflow_env == 'test':
        base += ['--airflow_test']

    return base


detect_move_normalize_dag = SubDagOperator(
    subdag=detect_move_normalize.detect_move_normalize(
        DAG_NAME,
        'detect_move_normalize',
        default_args['start_date'],
        mdag.schedule_interval,
        {
            'expected_matching_files_func'      : get_deid_file_names,
            'file_date_func'                    : date_utils.generate_insert_date_into_template_function(
                '{}/{}/{}'
            ),
            's3_payload_loc_url'                : S3_PAYLOAD_DEST,
            'vendor_uuid'                       : 'e082e26a-0c90-4e7c-b8ac-1cc6704d52aa',
            'pyspark_normalization_script_name' : '/home/hadoop/spark/providers/haystack/custom/sparkNormalizeHaystack.py',
            'pyspark_normalization_args_func'   : norm_args,
            'pyspark'                           : True
        }
    ),
    task_id='detect_move_normalize',
    dag=mdag
)

def do_fetch_return_file(ds, **kwargs):
    s3_url = S3_NORMALIZED_FILE_URL_TEMPLATE.format(ds.replace('-', '/'))
    return_file = [f for f in 
            s3_utils.list_s3_bucket_files(s3_url)
            if 'daily_response' in f][0]
    kwargs['ti'].xcom_push(key='return_file', value=return_file)

    s3_utils.fetch_file_from_s3(s3_url + return_file, get_tmp_dir(ds, kwargs) + return_file)

fetch_return_file = PythonOperator(
    task_id='fetch_return_file',
    provide_context=True,
    python_callable=do_fetch_return_file,
    dag=mdag
)

def do_deliver_return_file(ds, **kwargs):
    env = get_haystack_aws_env()
    return_file = kwargs['ti'].xcom_pull(dag_id=DAG_NAME, task_ids='fetch_return_file', key='return_file')

    subprocess.check_call(['aws', 's3', 'cp', '--sse', 'AES256',
        get_tmp_dir(ds, kwargs) + return_file, S3_DELIVERY_LOCATION], env=env)

deliver_return_file = PythonOperator(
    task_id='deliver_return_file',
    provide_context=True,
    python_callable=do_deliver_return_file,
    dag=mdag
)

def do_clean_up_workspace(ds, **kwargs):
    return_file = kwargs['ti'].xcom_pull(dag_id=DAG_NAME, task_ids='fetch_return_file', key='return_file')
    os.remove(get_tmp_dir(ds, kwargs) + return_file)
    os.rmdir(get_tmp_dir(ds, kwargs))

clean_up_workspace = PythonOperator(
    task_id='clean_up_workspace',
    provide_context=True,
    python_callable=do_clean_up_workspace,
    trigger_rule='all_done',
    dag=mdag
)

if HVDAG.HVDAG.airflow_env == 'test':
    for t in ['queue_up_for_matching', 'fetch_return_file', 'deliver_return_file']:
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
detect_move_normalize_dag.set_upstream([queue_up_for_matching, split_push_transactions, push_raw_files])
fetch_return_file.set_upstream(detect_move_normalize_dag)
deliver_return_file.set_upstream(fetch_return_file)

# cleanup
clean_up_workspace.set_upstream(deliver_return_file)
