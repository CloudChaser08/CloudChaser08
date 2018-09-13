from airflow.models import Variable
from airflow.operators import PythonOperator, SubDagOperator, DummyOperator
from datetime import datetime, timedelta
import os
import json
import subprocess

# hv-specific modules
import common.HVDAG as HVDAG
import subdags.detect_move_normalize as detect_move_normalize

import util.s3_utils as s3_utils
import util.sftp_utils as sftp_utils

for m in [detect_move_normalize, s3_utils, sftp_utils]:
    reload(m)

# Applies to all files
TMP_PATH_TEMPLATE = '/tmp/haystack/custom/{}/'
DAG_NAME = 'haystack_delivery_pipeline'

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2018, 8, 15),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2)
}

mdag = HVDAG.HVDAG(
    dag_id=DAG_NAME,
    schedule_interval=None,
    default_args=default_args
)

# Applies to all transaction files
if HVDAG.HVDAG.airflow_env == 'test':
    S3_PAYLOAD_DEST = 's3://salusv/testing/dewey/airflow/e2e/haystack/custom/payload/'
    S3_NORMALIZED_FILE_URL_TEMPLATE = 's3://salusv/testing/dewey/airflow/e2e/haystack/custom/spark-out/{}/'
    S3_DELIVERY_LOCATION = None
else:
    S3_PAYLOAD_DEST = 's3://salusv/matching/payload/custom/haystack/testing/'
    S3_NORMALIZED_FILE_URL_TEMPLATE = 's3://salusv/deliverable/haystack/testing/{}/'
    S3_DELIVERY_LOCATION = 's3://haystack-deid-test/incoming/alnylam/'

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

# Identify the group_id passed into this DagRun and push it to xcom
def do_persist_group_id(ds, **kwargs):
    group_id = kwargs['dag_run'].conf['group_id']

    kwargs['ti'].xcom_push(key='group_id', value=group_id)

persist_group_id = PythonOperator(
    task_id='persist_group_id',
    provide_context=True,
    python_callable=do_persist_group_id,
    dag=mdag
)

get_group_id = lambda ds, k: k['ti'].xcom_pull(dag_id=DAG_NAME, task_ids='persist_group_id', key='group_id')
get_tmp_dir  = lambda ds, k: TMP_PATH_TEMPLATE.format(get_group_id(ds, k))

create_tmp_dir = PythonOperator(
    task_id='create_tmp_dir',
    provide_context=True,
    python_callable=lambda ds, **k: os.makedirs(get_tmp_dir(ds, k)),
    dag=mdag
)

#
# Post-Matching
#
def norm_args(ds, k):
    group_id = get_group_id(ds, k)
    base = ['--group_id', group_id]
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
            'expected_matching_files_func'      : lambda ds, k: ['deid-' + get_group_id(ds, k)],
            'dest_dir_func'                     : get_group_id,
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
    gid = get_group_id(ds, kwargs)

    s3_url = S3_NORMALIZED_FILE_URL_TEMPLATE.format(gid)
    return_file = [f for f in 
            s3_utils.list_s3_bucket_files(S3_NORMALIZED_FILE_URL_TEMPLATE.format(gid))
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
    for t in ['fetch_return_file', 'deliver_return_file']:
        del mdag.task_dict[t]
        globals()[t] = DummyOperator(
            task_id = t,
            dag = mdag
        )

create_tmp_dir.set_upstream(persist_group_id)
detect_move_normalize_dag.set_upstream(create_tmp_dir)
fetch_return_file.set_upstream(detect_move_normalize_dag)
deliver_return_file.set_upstream(fetch_return_file)

# cleanup
clean_up_workspace.set_upstream(deliver_return_file)
