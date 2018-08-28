from airflow.models import Variable
from airflow.operators import *
from datetime import datetime, timedelta
import os
import json

# hv-specific modules
import common.HVDAG as HVDAG
import subdags.detect_move_normalize as detect_move_normalize

import util.s3_utils as s3_utils
import util.sftp_utils as sftp_utils
import util.ses_utils as ses_utils

for m in [detect_move_normalize, s3_utils, sftp_utils,
        ses_utils]:
    reload(m)

# Applies to all files
TMP_PATH_TEMPLATE = '/tmp/liquidhub/custom/{}/'
DAG_NAME = 'liquidhub_delivery_pipeline'

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2018, 7, 25),
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
    S3_PAYLOAD_DEST = 's3://salusv/testing/dewey/airflow/e2e/lhv2/custom/payload/'
    S3_NORMALIZED_FILE_URL_TEMPLATE = 's3://salusv/testing/dewey/airflow/e2e/lhv2/custom/spark-out/{}/'
else:
    S3_PAYLOAD_DEST = 's3://salusv/matching/payload/custom/lhv2/'
    S3_NORMALIZED_FILE_URL_TEMPLATE = 's3://salusv/deliverable/lhv2/{}/'

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
    group_id = get_group_id(d, k)
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
            'expected_matching_files_func'      : lambda ds, k: [get_group_id(ds, k)],
            'dest_dir_func'                     : get_group_id,
            's3_payload_loc_url'                : S3_PAYLOAD_DEST,
            'vendor_uuid'                       : '38fb274f-6438-4845-83f1-6d917fea6682',
            'pyspark_normalization_script_name' : '/home/hadoop/spark/providers/liquidhub/custom/sparkNormalizeLiquidhub.py',
            'pyspark_normalization_args_func'   : norm_args,
            'pyspark'                           : True
        }
    ),
    task_id='detect_move_normalize',
    dag=mdag
)

def do_check_errors(ds, **kwargs):
    gid = get_group_id(ds, kwargs)

    s3_url = S3_NORMALIZED_FILE_URL_TEMPLATE.format(gid)
    error_files = [f for f in 
            s3_utils.list_s3_bucket_files(s3_url)
            if f.startswith('error_report')]
    if len(error_files) > 0:
        kwargs['ti'].xcom_push(key='error_file', value=error_files[0])
        return 'fetch_error_file'

    return 'fetch_return_file'

check_errors = BranchPythonOperator(
    task_id='check_errors',
    provide_context=True,
    python_callable=do_check_errors,
    dag=mdag
)

def do_fetch_error_file(ds, **kwargs):
    gid = get_group_id(ds, kwargs)

    s3_url = S3_NORMALIZED_FILE_URL_TEMPLATE.format(gid)
    error_file = kwargs['ti'].xcom_pull(dag_id=DAG_NAME, task_ids='check_errors', key='error_file')

    s3_utils.fetch_file_from_s3(s3_url + error_file, get_tmp_dir(ds, kwargs) + error_file)

fetch_error_file = PythonOperator(
    task_id='fetch_error_file',
    provide_context=True,
    python_callable=do_fetch_error_file,
    dag=mdag
)

def do_email_error_report(ds, **kwargs):
    RECIPIENTS = [
        'Lauren Langhauser <lfrancis@healthverity.com>',
        'Cree Flory <cflory@healthverity.com>',
        'Dominique Hurley <dhurley@healthverity.com>'
    ]

    error_file = kwargs['ti'].xcom_pull(dag_id=DAG_NAME, task_ids='check_errors', key='error_file')
    with open(get_tmp_dir(ds, kwargs) + error_file) as fin:
        lines = fin.readlines()

    LOTS_PATIENTS_MSG = 'Source "{}" sent unexpected manufacturer value "{}" for {} patients\n'
    FEW_PATIENTS_MSG  = 'Source "{}" sent unexpected manufacturer value "{}" for patient id "{}"\n'
    email_content = ''
    for l in lines:
        (source, manufacturer, count, patient_ids) = l.strip().split('|')

        patient_ids = json.loads(patient_ids)
        if patient_ids:
            email_content += ''.join([FEW_PATIENTS_MSG.format(source, manufacturer, patient_id) for patient_id in patient_ids])
        else:
            email_content += LOTS_PATIENTS_MSG.format(source, manufacturer, count)

    ses_utils.send_email(
        'noreply@healthverity.com',
        RECIPIENTS,
        'Unexpected Manufacturers Detected in {}'.format(get_group_id(ds, kwargs)),
        email_content
    )

email_error_report = PythonOperator(
    task_id='email_error_report',
    provide_context=True,
    python_callable=do_email_error_report,
    dag=mdag
)

def do_fetch_return_file(ds, **kwargs):
    gid = get_group_id(ds, kwargs)

    s3_url = S3_NORMALIZED_FILE_URL_TEMPLATE.format(gid)
    return_file = [f for f in
            s3_utils.list_s3_bucket_files(S3_NORMALIZED_FILE_URL_TEMPLATE.format(gid))
            if f.startswith('LHV')][0]
    kwargs['ti'].xcom_push(key='return_file', value=return_file)

    s3_utils.fetch_file_from_s3(s3_url + return_file, get_tmp_dir(ds, kwargs) + return_file)

fetch_return_file = PythonOperator(
    task_id='fetch_return_file',
    provide_context=True,
    python_callable=do_fetch_return_file,
    dag=mdag
)

def do_deliver_return_file(ds, **kwargs):
    sftp_config = json.loads(Variable.get('lh_amgen_hv_sftp_configuration'))
    path = sftp_config['path']
    del sftp_config['path']
    gid = get_group_id(ds, kwargs)
    return_file = kwargs['ti'].xcom_pull(dag_id=DAG_NAME, task_ids='fetch_return_file', key='return_file')

    sftp_utils.upload_file(
        get_tmp_dir(ds, kwargs) + return_file,
        path,
        ignore_host_key=True,
         **sftp_config
    )

deliver_return_file = PythonOperator(
    task_id='deliver_return_file',
    provide_context=True,
    python_callable=do_deliver_return_file,
    dag=mdag
)

def do_clean_up_workspace(ds, **kwargs):
    return_file = kwargs['ti'].xcom_pull(dag_id=DAG_NAME, task_ids='fetch_return_file', key='return_file')
    error_file = kwargs['ti'].xcom_pull(dag_id=DAG_NAME, task_ids='check_errors', key='error_file')
    try:
        os.remove(get_tmp_dir(ds, kwargs) + return_file)
    except:
        pass

    try:
        os.remove(get_tmp_dir(ds, kwargs) + error_file)
    except:
        pass
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
check_errors.set_upstream(detect_move_normalize_dag)
fetch_error_file.set_upstream(check_errors)
email_error_report.set_upstream(fetch_error_file)
fetch_return_file.set_upstream(check_errors)
deliver_return_file.set_upstream(fetch_return_file)

# cleanup
clean_up_workspace.set_upstream([deliver_return_file, email_error_report])
