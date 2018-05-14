from airflow.operators import *
from airflow.models import Variable, DagRun
from airflow import settings
from datetime import datetime, timedelta

# hv-specific modules
import common.HVDAG as HVDAG
import subdags.detect_move_normalize as detect_move_normalize
import subdags.s3_push_files as s3_push_files

import util.s3_utils as s3_utils
import util.sftp_utils as sftp_utils

import logging
import json
import os

for m in [s3_push_files, detect_move_normalize, HVDAG,
        s3_utils, sftp_utils]:
    reload(m)

# Applies to all files
DAG_NAME = 'humana_hv000468_delivery'

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2018, 4, 26, 12),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2)
}

mdag = HVDAG.HVDAG(
    dag_id=DAG_NAME,
    schedule_interval=None,
    default_args=default_args
)

TMP_PATH_TEMPLATE = '/tmp/humana/hv000468/{}/'

# Applies to all transaction files
if HVDAG.HVDAG.airflow_env == 'test':
    S3_PAYLOAD_DEST = 's3://salusv/testing/dewey/airflow/e2e/humana/hv000468/payload/'
    S3_NORMALIZED_FILE_URL_TEMPLATE = 's3://salusv/testing/dewey/airflow/e2e/humana/hv000468/deliverable/{}/'
else:
    S3_PAYLOAD_DEST = 's3://salusv/matching/payload/custom/humana/hv000468/'
    S3_NORMALIZED_FILE_URL_TEMPLATE = 's3://salusv/deliverable/humana/hv000468/{}/'

# Deid file
DEID_FILE_DESCRIPTION = 'Humana hv000468 deid file'
DEID_FILE_NAME_TEMPLATE = 'deid_data_{}'

# Return files
MEDICAL_CLAIMS_EXTRACT_TEMPLATE = 'medical_claims_{}.psv.gz'
PHARMACY_CLAIMS_EXTRACT_TEMPLATE = 'pharmacy_claims_{}.psv.gz'

# Identify the group_id passed into this DagRun and push it to xcom
def do_get_group_id(ds, **kwargs):
    group_id = kwargs['dag_run'].conf['group_id']

    kwargs['ti'].xcom_push(key='group_id', value=group_id)

get_group_id = PythonOperator(
    task_id='get_group_id',
    provide_context=True,
    python_callable=do_get_group_id,
    dag=mdag
)

#
# Post-Matching
#

def get_expected_matching_files(ds, kwargs):
    group_id = kwargs['ti'].xcom_pull(dag_id=DAG_NAME, task_ids='get_group_id', key='group_id')

    expected_files = [
        DEID_FILE_NAME_TEMPLATE.format(group_id)
    ]
    if HVDAG.HVDAG.airflow_env != 'prod':
        logging.info(expected_files)
        return []
    return expected_files

def norm_args(ds, k):
    root_dag_run = get_root_dag_run(k)
    base = ['--group_id', group_id]
    if HVDAG.HVDAG.airflow_env == 'test':
        base += ['--airflow_test']

    return base


detect_move_extract_dag = SubDagOperator(
    subdag=detect_move_normalize.detect_move_normalize(
        DAG_NAME,
        'detect_move_extract',
        default_args['start_date'],
        mdag.schedule_interval,
        {
            'expected_matching_files_func'      : get_expected_matching_files,
            'file_date_func'                    : lambda ds, k:
                k['ti'].xcom_pull(dag_id=DAG_NAME, task_ids='get_group_id', key='group_id')
            's3_payload_loc_url'                : S3_PAYLOAD_DEST,
            'vendor_uuid'                       : '53769d77-189e-4d79-a5d4-d2d22d09331e',
            'pyspark_normalization_script_name' : '/home/hadoop/spark/delivery/humana/hv000468/sparkGenerateExtract.py',
            'pyspark_normalization_args_func'   : norm_args,
            'pyspark'                           : True,
            'connected_to_metastore'            : True,
            'emr_num_nodes'                     : 5,
            'emr_node_type'                     : 'm4.16xlarge',
            'emr_ebs_volume_size'               : 100,
            'spark_conf_args'                   : [
                '--conf', 'spark.sql.shuffle.partitions=5000',
                '--conf', 'spark.executor.cores=4', '--conf', 'spark.executor.memory=13G',
                '--conf', 'spark.hadoop.fs.s3a.maximum.connections=1000',
                '--conf', 'spark.files.useFetchCache=false'
            ]
        }
    ),
    task_id='detect_move_extract',
    dag=mdag
)

def get_tmp_dir(ds, kwargs):
    return TMP_PATH_TEMPLATE.format(
        kwargs['ti'].xcom_pull(dag_id=DAG_NAME, task_ids='get_group_id', key='group_id')
    )


def do_create_tmp_dir(ds, **kwargs):
    os.makedirs(TMP_PATH_TEMPLATE.format(
        kwargs['ti'].xcom_pull(dag_id=DAG_NAME, task_ids='get_group_id', key='group_id')
    )

# Fetch, decrypt, push up transactions file
create_tmp_dir = PythonOperator(
    task_id='create_tmp_dir',
    provide_context=True,
    python_callable=do_create_tmp_dir,
    dag=mdag
)

def do_fetch_extracted_data(ds, **kwargs):
    gid = kwargs['ti'].xcom_pull(dag_id=DAG_NAME, task_ids='get_group_id', key='group_id')
    for t in [MEDICAL_CLAIMS_EXTRACT_TEMPLATE, PHARMACY_CLAIMS_EXTRACT_TEMPLATE]:

        s3_utils.fetch_file_from_s3(
            S3_NORMALIZED_FILE_URL_TEMPLATE.format(gid) + \
                t.format(gid),
            get_tmp_dir(ds, kwargs) + t.format(gid)
        )

fetch_extracted_data = PythonOperator(
    task_id='fetch_extracted_data',
    provide_context=True,
    python_callable=do_fetch_extracted_data,
    dag=mdag
)

def do_deliver_extracted_data(ds, **kwargs):
    sftp_config = json.loads(Variable.get('humana_prod_sftp_configuration'))
    gid = kwargs['ti'].xcom_pull(dag_id=DAG_NAME, task_ids='get_group_id', key='group_id')
    for t in [MEDICAL_CLAIMS_EXTRACT_TEMPLATE, PHARMACY_CLAIMS_EXTRACT_TEMPLATE]:

        sftp_utils.upload_file(
            get_tmp_dir(ds, kwargs) + t.format(gid), **sftp_config
        )

deliver_extracted_data = PythonOperator(
    task_id='deliver_extracted_data',
    provide_context=True,
    python_callable=do_deliver_extracted_data,
    dag=mdag
)

def do_clean_up_workspace(ds, **kwargs):
    gid = kwargs['ti'].xcom_pull(dag_id=DAG_NAME, task_ids='get_group_id', key='group_id')
    for t in [MEDICAL_CLAIMS_EXTRACT_TEMPLATE, PHARMACY_CLAIMS_EXTRACT_TEMPLATE]:
        os.remove(TMP_PATH_TEMPLATE.format(gid) + t.format(gid))

    os.rmdir(TMP_PATH_TEMPLATE.format(gid))

clean_up_workspace = PythonOperator(
    task_id='clean_up_workspace',
    provide_context=True,
    python_callable=do_clean_up_workspace,
    trigger_rule='all_done',
    dag=mdag
)

if HVDAG.HVDAG.airflow_env == 'test':
    for t in ['deliver_extracted_data']:
        del mdag.task_dict[t]
        globals()[t] = DummyOperator(
            task_id = t,
            dag = mdag
        )

detect_move_extract_dag.set_upstream(get_group_id)
create_tmp_dir.set_upstream(detect_move_extract_dag)
fetch_extracted_data.set_upstream(create_tmp_dir)
deliver_extracted_data.set_upstream(fetch_extracted_data)
clean_up_workspace.set_upstream(deliver_extracted_data)
