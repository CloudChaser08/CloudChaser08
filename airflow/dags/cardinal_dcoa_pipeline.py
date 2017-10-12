from airflow.models import Variable
from airflow.operators import PythonOperator, SubDagOperator
from datetime import datetime, timedelta
from subprocess import check_call
import re
import os

# hv-specific modules
import common.HVDAG as HVDAG
import subdags.s3_validate_file as s3_validate_file
import subdags.s3_fetch_file as s3_fetch_file
import subdags.s3_push_files as s3_push_files
import subdags.run_pyspark_routine as run_pyspark_routine
import util.decompression as decompression
import util.s3_utils as s3_utils

for m in [s3_validate_file, s3_fetch_file,
          detect_move_normalize, HVDAG, decompression,
          s3_utils]:
    reload(m)

# Applies to all files
DAG_NAME = 'cardinal_dcoa_pipeline'

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2017, 8, 24, 15),    #TODO: TBD
    'retries': 3,
    'retry_delay': timedelta(minutes=2)
}

mdag = HVDAG.HVDAG(
    dag_id=DAG_NAME,
    schedule_interval=None,         #TODO: TBD
    default_args=default_args
)

TMP_PATH_TEMPLATE='/tmp/cardinal_dcoa/pharmacyclaims/{}/'

TRANSACTION_FILE_NAME_TEMPLATE = 'dcoa_record_{}'   #TODO: This might change
if HVDAG.HVDAG.airflow_env == 'test':
    S3_TRANSACTION_RAW_URL = 's3://salusv/testing/dewey/airflow/e2e/cardinal/dcoa/raw/'
    S3_DELIVERY_FILE_URL_TEMPLATE = 's3://salusv/testing/dewey/airflow/e2e/cardinal/dcoa/out/{}/{}/{}/'
    S3_DESTINATION_FILE_URL_TEMPLATE = 's3://salusv/testing/dewey/airflow/e2e/cardinal/dcoa/moved_out/'
else:
    S3_TRANSACTION_RAW_URL = 's3://salusv/incoming/cardinal/dcoa/'
    S3_DELIVERY_FILE_URL_TEMPLATE = 's3://salusv/deliverable/cardinal_dcoa/{}/{}/{}/'
    S3_DESTINATION_FILE_URL_TEMPLATE='s3://fuse-file-drop/healthverity/dcoa/cardinal_dcoa_normalized_{}{}{}.psv.gz'      #TODO: Decide where this is dropped

def insert_current_date_function(date_template):
    def out(ds, kwargs):
        date = kwargs['execution_date'] + timedelta(days=1)
        return date.strftime(date_template)
    return out


def insert_current_date(template, kwargs):
    return insert_current_date_function(template)(None, kwargs)


def get_tmp_dir(ds, kwargs):
    return TMP_PATH_TEMPLATE.format(kwargs['ds_nodash'])


def norm_args(ds, k):
    base = ['--date', insert_current_date('%Y-%m-%d', k),
            '--num_output_files', '1']
    if HVDAG.HVDAG.airflow_env == 'test':
        base += ['--airflow_test']

    return base


def generate_file_validation_task(
        task_id, path_template, minimum_file_size
):
    return SubDagOperator(
        subdag=s3_validate_file.s3_validate_file(
            DAG_NAME,
            'validate_' + task_id + '_file',
            default_args['start_date'],
            mdag.schedule_interval,
            {
                'expected_file_name_func'   : lambda ds, k: (
                    insert_current_date_function(
                        path_template
                    )(ds, k)
                ),
                'file_name_pattern_func'    : lambda ds, k: (
                    DEID_FILE_NAME_REGEX
                ),
                'minimum_file_size'         : minimum_file_size,
                's3_prefix'                 : '/'.join(S3_TRANSACTION_RAW_URL.split('/')[3:]),
                's3_bucket'                 : 'hvincoming',
                'file_description'          : 'Cardinal DCOA ' + task_id + ' file',
                'regex_name_match'          : True,
                'quiet_retries'             : 24
            }
        ),
        task_id='validate_' + task_id + '_file',
        retries=6,
        retry_delay=timedelta(minutes=2),
        dag=mdag
    )

if HVDAG.HVDAG.airflow_env != 'test':
    validate_transaction_file = generate_file_validation_task(
        'transaction', TRANSACTION_FILE_NAME_TEMPLATE,
        10000
    )

run_normalization = SubDagOperator(
    subdag = run_pyspark.routine.run_pyspark_routine(
        DAG_NAME,
        'run_normalization_script',
        default_args['start_date'],
        mdag.schedule_interval,
        {
            'EMR_CLUSTER_NAME': emr_cluster_name_function(CLUSTER_NAME_TEMPLATE),
            'PYSPARK_SCRIPT_NAME': 'spark/providers/cardinal_dcoa/pharmacyclaims/sparkNormalizeCardinalDCOA.py',
            'PYSPARK_ARGS_FUNC': norm_args,
            'NUM_NODES': 5,
            'NODE_TYPE': 'm4.xlarge',
            'EBS_VOLUME_SIZE': 50,
            'PURPOSE': 'delivery',
            'CONNECTED_TO_METASTORE': False,
        }
    ),
    task_id = 'run_normalization_script',
    dag = mdag
)

#TODO: Determine what needs to be changed here.
if HVDAG.HVDAG.airflow_env != 'test':
    deliver_normalized_data = SubDagOperator(
        subdag = s3_push_files.s3_push_files(
            DAG_NAME,
            'deliver_normalized_data',
            default_args['start_date'],
            mdag.schedule_interval,
            {
                'file_paths_func'       : lambda ds, kwargs: (
                    get_tmp_dir(ds, kwargs) + \
                        insert_current_date(S3_NORMALIZED_FILE_URL_TEMPLATE, kwargs).split('/')[-1]
                ),
                's3_prefix_func'        : lambda ds, kwargs: \
                    '/'.join(insert_current_date(S3_DESTINATION_FILE_URL_TEMPLATE, kwargs).split('/')[3:]),
                's3_bucket'             : S3_DESTINATION_FILE_URL_TEMPLATE.split('/')[2],
                'aws_secret_key_id'     : Variable.get('CardinalRaintree_AWS_ACCESS_KEY_ID'),
                'aws_secret_access_key' : Variable.get('CardinalRaintree_AWS_SECRET_ACCESS_KEY')
            }
        ),
        task_id = 'deliver_normalized_data',
        dag = mdag
    )

if HVDAG.HVDAG.airflow_env == 'test':
    fetch_transaction_files.set_upstream(validate_transaction_files)

run_normalization.set_upstream(fetch_transaction_files)
fetch_normalized_data.set_upstream(detect_move_normalize_dag)

if HVDAG.HVDAG.airflow_env != 'test':
    deliver_normalized_data.set_upstream(fetch_normalized_data)

