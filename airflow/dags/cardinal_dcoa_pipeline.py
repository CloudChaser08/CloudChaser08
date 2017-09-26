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
import subdags.detect_move_normalize as detect_move_normalize
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

S3_DESTINATION_FILE_URL_TEMPLATE='s3://fuse-file-drop/healthverity/mpi/cardinal_mpi_matched_%Y%m%d.psv.gz'      #TODO: Decide where this is dropped

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
    base = ['--date', insert_current_date('%Y-%m-%d', k)]
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
                's3_prefix'                 : '/'.join(S3_DEID_RAW_URL.split('/')[3:]),
                's3_bucket'                 : 'hvincoming',
                'file_description'          : 'Cardinal MPI ' + task_id + ' file',
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

detect_move_normalize_dag = SubDagOperator(
    subdag=detect_move_normalize.detect_move_normalize(
        DAG_NAME,
        'detect_move_normalize',
        default_args['start_date'],
        mdag.schedule_interval,
        {
            'expected_matching_files_func': lambda ds, k: (
                map(lambda f: f.split('/')[-1], get_deid_file_urls(ds, k))
            ),
            'file_date_func': insert_current_date_function(
                '%Y/%m/%d'
            ),
            's3_payload_loc_url': S3_PAYLOAD_DEST,          #TODO: there is no matching payload
            'vendor_uuid': 'eb27309e-adce-4057-b00c-69b8373e6f9c',  #TODO: maybe change?
            'pyspark_normalization_script_name': '/home/hadoop/spark/providers/cardinal_dcoa/pharmacyclaims/sparkNormalizeCardinal.py',
            'pyspark_normalization_args_func': norm_args,
            'pyspark': True
        }
    ),
    task_id='detect_move_normalize',
    dag=mdag
)

fetch_normalized_data = PythonOperator(
    task_id='fetch_normalized_data',
    provide_context=True,
    python_callable=lambda ds, kwargs: \
        s3_utils.fetch_file_from_s3(
            insert_current_date(S3_NORMALIZED_FILE_URL_TEMPLATE, kwargs),
            get_tmp_dir(ds, kwargs)
    ),
    dag=mdag
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

detect_move_normalize_dag.set_upstream(fetch_transaction_files)
fetch_normalized_data.set_upstream(detect_move_normalize_dag)

if HVDAG.HVDAG.airflow_env != 'test':
    deliver_normalized_data.set_upstream(fetch_normalized_data)

