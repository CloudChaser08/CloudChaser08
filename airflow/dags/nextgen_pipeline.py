from airflow.models import Variable
from airflow.operators import PythonOperator, SubDagOperator
from datetime import datetime, timedelta
from subprocess import check_call
import re
import os

# hv-specific modules
import common.HVDAG as HVDAG
import subdags.s3_validate_file as s3_validate_file
import subdags.detect_move_normalize as detect_move_normalize
import util.s3_utils as s3_utils

for m in [s3_validate_file, detect_move_normalize, HVDAG, s3_utils]:
    reload(m)

# Applies to all files
DAG_NAME = 'nextgen_pipeline'
TMP_PATH_TEMPLATE = '/tmp/nextgen/emr/{}/'

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2017, 9, 15, 12), # TDB, unclear when this will start
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2)
}

mdag = HVDAG.HVDAG(
    dag_id = DAG_NAME,
    schedule_interval = '0 12 * * 5',   # TBD
    default_args = default_args
)

if HVDAG.HVDAG.airflow_env == 'test':
    S3_TRANSACTION_RAW_URL = 's3://salusv/testing/dewey/airflow/e2e/nextgen/emr/raw/'
    S3_TRANSACTION_PROCESSED_URL_TEMPLATE = 's3://salusv/testing/dewey/airflow/e2e/nextgen/emr/out/{}/{}/{}/'
    S3_PAYLOAD_DEST = 's3://salusv/testing/dewey/airflow/e2e/nextgen/emr/payload/'
else:
    S3_TRANSACTION_RAW_URL = 's3://healthverity/incoming/ng-lssa/'
    S3_TRANSACTION_PROCESSED_URL_TEMPLATE = 's3://salusv/incoming/emr/nextgen/{}/{}/{}/'
    S3_PAYLOAD_DEST = 's3://salusv/matching/payload/emr/nextgen/'

# Transaction Addon file
TRANSACTION_TMP_PATH_TEMPLATE = TMP_PATH_TEMPLATE + 'raw/'
TRANSACTION_FILE_DESCRIPTION = 'Nextgen EMR transaction file'
TRANSACTION_FILE_NAME_TEMPLATE = 'NG_LSSA_{}_{}.txt.gz'

def get_file_date_nodash(kwargs):
    # TODO: Updat ethis to the correct date
    return (kwargs['execution_date']).strftime('%Y%m01')


def insert_formatted_file_date_function(template):
    def out(ds, kwargs):
        return template.format(get_file_date_nodash(kwargs))
    return out

def get_date(kwargs):
    return kwargs['ds_nodash']


def insert_formatted_date_function(template):
    def out(ds, kwargs):
        return template.format(get_date(kwargs))
    return out


def get_formatted_datetime(ds, kwargs):
    return kwargs['ti'].xcom_pull(dag_id = DAG_NAME, task_ids = 'get_datetime', key = 'file_datetime')


def insert_formatted_datetime_function(template):
    def out(ds, kwargs):
        return template.format(get_formatted_datetime(ds, kwargs))
    return out


def insert_formatted_regex_function(template):
    def out(ds, kwargs):
        return template.format('\d{5}', get_file_date_nodash(kwargs))
    return out


def insert_current_date(template, kwargs):
    ds_nodash = get_file_date_nodash(kwargs)
    return template.format(
        ds_nodash[0:4],
        ds_nodash[4:6],
        ds_nodash[6:8]
    )


def insert_current_date_function(template):
    def out(ds, kwargs):
        return insert_current_date(template, kwargs)
    return out


get_tmp_dir = insert_formatted_date_function(TRANSACTION_TMP_PATH_TEMPLATE)

def get_transaction_file_paths(ds, kwargs):
    return [get_tmp_dir(ds, kwargs) + TRANSACTION_FILE_NAME_TEMPLATE.format(
        get_formatted_datetime(ds, kwargs)
    )]


def get_deid_file_urls(ds, kwargs):
    return [S3_TRANSACTION_RAW_URL + DEID_FILE_NAME_TEMPLATE.format(
        get_formatted_datetime(ds, kwargs)
    )]


def get_deid_file_names(ds, kwargs):
    return [DEID_FILE_NAME_TEMPLATE.format(
        get_formatted_datetime(ds, kwargs)
    )]


def encrypted_decrypted_file_paths_function(ds, kwargs):
    file_dir = get_tmp_dir(ds, kwargs)
    encrypted_file_path = file_dir \
        + TRANSACTION_FILE_NAME_TEMPLATE.format(
            get_formatted_datetime(ds, kwargs)
        )
    return [
        [encrypted_file_path, encrypted_file_path + '.gz']
    ]

# There are going to be hundreds of files, check that at
# least the first one is like this
def generate_file_validation_task(
    task_id, path_template, minimum_file_size
):
    return SubDagOperator(
        subdag = s3_validate_file.s3_validate_file(
            DAG_NAME,
            'validate_' + task_id + '_file',
            default_args['start_date'],
            mdag.schedule_interval,
            {
                'expected_file_name_func' : insert_formatted_file_date_function(
                    path_template
                ),
                'file_name_pattern_func'  : insert_formatted_regex_function(
                    path_template
                ),
                'minimum_file_size'       : minimum_file_size,
                's3_prefix'               : '/'.join(S3_TRANSACTION_RAW_URL.split('/')[3:]),
                's3_bucket'               : 'healthverity',
                'file_description'        : 'Nextgen EMR ' + task_id + ' file'
            }
        ),
        task_id = 'validate_' + task_id + '_file',
        dag = mdag
    )


if HVDAG.HVDAG.airflow_env != 'test':
    validate_transaction = generate_file_validation_task(
        'transaction', TRANSACTION_FILE_NAME_TEMPLATE,
        30
    )

def do_copy_to_internal_s3(ds, kwargs):
    all_files = s3_utils.list_s3_bucket_files(S3_TRANSACTION_RAW_URL)
    regex_pattern = insert_formatted_regex_function(TRANSACTION_FILE_NAME_TEMPLATE)
    new_files = [f for f in all_files if re.search(regex_pattern, f)]
    internal_s3_dest = insert_current_date(S3_TRANSACTION_PROCESSED_URL_TEMPLATE, kwargs)
    i = 0
    copy_ops = []
    while i < len(new_files):
        for j in xrange(i, min(i+10, len(new_files))):
            copy_ops.append(copy_file_async(
                S3_TRANSACTION_RAW_URL + new_files[i],
                internal_s3_dest + new_files[i]
            ))
        i = min(i+10, len(new_files))
        for o in copy_ops:
            o.wait()

if HVDAG.HVDAG.airflow_env != 'test':
    copy_to_internal_s3 = PythonOperator(
        task_id = 'copy_to_internal_s3',
        python_callable = do_get_datetime,
        provide_context = True,
        dag = mdag
    )

#
# Post-Matching
#
def norm_args(ds, k):
    base = ['--date', insert_current_date('{}-{}-{}', k)]
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
            'expected_matching_files_func'      : lambda x: [],
            'file_date_func'                    : insert_current_date_function(
                '{}/{}/{}'
            ),
            's3_payload_loc_url'                : S3_PAYLOAD_DEST,
            'vendor_uuid'                       : 'c7cd671d-b60e-4cbd-b703-27267bcd9062',
            'pyspark_normalization_script_name' : '/home/hadoop/spark/providers/nextgen/emr/sparkNormalizeNextgenEMR.py',
            'pyspark_normalization_args_func'   : norm_args,
            'pyspark'                           : True
        }
    ),
    task_id='detect_move_normalize',
    dag=mdag
)

### Dag Structure ###
if HVDAG.HVDAG.airflow_env != 'test':
    deliver_normalized_data.set_upstream(copy_to_internal_s3)
