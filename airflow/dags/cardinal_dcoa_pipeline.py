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
import subdags.decrypt_files as decrypt_files
import subdags.split_push_files as split_push_files
import subdags.clean_up_tmp_dir as clean_up_tmp_dir
import subdags.run_pyspark_routine as run_pyspark_routine
import util.decompression as decompression
import util.s3_utils as s3_utils

for m in [s3_validate_file, s3_fetch_file,
          s3_push_files, decrypt_files, split_push_files,
          clean_up_tmp_dir, run_pyspark_routine, HVDAG,
          decompression, s3_utils]:
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
    schedule_interval='0 0 * * *',         #TODO: TBD
    default_args=default_args
)

TRANSACTION_FILE_NAME_TEMPLATE = 'out-record-{}.dat'   #TODO: This might change
EMR_CLUSTER_NAME_TEMPLATE = 'cardinal_dcoa_delivery_{}'
if HVDAG.HVDAG.airflow_env == 'test':
    S3_TRANSACTION_RAW_URL = 's3://salusv/testing/dewey/airflow/e2e/cardinal_dcoa/raw/'
    S3_TRANSACTION_PROCESSED_URL_TEMPLATE = 's3://salusv/testing/dewey/airflow/e2e/cardinal_dcoa/out/{}/{}/{}/'
    S3_DELIVERY_FILE_OUTPUT_LOCATION = 's3://salusv/testing/dewey/airflow/e2e/cardinal_dcoa/delivery/{}/{}/{}/'
    S3_DESTINATION_FILE_URL_TEMPLATE = 's3://salusv/testing/dewey/airflow/e2e/cardinal_dcoa/moved_out/cardinal_dcoa_normalized_{}{}{}.psv.gz'
else:
    S3_TRANSACTION_RAW_URL = 's3://healthverity/incoming/cardinal/dcoa/'
    S3_TRANSACTION_PROCESSED_URL_TEMPLATE = 's3://salusv/incoming/pharmacyclaims/cardinal_dcoa/{}/{}/{}/'
    S3_DELIVERY_FILE_OUTPUT_LOCATION = 's3://salusv/deliverable/cardinal_dcoa/{}/{}/{}/'
    S3_DESTINATION_FILE_URL_TEMPLATE='s3://fuse-file-drop/healthverity/dcoa/cardinal_dcoa_normalized_{}{}{}.psv.gz'      #TODO: Decide where this is dropped

TMP_PATH_TEMPLATE='/tmp/cardinal_dcoa/pharmacyclaims/{}/'

def get_date(kwargs):
    return (kwargs['execution_date'] + timedelta(days=1)).strftime('%Y%m%d')


def insert_formatted_date_function(template):
    def out(ds, kwargs):
        return template.format(get_date(kwargs))
    return out


def insert_current_date(template, kwargs):
    ds_nodash = get_date(kwargs)
    return template.format(
        ds_nodash[0:4],
        ds_nodash[4:6],
        ds_nodash[6:8]
    )

def insert_current_date_function(template):
    def out(ds, kwargs):
        return insert_current_date(template, kwargs)
    return out


def get_tmp_dir(ds, kwargs):
    return TMP_PATH_TEMPLATE.format(kwargs['ds_nodash'])


def norm_args(ds, k):
    base = ['--date', insert_current_date('{}-{}-{}', k),
            '--num_output_files', '1']
    if HVDAG.HVDAG.airflow_env == 'test':
        base += ['--airflow_test']

    return base


def get_transaction_file_paths(ds, kwargs):
    return [get_tmp_dir(ds, kwargs) + insert_formatted_date_function(TRANSACTION_FILE_NAME_TEMPLATE)(ds, kwargs)]


def encrypted_decrypted_file_paths_function(ds, kwargs):
    file_dir = get_tmp_dir(ds, kwargs)
    encrypted_file_path = file_dir \
        + insert_formatted_date_function(TRANSACTION_FILE_NAME_TEMPLATE)(ds, kwargs)
    return [
        [encrypted_file_path, encrypted_file_path + '.gz']
    ]


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
                    insert_formatted_date_function(
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
    validate_transaction_files = generate_file_validation_task(
        'transaction', TRANSACTION_FILE_NAME_TEMPLATE,
        10000
    )

fetch_transaction = SubDagOperator(
    subdag = s3_fetch_file.s3_fetch_file(
        DAG_NAME,
        'fetch_transaction_file',
        default_args['start_date'],
        mdag.schedule_interval,
        {
            'tmp_path_template'         : TMP_PATH_TEMPLATE,
            'expected_file_name_func'   : insert_formatted_date_function(
                TRANSACTION_FILE_NAME_TEMPLATE
            ),
            's3_prefix'                 : '/'.join(S3_TRANSACTION_RAW_URL.split('/')[3:]),
            's3_bucket'                 : 'salusv' if HVDAG.HVDAG.airflow_env == 'test' else 'hvincoming'
        }
    ),
    task_id = 'fetch_transaction_file',
    dag = mdag
)

decrypt_transaction = SubDagOperator(
    subdag = decrypt_files.decrypt_files(
        DAG_NAME,
        'decrypt_transaction_file',
        default_args['start_date'],
        mdag.schedule_interval,
        {
            'tmp_dir_func'                        : get_tmp_dir,
            'encrypted_decrypted_file_paths_func' : encrypted_decrypted_file_paths_function
        }
    ),
    task_id = 'decrypt_transaction_file',
    dag = mdag
)

split_transaction = SubDagOperator(
    subdag=split_push_files.split_push_files(
        DAG_NAME,
        'split_transaction_file',
        default_args['start_date'],
        mdag.schedule_interval,
        {
            'tmp_dir_func'             : get_tmp_dir,
            'file_paths_to_split_func' : get_transaction_file_paths,
            's3_prefix_func'           : insert_current_date_function(
                S3_TRANSACTION_PROCESSED_URL_TEMPLATE
            ),
            'num_splits'               : 20
        }
    ),
    task_id='split_transaction_file',
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

run_normalization = SubDagOperator(
    subdag = run_pyspark_routine.run_pyspark_routine(
        DAG_NAME,
        'run_normalization_script',
        default_args['start_date'],
        mdag.schedule_interval,
        {
            'EMR_CLUSTER_NAME_FUNC': insert_formatted_date_function(EMR_CLUSTER_NAME_TEMPLATE),
            'PYSPARK_SCRIPT_NAME': '/home/hadoop/spark/providers/cardinal_dcoa/pharmacyclaims/sparkNormalizeCardinalDCOA.py',
            'PYSPARK_ARGS_FUNC': norm_args,
            'NUM_NODES': 5,
            'NODE_TYPE': 'm4.xlarge',
            'EBS_VOLUME_SIZE': 50,
            'PURPOSE': 'delivery',
            'CONNECT_TO_METASTORE': False,
        }
    ),
    task_id = 'run_normalization_script',
    dag = mdag
)

def do_fetch_normalized_data(ds, **kwargs):
    bucket_files = s3_utils.list_s3_bucket_files(
                    insert_current_date(
                        S3_DELIVERY_FILE_OUTPUT_LOCATION,
                        kwargs
                    ))
    part_files = list(filter(lambda x: x != '_SUCCESS', bucket_files))

    for f in part_files:
        s3_utils.fetch_file_from_s3(
            insert_current_date(S3_DELIVERY_FILE_OUTPUT_LOCATION + f, kwargs),
            get_tmp_dir(ds, kwargs) + insert_current_date(S3_DELIVERY_FILE_OUTPUT_LOCATION + f, kwargs).split('/')[-1]
        )

    
fetch_normalized_data = PythonOperator(
    task_id='fetch_normalized_data',
    provide_context=True,
    python_callable=do_fetch_normalized_data,
    dag=mdag
)

deliver_normalized_data = SubDagOperator(
    subdag = s3_push_files.s3_push_files(
        DAG_NAME,
        'deliver_normalized_data',
        default_args['start_date'],
        mdag.schedule_interval,
        {
            'file_paths_func'       : lambda ds, kwargs: [
                get_tmp_dir(ds, kwargs) + \
                    insert_current_date(S3_DELIVERY_FILE_OUTPUT_LOCATION, kwargs).split('/')[-1]
            ],
            's3_prefix_func'        : lambda ds, kwargs: \
                '/'.join(insert_current_date(S3_DESTINATION_FILE_URL_TEMPLATE, kwargs).split('/')[3:]),
            's3_bucket'             : S3_DESTINATION_FILE_URL_TEMPLATE.split('/')[2],
            'aws_secret_key_id'     : Variable.get('AWS_ACCESS_KEY_ID') if HVDAG.HVDAG.airflow_env == 'test' else Variable.get('CardinalRaintree_AWS_ACCESS_KEY_ID'),
            'aws_secret_access_key' : Variable.get('AWS_SECRET_ACCESS_KEY') if HVDAG.HVDAG.airflow_env == 'test' else Variable.get('CardinalRaintree_AWS_SECRET_ACCESS_KEY')
        }
    ),
    task_id = 'deliver_normalized_data',
    dag = mdag
)

### DAG Structure ###
if HVDAG.HVDAG.airflow_env != 'test':
    fetch_transaction.set_upstream(validate_transaction_files)

decrypt_transaction.set_upstream(fetch_transaction)
split_transaction.set_upstream(decrypt_transaction)

run_normalization.set_upstream(split_transaction)

fetch_normalized_data.set_upstream(run_normalization)
deliver_normalized_data.set_upstream(fetch_normalized_data)
clean_up_workspace.set_upstream(deliver_normalized_data)

