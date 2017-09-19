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
import subdags.queue_up_for_matching as queue_up_for_matching
import subdags.detect_move_normalize as detect_move_normalize
import util.decompression as decompression
import util.s3_utils as s3_utils

for m in [s3_validate_file, queue_up_for_matching, s3_fetch_file,
          detect_move_normalize, HVDAG, decompression,
          s3_utils]:
    reload(m)

# Applies to all files
DAG_NAME = 'cardinal_mpi_pipeline'

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2017, 8, 24, 15),
    'retries': 3,
    'retry_delay': timedelta(minutes=2)
}

mdag = HVDAG.HVDAG(
    dag_id=DAG_NAME,
    schedule_interval="0 15 * * *",
    default_args=default_args
)


if HVDAG.HVDAG.airflow_env == 'test':
    S3_PAYLOAD_DEST = 's3://salusv/testing/dewey/airflow/e2e/cardinal_mpi/custom/payload/'
else:
    S3_PAYLOAD_DEST = 's3://salusv/matching/payload/custom/cardinal_mpi/'

TMP_PATH_TEMPLATE='/tmp/cardinal_mpi/custom/{}/'
S3_DEID_RAW_URL='s3://hvincoming/cardinal_raintree/mpi/'
DEID_FILE_NAME_TEMPLATE = 'mpi.%Y%m%dT\d{2}\d{2}\d{2}.zip'
DEID_FILE_NAME_REGEX = 'mpi.\d{4}\d{2}\d{2}T\d{2}\d{2}\d{2}.zip'
DEID_FILE_NAME_UNZIPPED_TEMPLATE = 'mpi-deid.%Y%m%dT\d{2}\d{2}\d{2}.dat'

S3_NORMALIZED_FILE_URL_TEMPLATE='s3://salusv/warehouse/text/custom/cardinal_mpi/%Y/%m/%d/part-00000.gz'
S3_DESTINATION_FILE_URL_TEMPLATE='s3://fuse-file-drop/healthverity/mpi/cardinal_mpi_matched_%Y%m%d.psv.gz'

def insert_current_date_function(date_template):
    def out(ds, kwargs):
        date = kwargs['execution_date'] + timedelta(days=1)
        return date.strftime(date_template)
    return out


def insert_current_date(template, kwargs):
    return insert_current_date_function(template)(None, kwargs)

def get_tmp_dir(ds, kwargs):
    return TMP_PATH_TEMPLATE.format(kwargs['ds_nodash'])

def get_deid_file_urls(ds, kwargs):
    file_dir = get_tmp_dir(ds, kwargs)
    todays_file_regex = insert_current_date(DEID_FILE_NAME_UNZIPPED_TEMPLATE, kwargs)
    files = filter(lambda f: re.search(todays_file_regex, f), os.listdir(file_dir))
    return map(lambda f: file_dir + f, files)


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
    validate_deid = generate_file_validation_task(
        'deid', DEID_FILE_NAME_TEMPLATE,
        1000000
    )

fetch_deid_file_dag = SubDagOperator(
    subdag=s3_fetch_file.s3_fetch_file(
        DAG_NAME,
        'fetch_deid_file',
        default_args['start_date'],
        mdag.schedule_interval,
        {
            'tmp_path_template'      : TMP_PATH_TEMPLATE,
            'expected_file_name_func': lambda ds, k: (
                insert_current_date_function(
                    DEID_FILE_NAME_TEMPLATE
                )(ds, k)
            ),
            's3_prefix'              : '/'.join(S3_DEID_RAW_URL.split('/')[3:]),
            's3_bucket'              : 'hvincoming',
            'regex_name_match'       : True
        }
    ),
    task_id='fetch_deid_file',
    dag=mdag
)

def do_unzip_file(ds, **kwargs):
    file_dir = get_tmp_dir(ds, kwargs)
    todays_file_regex = insert_current_date(DEID_FILE_NAME_TEMPLATE, kwargs)
    files = filter(lambda f: re.search(todays_file_regex, f), os.listdir(file_dir))
    decompression.decompress_zip_file(file_dir + files[0], file_dir)

unzip_deid_file = PythonOperator(
    task_id='unzip_deid_file',
    provide_context=True,
    python_callable=do_unzip_file,
    dag=mdag
)

if HVDAG.HVDAG.airflow_env != 'test':
    queue_up_for_matching = SubDagOperator(
        subdag=queue_up_for_matching.queue_up_for_matching(
            DAG_NAME,
            'queue_up_for_matching',
            default_args['start_date'],
            mdag.schedule_interval,
            {
                'source_files_func' : get_deid_file_urls,
                'priority'          : 'priority1'
            }
        ),
        task_id='queue_up_for_matching',
        dag=mdag
    )


#
# Post-Matching
#
def norm_args(ds, k):
    base = ['--date', insert_current_date('%Y-%m-%d', k)]
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
            'expected_matching_files_func': lambda ds, k: (
                map(lambda f: f.split('/')[-1], get_deid_file_urls(ds, k))
            ),
            'file_date_func': insert_current_date_function(
                '%Y/%m/%d'
            ),
            's3_payload_loc_url': S3_PAYLOAD_DEST,
            'vendor_uuid': 'eb27309e-adce-4057-b00c-69b8373e6f9c',
            'pyspark_normalization_script_name': '/home/hadoop/spark/providers/cardinal_mpi/custom/sparkNormalizeCardinal.py',
            'pyspark_normalization_args_func': norm_args,
            'pyspark': True
        }
    ),
    task_id='detect_move_normalize',
    dag=mdag
)

if HVDAG.HVDAG.airflow_env != 'test':
    def do_deliver_data(ds, **kwargs):
        source_file_url = insert_current_date(S3_NORMALIZED_FILE_URL_TEMPLATE, kwargs)
        destination_file_url = insert_current_date(S3_DESTINATION_FILE_URL_TEMPLATE, kwargs)
        destination_file_name = destination_file_url.split('/')[-1]
        file_dir = get_tmp_dir(ds, kwargs)

        check_call(['aws', 's3', 'cp', source_file_url, file_dir + destination_file_name])
        env = dict(os.environ)
        env['AWS_ACCESS_KEY_ID'] = Variable.get('CardinalRaintree_AWS_ACCESS_KEY_ID')
        env['AWS_SECRET_ACCESS_KEY'] = Variable.get('CardinalRaintree_AWS_SECRET_ACCESS_KEY')
        check_call(['aws', 's3', 'cp', '--sse', 'AES256', '--acl', \
            'bucket-owner-full-control', file_dir + destination_file_name, \
            destination_file_url], env=env)


    deliver_normalized_data = PythonOperator(
        task_id='deliver_normalized_data',
        provide_context=True,
        python_callable=do_deliver_data,
        dag=mdag
    )

if HVDAG.HVDAG.airflow_env != 'test':
    fetch_deid_file_dag.set_upstream(validate_deid)
    queue_up_for_matching.set_upstream(unzip_deid_file)
    detect_move_normalize_dag.set_upstream(queue_up_for_matching)
    deliver_normalized_data.set_upstream(detect_move_normalize_dag)

unzip_deid_file.set_upstream(fetch_deid_file_dag)

# implicit else, run detect_move_normalize_dag without waiting for the
# matching payload (test mode)
