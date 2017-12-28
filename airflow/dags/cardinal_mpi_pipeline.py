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
import subdags.queue_up_for_matching as queue_up_for_matching
import subdags.detect_move_normalize as detect_move_normalize
import subdags.clean_up_tmp_dir as clean_up_tmp_dir
import util.decompression as decompression
import util.s3_utils as s3_utils
import util.date_utils as date_utils

for m in [s3_validate_file, queue_up_for_matching, s3_fetch_file,
          detect_move_normalize, HVDAG, decompression,
          s3_utils, clean_up_tmp_dir, date_utils]:
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
    RAW_SOURCE_OF_TRUTH = 'testing/dewey/airflow/e2e/cardinal_mpi/medicalclaims/moved_raw/'
else:
    S3_PAYLOAD_DEST = 's3://salusv/matching/payload/custom/cardinal_mpi/'
    RAW_SOURCE_OF_TRUTH = 'incoming/cardinal/mpi/'

TMP_PATH_TEMPLATE='/tmp/cardinal_mpi/custom/{}{}{}/'
S3_DEID_RAW_URL='s3://hvincoming/cardinal_raintree/mpi/'
DEID_FILE_NAME_TEMPLATE = 'mpi.{}{}{}T\d{{2}}\d{{2}}\d{{2}}.zip'
DEID_FILE_NAME_REGEX = 'mpi.\d{{4}}\d{{2}}\d{{2}}T\d{{2}}\d{{2}}\d{{2}}.zip'
DEID_FILE_NAME_UNZIPPED_TEMPLATE = 'mpi-deid.{}{}{}T\d{2}\d{2}\d{2}.dat'

S3_NORMALIZED_FILE_URL_TEMPLATE='s3://salusv/warehouse/text/custom/cardinal_mpi/{}/{}/{}/part-00000.gz'
S3_DESTINATION_FILE_URL_TEMPLATE='s3://fuse-file-drop/healthverity/mpi/cardinal_mpi_matched_{}{}{}.psv.gz'

CARDINAL_MPI_DAY_OFFSET = 1

get_tmp_dir = date_utils.generate_insert_date_into_template_function(TMP_PATH_TEMPLATE)

def get_files_matching_template(template, ds, kwargs):
    file_dir = get_tmp_dir(ds, kwargs)
    file_regex = insert_date_into_template(template, kwargs, day_offset = CARDINAL_MPI_DAY_OFFSET)
    return [file_dir + f for f in os.listdir(file_dir) if re.search(file_regex, f)]

def get_deid_file_urls(ds, kwargs):
    return get_files_matching_template(DEID_FILE_NAME_UNZIPPED_TEMPLATE, ds, kwargs)


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
                    date_utils.generate_insert_date_into_template_function(
                        path_template,
                        k,
                        day_offset = CARDINAL_MPI_DAY_OFFSET
                    )
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
        10000
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
                date_utils.generate_insert_date_into_template_function(
                    DEID_FILE_NAME_TEMPLATE,
                    k,
                    day_offset = CARDINAL_MPI_DAY_OFFSET  
                )
            ),
            's3_prefix'              : '/'.join(S3_DEID_RAW_URL.split('/')[3:]),
            's3_bucket'              : 'hvincoming',
            'regex_name_match'       : True
        }
    ),
    task_id='fetch_deid_file',
    dag=mdag
)

push_to_healthverity_incoming = SubDagOperator(
    subdag = s3_push_files.s3_push_files(
        DAG_NAME,
        'push_to_healthverity_incoming',
        default_args['start_date'],
        mdag.schedule_interval,
        {
            'file_paths_func'   : lambda ds, kwargs: get_files_matching_template(DEID_FILE_NAME_TEMPLATE, ds, kwargs),
            'tmp_dir_func'      : get_tmp_dir,
            's3_prefix_func'    : lambda ds, kwargs: RAW_SOURCE_OF_TRUTH,
            's3_bucket'         : 'salusv' if HVDAG.HVDAG.airflow_env == 'test' else 'healthverity'
        }
    ),
    task_id = 'push_to_healthverity_incoming',
    dag = mdag
)

def do_unzip_file(ds, **kwargs):
    files = get_files_matching_template(DEID_FILE_NAME_TEMPLATE, ds, kwargs)
    decompression.decompress_zip_file(files[0], file_dir)

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
    base = ['--date', date_utils.insert_date_into_template('{}-{}-{}', k, day_offset = CARDINAL_MPI_DAY_OFFSET)]
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
            'file_date_func': date_utils.generate_insert_date_into_template_function(
                '{}/{}/{}',
                day_offset = CARDINAL_MPI_DAY_OFFSET
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
    fetch_normalized_data = PythonOperator(
        task_id='fetch_normalized_data',
        provide_context=True,
        python_callable=lambda ds, **kwargs: \
            s3_utils.fetch_file_from_s3(
                date_utils.insert_date_into_template(
                    S3_NORMALIZED_FILE_URL_TEMPLATE,
                    kwargs,
                    day_offset = CARDINAL_MPI_DAY_OFFSET
                ),
                get_tmp_dir(ds, kwargs) + \
                    date_utils.insert_date_into_template(
                        S3_NORMALIZED_FILE_URL_TEMPLATE, 
                        kwargs,
                        day_offset = CARDINAL_MPI_DAY_OFFSET).split("/")[-1]
        ),
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
                        date_utils.insert_date_into_template(
                            S3_NORMALIZED_FILE_URL_TEMPLATE, 
                            kwargs, 
                            day_offset = CARDINAL_MPI_DAY_OFFSET).split('/')[-1]
                ],
                's3_prefix_func'        : lambda ds, kwargs: \
                    '/'.join(date_utils.insert_date_into_template(S3_DESTINATION_FILE_URL_TEMPLATE, kwargs).split('/')[3:]),
                's3_bucket'             : S3_DESTINATION_FILE_URL_TEMPLATE.split('/')[2],
                'aws_access_key_id'     : Variable.get('CardinalRaintree_AWS_ACCESS_KEY_ID'),
                'aws_secret_access_key' : Variable.get('CardinalRaintree_AWS_SECRET_ACCESS_KEY')
            }
        ),
        task_id = 'deliver_normalized_data',
        dag = mdag
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

before_cleanup = [unzip_deid_file, push_to_healthverity_incoming]
if HVDAG.HVDAG.airflow_env != 'test':
    fetch_deid_file_dag.set_upstream(validate_deid)
    queue_up_for_matching.set_upstream(unzip_deid_file)
    detect_move_normalize_dag.set_upstream(queue_up_for_matching)
    fetch_normalized_data.set_upstream(detect_move_normalize_dag)
    deliver_normalized_data.set_upstream(fetch_normalized_data)
    before_cleanup += [deliver_normalized_data]

unzip_deid_file.set_upstream(fetch_deid_file_dag)
push_to_healthverity_incoming.set_upstream(fetch_deid_file_dag)
clean_up_workspace.set_upstream(before_cleanup)

# implicit else, run detect_move_normalize_dag without waiting for the
# matching payload (test mode)
