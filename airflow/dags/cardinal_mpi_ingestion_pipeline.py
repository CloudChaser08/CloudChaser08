from airflow.models import Variable
from airflow.operators import PythonOperator, SubDagOperator
from datetime import datetime, timedelta
from subprocess import check_call
import re

# hv-specific modules
import common.HVDAG as HVDAG
import subdags.s3_validate_file as s3_validate_file
import subdags.queue_up_for_matching as queue_up_for_matching
import subdags.detect_move_normalize as detect_move_normalize

for m in [s3_validate_file, queue_up_for_matching,
          detect_move_normalize, HVDAG]:
    reload(m)

# Applies to all files
DAG_NAME = 'cardinal_mpi_ingestion_pipeline'

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2017, 8, 25, 14), # Unclear when this is going to start
    'depends_on_past': True,
    'retries': 3,
    'retry_delay': timedelta(minutes=2)
}

mdag = HVDAG.HVDAG(
    dag_id=DAG_NAME,
    schedule_interval=None, # No information about schedule yet
    default_args=default_args
)


if HVDAG.HVDAG.airflow_env == 'test':
    S3_PAYLOAD_DEST = 's3://salusv/testing/dewey/airflow/e2e/cardinal_mpi/custom/payload/'
else:
    S3_PAYLOAD_DEST = 's3://salusv/matching/payload/custom/cardinal_mpi/'


# Deid file without the trailing timestamp
# NOTE: File name format is not yet 
DEID_FILE_NAME_TEMPLATE = 'MPI.%Y%m%d.dat.gz'
DEID_FILE_NAME_UNZIPPED_TEMPLATE = 'MPI.%Y%m%d.dat'

def insert_execution_date_function(template):
    def out(ds, kwargs):
        return template.format(kwargs['ds_nodash'])
    return out


def insert_formatted_regex_function(template):
    def out(ds, kwargs):
        return re.sub(r'(%Y|%m|%d)', '{}', template).format('\d{4}', '\d{2}', '\d{2]')
    return out


def insert_current_date_function(date_template):
    def out(ds, kwargs):
        adjusted_date = kwargs['execution_date'] + timedelta(days=7)
        return adjusted_date.strftime(date_template)
    return out


def insert_current_date(template, kwargs):
    return insert_current_date_function(template)(None, kwargs)


def get_deid_file_urls(ds, kwargs):
    return [S3_TRANSACTION_RAW_URL + insert_current_date(
        DEID_FILE_NAME_TEMPLATE,
        kwargs
    )]


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
                'expected_file_name_func': lambda ds, k: (
                    insert_current_date_function(
                        path_template
                    )(ds, k)
                ),
                'file_name_pattern_func': lambda ds, k: (
                    insert_formatted_regex_function(
                        path_template
                    )(ds, k)
                ),
                'minimum_file_size': minimum_file_size,
                's3_prefix': '/'.join(S3_TRANSACTION_RAW_URL.split('/')[3:]),
                's3_bucket': 'healthverity',
                'file_description': 'Neogenomics ' + task_id + ' file'
            }
        ),
        task_id='validate_' + task_id + '_file',
        dag=mdag
    )


if HVDAG.HVDAG.airflow_env != 'test':
    validate_deid = generate_file_validation_task(
        'deid', DEID_FILE_NAME_TEMPLATE,
        1000000
    )

if HVDAG.HVDAG.airflow_env != 'test':
    queue_up_for_matching = SubDagOperator(
        subdag=queue_up_for_matching.queue_up_for_matching(
            DAG_NAME,
            'queue_up_for_matching',
            default_args['start_date'],
            mdag.schedule_interval,
            {
                'source_files_func': get_deid_file_urls
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
            'expected_matching_files_func': lambda ds, k: [
                insert_current_date_function(
                    DEID_FILE_NAME_UNZIPPED_TEMPLATE
                )(ds, k)
            ],
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
    queue_up_for_matching.set_upstream(validate_deid)
    detect_move_normalize_dag.set_upstream(queue_up_for_matching)

# implicit else, run detect_move_normalize_dag without waiting for the
# matching payload (test mode)
