from airflow.models import Variable
from airflow.operators import PythonOperator, SubDagOperator
from datetime import datetime, timedelta
from subprocess import check_call
import re

# hv-specific modules
import common.HVDAG as HVDAG
import subdags.s3_validate_file as s3_validate_file
import subdags.s3_fetch_file as s3_fetch_file
import subdags.decrypt_files as decrypt_files
import subdags.split_push_files as split_push_files
import subdags.queue_up_for_matching as queue_up_for_matching
import subdags.detect_move_normalize as detect_move_normalize
import util.s3_utils as s3_utils
import util.decompression as decompression
import util.date_utils as date_utils

for m in [s3_validate_file, s3_fetch_file, decrypt_files, split_push_files,
          queue_up_for_matching, detect_move_normalize, s3_utils,
          decompression, date_utils, HVDAG]:
    reload(m)

# Applies to all files
TMP_PATH_TEMPLATE = '/tmp/neogenomics/labtests/{}{}{}/'
DAG_NAME = 'neogenomics_pipeline'

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2017, 4, 13, 12),
    'depends_on_past': True,
    'retries': 3,
    'retry_delay': timedelta(minutes=2)
}

mdag = HVDAG.HVDAG(
    dag_id=DAG_NAME,
    schedule_interval="0 12 * * 0",
    default_args=default_args
)

NEOGENOMICS_DAY_OFFSET = 7

if HVDAG.HVDAG.airflow_env == 'test':
    S3_TRANSACTION_RAW_URL = 's3://salusv/testing/dewey/airflow/e2e/neogenomics/labtests/raw/'
    S3_TRANSACTION_PROCESSED_URL_TEMPLATE = 's3://salusv/testing/dewey/airflow/e2e/neogenomics/labtests/out/{}/{}/{}/'
    S3_PAYLOAD_DEST = 's3://salusv/testing/dewey/airflow/e2e/neogenomics/labtests/payload/'
else:
    S3_TRANSACTION_RAW_URL = 's3://healthverity/incoming/neogenomics/'
    S3_TRANSACTION_PROCESSED_URL_TEMPLATE = 's3://salusv/incoming/labtests/neogenomics/{}/{}/{}/'
    S3_PAYLOAD_DEST = 's3://salusv/matching/payload/labtests/neogenomics/'


# Transaction file without the trailing timestamp
TRANSACTION_FILE_NAME_TEMPLATE = 'TestMeta_{}{}{}.dat.gz'
TRANSACTION_FILE_NAME_UNZIPPED_TEMPLATE = 'TestMeta_{}{}{}.dat'

# Deid file without the trailing timestamp
DEID_FILE_NAME_TEMPLATE = 'TestPHI_{}{}{}.dat.gz'
DEID_FILE_NAME_UNZIPPED_TEMPLATE = 'TestPHI_{}{}{}.dat'

def get_deid_file_urls(ds, kwargs):
    return [S3_TRANSACTION_RAW_URL + date_utils.insert_date_into_template(
        DEID_FILE_NAME_TEMPLATE, kwargs, day_offset = NEOGENOMICS_DAY_OFFSET
    )]

def get_unzipped_file_paths(ds, kwargs):
    file_dir = date_utils.insert_date_into_template(TMP_PATH_TEMPLATE, kwargs)
    return [
        file_dir
        + date_utils.insert_date_into_template(
            TRANSACTION_FILE_NAME_UNZIPPED_TEMPLATE, kwargs, 
            day_offset = NEOGENOMICS_DAY_OFFSET
        )
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
                'expected_file_name_func': date_utils.generate_insert_date_into_template_function(
                    path_template, 
                    k,
                    day_offset = NEOGENOMICS_DAY_OFFSET
                ),
                'file_name_pattern_func': date_utils.generate_insert_regex_into_template_function(
                    path_template
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
    validate_transactional = generate_file_validation_task(
        'transaction', TRANSACTION_FILE_NAME_TEMPLATE,
        1000000
    )
    validate_deid = generate_file_validation_task(
        'deid', DEID_FILE_NAME_TEMPLATE,
        1000000
    )

fetch_transactional = SubDagOperator(
    subdag=s3_fetch_file.s3_fetch_file(
        DAG_NAME,
        'fetch_transaction_file',
        default_args['start_date'],
        mdag.schedule_interval,
        {
            'tmp_path_template': TMP_PATH_TEMPLATE,
            'expected_file_name_func': date_utils.generate_insert_date_into_template_function(
                TRANSACTION_FILE_NAME_TEMPLATE, 
                day_offset = NEOGENOMICS_DAY_OFFSET
            ),
            's3_prefix': '/'.join(S3_TRANSACTION_RAW_URL.split('/')[3:]),
            's3_bucket': 'salusv' if HVDAG.HVDAG.airflow_env == 'test' else 'healthverity',
        }
    ),
    task_id='fetch_transaction_file',
    dag=mdag
)


def gunzip_step(tmp_path_template, tmp_file_template):
    def execute(ds, **kwargs):
        decompression.decompress_gzip_file(
            date_utils.insert_date_into_template(tmp_path_template, kwargs)
            + date_utils.insert_date_into_template(tmp_file_template, kwargs, day_offset = NEOGENOMICS_DAY_OFFSET)
        )
    return PythonOperator(
        task_id='gunzip_transaction_file',
        provide_context=True,
        python_callable=execute,
        dag=mdag
    )


gunzip_transactional = gunzip_step(
    TMP_PATH_TEMPLATE, TRANSACTION_FILE_NAME_TEMPLATE
)

split_transactional = SubDagOperator(
    subdag=split_push_files.split_push_files(
        DAG_NAME,
        'split_transaction_file',
        default_args['start_date'],
        mdag.schedule_interval,
        {
            'tmp_dir_func': date_utils.generate_insert_date_into_template_function(
                TMP_PATH_TEMPLATE
            ),
            'file_paths_to_split_func': get_unzipped_file_paths,
            'file_name_pattern_func': date_utils.generate_insert_regex_into_template_function(
                    TRANSACTION_FILE_NAME_TEMPLATE
            ),
            's3_prefix_func': date_utils.generate_insert_date_into_template_function(
                S3_TRANSACTION_PROCESSED_URL_TEMPLATE, 
                day_offset = NEOGENOMICS_DAY_OFFSET
            ),
            'num_splits': 20
        }
    ),
    task_id='split_transaction_file',
    dag=mdag
)


def clean_up_workspace_step(task_id, template):
    def execute(ds, **kwargs):
        check_call([
            'rm', '-rf', date_utils.insert_date_into_template(template, kwargs)
        ])
    return PythonOperator(
        task_id='clean_up_workspace_' + task_id,
        provide_context=True,
        python_callable=execute,
        trigger_rule='all_done',
        dag=mdag
    )


clean_up_workspace = clean_up_workspace_step("all", TMP_PATH_TEMPLATE)

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
    base = ['--date', date_utils.insert_date_into_template('{}-{}-{}', k, day_offset = NEOGENOMICS_DAY_OFFSET)]
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
                date_utils.insert_date_into_template(
                    DEID_FILE_NAME_UNZIPPED_TEMPLATE, 
                    k,
                    day_offset = NEOGENOMICS_DAY_OFFSET
                )
            ],
            'file_date_func': date_utils.insert_date_into_template(
                '{}/{}/{}', kwargs, day_offset = NEOGENOMICS_DAY_OFFSET
            ),
            's3_payload_loc_url': S3_PAYLOAD_DEST,
            'vendor_uuid': 'cc11bfe2-d75a-432f-96b4-71240433d46f',
            'pyspark_normalization_script_name': '/home/hadoop/spark/providers/neogenomics/sparkNormalizeNeogenomics.py',
            'pyspark_normalization_args_func': norm_args,
            'pyspark': True
        }
    ),
    task_id='detect_move_normalize',
    dag=mdag
)

if HVDAG.HVDAG.airflow_env != 'test':
    fetch_transactional.set_upstream(validate_transactional)
    queue_up_for_matching.set_upstream(validate_deid)
    detect_move_normalize_dag.set_upstream(
        [queue_up_for_matching, split_transactional]
    )
else:
    detect_move_normalize_dag.set_upstream(
        split_transactional
    )

gunzip_transactional.set_upstream(fetch_transactional)
split_transactional.set_upstream(gunzip_transactional)

clean_up_workspace.set_upstream(
    split_transactional
)
