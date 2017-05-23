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

for m in [s3_validate_file, s3_fetch_file, decrypt_files, split_push_files,
          queue_up_for_matching, detect_move_normalize, s3_utils,
          decompression, HVDAG]:
    reload(m)

if Variable.get("AIRFLOW_ENV", default_var='').find('prod') != -1:
    airflow_env = 'prod'
elif Variable.get("AIRFLOW_ENV", default_var='').find('test') != -1:
    airflow_env = 'test'
else:
    airflow_env = 'dev'

if airflow_env == 'test':
    S3_TRANSACTION_RAW_URL = 's3://salusv/testing/dewey/airflow/e2e/neogenomics/labtests/raw/'
    S3_TRANSACTION_PROCESSED_URL_TEMPLATE = 's3://salusv/testing/dewey/airflow/e2e/neogenomics/labtests/out/%Y/%m/%d/'
    S3_PAYLOAD_DEST = 's3://salusv/testing/dewey/airflow/e2e/neogenomics/labtests/payload/'
else:
    S3_TRANSACTION_RAW_URL = 's3://healthverity/incoming/neogenomics/'
    S3_TRANSACTION_PROCESSED_URL_TEMPLATE = 's3://salusv/incoming/labtests/neogenomics/%Y/%m/%d/'
    S3_PAYLOAD_DEST = 's3://salusv/matching/payload/labtests/neogenomics/'


# Applies to all files
TMP_PATH_TEMPLATE = '/tmp/neogenomics/labtests/{}/'
DAG_NAME = 'neogenomics_pipeline'

# Transaction file without the trailing timestamp
TRANSACTION_FILE_NAME_TEMPLATE = 'TestMeta_%Y%m%d.dat.gz'
TRANSACTION_FILE_NAME_UNZIPPED_TEMPLATE = 'TestMeta_%Y%m%d.dat'

# Deid file without the trailing timestamp
DEID_FILE_NAME_TEMPLATE = 'TestPHI_%Y%m%d.dat.gz'
DEID_FILE_NAME_UNZIPPED_TEMPLATE = 'TestPHI_%Y%m%d.dat'

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2017, 4, 13, 12),
    'depends_on_past': True,
    'retries': 3,
    'retry_delay': timedelta(minutes=2)
}

mdag = HVDAG.HVDAG(
    dag_id=DAG_NAME,
    schedule_interval=(
        "0 12 * * 0"
        if airflow_env in ['prod', 'test']
        else None
    ),
    default_args=default_args
)


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


def get_unzipped_file_paths(ds, kwargs):
    file_dir = insert_execution_date_function(TMP_PATH_TEMPLATE)(ds, kwargs)
    return [
        file_dir
        + insert_current_date(
            TRANSACTION_FILE_NAME_UNZIPPED_TEMPLATE, kwargs
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


if airflow_env != 'test':
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
            'expected_file_name_func': lambda ds, k: (
                insert_current_date_function(
                    TRANSACTION_FILE_NAME_TEMPLATE
                )(ds, k)
            ),
            's3_prefix': '/'.join(S3_TRANSACTION_RAW_URL.split('/')[3:]),
            's3_bucket': 'healthveritydev' if airflow_env == 'test' else 'healthverity',
        }
    ),
    task_id='fetch_transaction_file',
    dag=mdag
)


def gunzip_step(tmp_path_template, tmp_file_template):
    def execute(ds, **kwargs):
        decompression.decompress_gzip_file(
            insert_execution_date_function(tmp_path_template)(ds, kwargs)
            + insert_current_date(tmp_file_template, kwargs)
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
            'tmp_dir_func': insert_execution_date_function(
                TMP_PATH_TEMPLATE
            ),
            'file_paths_to_split_func': get_unzipped_file_paths,
            's3_prefix_func': insert_current_date_function(
                S3_TRANSACTION_PROCESSED_URL_TEMPLATE
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
            'rm', '-rf', insert_execution_date_function(template)(ds, kwargs)
        ])
    return PythonOperator(
        task_id='clean_up_workspace_' + task_id,
        provide_context=True,
        python_callable=execute,
        trigger_rule='all_done',
        dag=mdag
    )


clean_up_workspace = clean_up_workspace_step("all", TMP_PATH_TEMPLATE)

if airflow_env != 'test':
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
    if airflow_env == 'test':
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
            'vendor_uuid': 'cc11bfe2-d75a-432f-96b4-71240433d46f',
            'pyspark_normalization_script_name': '/home/hadoop/spark/providers/neogenomics/sparkNormalizeNeogenomics.py',
            'pyspark_normalization_args_func': norm_args,
            'pyspark': True
        }
    ),
    task_id='detect_move_normalize',
    dag=mdag
)

if airflow_env != 'test':
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
