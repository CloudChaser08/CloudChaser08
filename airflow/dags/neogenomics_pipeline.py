from airflow.operators import PythonOperator, SubDagOperator
from datetime import datetime, timedelta
from subprocess import check_call

# hv-specific modules
import common.HVDAG as HVDAG
import subdags.s3_validate_file as s3_validate_file
import subdags.s3_fetch_file as s3_fetch_file
import subdags.decrypt_files as decrypt_files
import subdags.split_push_files as split_push_files
import subdags.queue_up_for_matching as queue_up_for_matching
import subdags.update_analytics_db as update_analytics_db
import subdags.detect_move_normalize as detect_move_normalize
import util.s3_utils as s3_utils
import util.decompression as decompression
import util.date_utils as date_utils

for m in [s3_validate_file, s3_fetch_file, decrypt_files, split_push_files,
          queue_up_for_matching, update_analytics_db, detect_move_normalize, s3_utils,
          decompression, date_utils, HVDAG]:
    reload(m)

# Applies to all files
TMP_PATH_TEMPLATE = '/tmp/neogenomics/labtests/{}{}{}/'
DAG_NAME = 'neogenomics_pipeline'

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2017, 10, 2, 17),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2)
}

mdag = HVDAG.HVDAG(
    dag_id=DAG_NAME,
    schedule_interval="0 17 * * 1",
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

S3_TRANSACTION_PROCESSED_TESTS_URL_TEMPLATE = S3_TRANSACTION_PROCESSED_URL_TEMPLATE + 'tests/'
S3_TRANSACTION_PROCESSED_RESULTS_URL_TEMPLATE = S3_TRANSACTION_PROCESSED_URL_TEMPLATE + 'results/'

TRANSACTION_RESULTS_FILE_NAME_TEMPLATE = 'NeoG_HV_STD_W_{{}}{{}}{{}}_{}{}{}_R.txt'
TRANSACTION_TESTS_FILE_NAME_TEMPLATE = 'NeoG_HV_STD_W_{{}}{{}}{{}}_{}{}{}_NPHI.txt'

TMP_PATH_TESTS_TEMPLATE = TMP_PATH_TEMPLATE + 'tests/'
TMP_PATH_RESULTS_TEMPLATE = TMP_PATH_TEMPLATE + 'results/'

DEID_FILE_NAME_TEMPLATE = 'NeoG_HV_STD_W_{{}}{{}}{{}}_{}{}{}_PHI.txt'

def generate_file_name_function(template):
    return lambda ds, k: date_utils.insert_date_into_template(
        date_utils.insert_date_into_template(
            template, k, day_offset=NEOGENOMICS_DAY_OFFSET
        ), k, day_offset=NEOGENOMICS_DAY_OFFSET - 1
    )


def generate_file_name_pattern_function(template):
    return lambda ds, k: date_utils.generate_insert_regex_into_template_function(
        date_utils.generate_insert_regex_into_template_function(
            template, year_regex = '\d{{4}}', month_regex = '\d{{2}}', day_regex = '\d{{2}}'
        )(ds, k)
    )(ds, k)


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
                'expected_file_name_func': generate_file_name_function(path_template),
                'file_name_pattern_func': generate_file_name_pattern_function(path_template),
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
    validate_transactional_tests = generate_file_validation_task(
        'transaction_tests', TRANSACTION_TESTS_FILE_NAME_TEMPLATE,
        1000000
    )
    validate_transactional_results = generate_file_validation_task(
        'transaction_results', TRANSACTION_RESULTS_FILE_NAME_TEMPLATE,
        1000000
    )
    validate_deid = generate_file_validation_task(
        'deid', DEID_FILE_NAME_TEMPLATE,
        1000000
    )

def generate_fetch_transactional_task(task_id, file_template, local_template):
    return SubDagOperator(
        subdag=s3_fetch_file.s3_fetch_file(
            DAG_NAME,
            'fetch_' + task_id + '_file',
            default_args['start_date'],
            mdag.schedule_interval,
            {
                'tmp_path_template': local_template,
                'expected_file_name_func': generate_file_name_function(file_template),
                's3_prefix': '/'.join(S3_TRANSACTION_RAW_URL.split('/')[3:]),
                's3_bucket': 'salusv' if HVDAG.HVDAG.airflow_env == 'test' else 'healthverity',
            }
        ),
        task_id='fetch_' + task_id + '_file',
        dag=mdag
    )


fetch_transactional_tests = generate_fetch_transactional_task(
    'transaction_tests', TRANSACTION_TESTS_FILE_NAME_TEMPLATE, TMP_PATH_TESTS_TEMPLATE
)
fetch_transactional_results = generate_fetch_transactional_task(
    'transaction_results', TRANSACTION_RESULTS_FILE_NAME_TEMPLATE, TMP_PATH_RESULTS_TEMPLATE
)


def generate_split_transactional_task(task_id, file_template, s3_template, local_template):
    return SubDagOperator(
        subdag=split_push_files.split_push_files(
            DAG_NAME,
            'split_' + task_id + '_file',
            default_args['start_date'],
            mdag.schedule_interval,
            {
                'tmp_dir_func': date_utils.generate_insert_date_into_template_function(
                    local_template
                ),
                'file_paths_to_split_func': lambda ds, k: [
                    date_utils.insert_date_into_template(local_template, k)
                    + generate_file_name_function(file_template)(ds, k)
                ],
                'file_name_pattern_func': generate_file_name_pattern_function(file_template),
                's3_prefix_func': date_utils.generate_insert_date_into_template_function(
                    s3_template, day_offset = NEOGENOMICS_DAY_OFFSET
                ),
                'num_splits': 20
            }
        ),
        task_id='split_' + task_id + '_file',
        dag=mdag
    )


split_transactional_tests = generate_split_transactional_task(
    'transaction_tests', TRANSACTION_TESTS_FILE_NAME_TEMPLATE, S3_TRANSACTION_PROCESSED_TESTS_URL_TEMPLATE,
    TMP_PATH_TESTS_TEMPLATE
)
split_transactional_results = generate_split_transactional_task(
    'transaction_results', TRANSACTION_RESULTS_FILE_NAME_TEMPLATE, S3_TRANSACTION_PROCESSED_RESULTS_URL_TEMPLATE,
    TMP_PATH_RESULTS_TEMPLATE
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
                'source_files_func': lambda ds, k: [
                    S3_TRANSACTION_RAW_URL + generate_file_name_function(DEID_FILE_NAME_TEMPLATE)(ds, k)
                ]
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
                generate_file_name_function(DEID_FILE_NAME_TEMPLATE)(ds, k)
            ],
            'file_date_func': date_utils.generate_insert_date_into_template_function(
                '{}/{}/{}', day_offset = NEOGENOMICS_DAY_OFFSET
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

sql_new_template = """MSCK REPAIR TABLE labtests_20170216"""

if HVDAG.HVDAG.airflow_env != 'test':
    update_analytics_db = SubDagOperator(
        subdag=update_analytics_db.update_analytics_db(
            DAG_NAME,
            'update_analytics_db',
            default_args['start_date'],
            mdag.schedule_interval,
            {
                'sql_command_func' : lambda ds, k: sql_new_template
            }
        ),
        task_id='update_analytics_db',
        dag=mdag
    )

if HVDAG.HVDAG.airflow_env != 'test':
    fetch_transactional_tests.set_upstream(validate_transactional_tests)
    fetch_transactional_results.set_upstream(validate_transactional_results)
    queue_up_for_matching.set_upstream(validate_deid)
    detect_move_normalize_dag.set_upstream(
        [queue_up_for_matching, split_transactional_tests, split_transactional_results]
    )

    update_analytics_db.set_upstream(detect_move_normalize_dag)
else:
    detect_move_normalize_dag.set_upstream(
        [split_transactional_tests, split_transactional_results]
    )

split_transactional_tests.set_upstream(fetch_transactional_tests)
split_transactional_results.set_upstream(fetch_transactional_results)

clean_up_workspace.set_upstream(
    [split_transactional_tests, split_transactional_results]
)
