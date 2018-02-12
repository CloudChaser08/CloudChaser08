from airflow.models import Variable
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
import subdags.detect_move_normalize as detect_move_normalize
import subdags.update_analytics_db as update_analytics_db
import subdags.clean_up_tmp_dir as clean_up_tmp_dir

import util.decompression as decompression
import util.date_utils as date_utils

for m in [s3_validate_file, s3_fetch_file, decrypt_files,
          split_push_files, queue_up_for_matching, clean_up_tmp_dir,
          detect_move_normalize, decompression, HVDAG,
          update_analytics_db, date_utils]:
    reload(m)

# Applies to all files
TMP_PATH_TEMPLATE = '/tmp/obit/consumer/{}{}{}/'
DAG_NAME = 'obituary_data_pipeline'

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2017, 10, 23),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2)
}

mdag = HVDAG.HVDAG(
    dag_id=DAG_NAME,
    schedule_interval="0 0 1 1,4,7,10 *",
    default_args=default_args
)

OBIT_MONTH_OFFSET = 3

# Applies to all transaction files
if HVDAG.HVDAG.airflow_env == 'test':
    S3_TRANSACTION_RAW_URL = 's3://salusv/testing/dewey/airflow/e2e/obituarydata/event/raw/'
    S3_TRANSACTION_PROCESSED_URL_TEMPLATE = 's3://salusv/testing/dewey/airflow/e2e/obituarydata/event/out/{}/{}/{}/'
    S3_PAYLOAD_DEST = 's3://salusv/testing/dewey/airflow/e2e/obituarydata/event/payload/'
else:
    S3_TRANSACTION_RAW_URL = 's3://healthverity/incoming/obituarydata/'
    S3_TRANSACTION_PROCESSED_URL_TEMPLATE = 's3://salusv/incoming/consumer/obituarydata/{}/{}/{}/'
    S3_PAYLOAD_DEST = 's3://salusv/matching/payload/consumer/obituarydata/'

# Transaction Addon file
TRANSACTION_TMP_PATH_TEMPLATE = TMP_PATH_TEMPLATE + 'raw/'
TRANSACTION_FILE_DESCRIPTION = 'ObituaryData transaction file'
TRANSACTION_FILE_NAME_TEMPLATE = 'OD_record_data_{}{}{}_'

# Deid file
DEID_FILE_DESCRIPTION = 'ObituaryData deid file'
DEID_FILE_NAME_TEMPLATE = 'OD_deid_data_{}{}{}_'

get_tmp_dir = date_utils.generate_insert_date_into_template_function(
    TRANSACTION_TMP_PATH_TEMPLATE
)


def get_transaction_file_paths(ds, kwargs):
    return [get_tmp_dir(ds, kwargs) +\
        date_utils.insert_date_into_template(TRANSACTION_FILE_NAME_TEMPLATE, 
            kwargs, month_offset = OBIT_MONTH_OFFSET)
    ]


def get_deid_file_urls(ds, kwargs):
    return [
        date_utils.insert_date_into_template(DEID_FILE_NAME_TEMPLATE, 
            kwargs, month_offset = OBIT_MONTH_OFFSET)
    ]


def encrypted_decrypted_file_paths_function(ds, kwargs):
    file_dir = get_tmp_dir(ds, kwargs)
    encrypted_file_path = file_dir \
        + date_utils.insert_date_into_template(
            TRANSACTION_FILE_NAME_TEMPLATE, 
            kwargs,
            month_offset = OBIT_MONTH_OFFSET
        )
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
                'expected_file_name_func' : date_utils.generate_insert_date_into_template_function(
                    path_template, month_offset = OBIT_MONTH_OFFSET
                ),
                'file_name_pattern_func'  : date_utils.generate_insert_regex_into_template_function(
                    path_template
                ),
                'minimum_file_size'       : minimum_file_size,
                's3_prefix'               : '/'.join(S3_TRANSACTION_RAW_URL.split('/')[3:]),
                's3_bucket'               : 'healthverity',
                'file_description'        : 'Obituary ' + task_id + ' file'
            }
        ),
        task_id='validate_' + task_id + '_file',
        dag=mdag
    )


if HVDAG.HVDAG.airflow_env != 'test':
    validate_transaction = generate_file_validation_task(
        'transaction', TRANSACTION_FILE_NAME_TEMPLATE,
        1000000
    )
    validate_deid = generate_file_validation_task(
        'deid', DEID_FILE_NAME_TEMPLATE,
        10000000
    )

fetch_transaction = SubDagOperator(
    subdag=s3_fetch_file.s3_fetch_file(
        DAG_NAME,
        'fetch_transaction_file',
        default_args['start_date'],
        mdag.schedule_interval,
        {
            'tmp_path_template'      : TRANSACTION_TMP_PATH_TEMPLATE,
            'expected_file_name_func': date_utils.generate_insert_date_into_template_function(
                TRANSACTION_FILE_NAME_TEMPLATE, month_offset = OBIT_MONTH_OFFSET
            ),
            's3_prefix'              : '/'.join(S3_TRANSACTION_RAW_URL.split('/')[3:]),
            's3_bucket'              : S3_TRANSACTION_RAW_URL.split('/')[2]
        }
    ),
    task_id='fetch_transaction_file',
    dag=mdag
)


decrypt_transaction = SubDagOperator(
    subdag=decrypt_files.decrypt_files(
        DAG_NAME,
        'decrypt_transaction_file',
        default_args['start_date'],
        mdag.schedule_interval,
        {
            'tmp_dir_func'                        : get_tmp_dir,
            'encrypted_decrypted_file_paths_func' : encrypted_decrypted_file_paths_function
        }
    ),
    task_id='decrypt_transaction_file',
    dag=mdag
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
            'file_name_pattern_func'   : 
                date_utils.generate_insert_regex_into_template_function(
                    TRANSACTION_FILE_NAME_TEMPLATE
                ),
            's3_prefix_func'           : 
                date_utils.generate_insert_date_into_template_function(
                    S3_TRANSACTION_PROCESSED_URL_TEMPLATE,
                    month_offset = OBIT_MONTH_OFFSET
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

if HVDAG.HVDAG.airflow_env != 'test':
    queue_up_for_matching = SubDagOperator(
        subdag=queue_up_for_matching.queue_up_for_matching(
            DAG_NAME,
            'queue_up_for_matching',
            default_args['start_date'],
            mdag.schedule_interval,
            {
                'source_files_func' : lambda ds, k: [
                    S3_TRANSACTION_RAW_URL + f for f in get_deid_file_urls(ds, k)
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
    base = ['--date', date_utils.insert_date_into_template('{}-{}-{}', k, month_offset = OBIT_MONTH_OFFSET)]
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
            'expected_matching_files_func'      : get_deid_file_urls,
            'file_date_func'                    : 
                date_utils.generate_insert_date_into_template_function('{}/{}/{}', month_offset = OBIT_MONTH_OFFSET),
            's3_payload_loc_url'                : S3_PAYLOAD_DEST,
            'vendor_uuid'                       : 'a6d6b232-a822-49ab-88be-01698901e787',
            'pyspark_normalization_script_name' : '/home/hadoop/spark/providers/obituarydata/sparkNormalizeObituaryData.py',
            'pyspark_normalization_args_func'   : norm_args,
            'pyspark'                           : True
        }
    ),
    task_id='detect_move_normalize',
    dag=mdag
)

sql_template = """
    MSCK REPAIR TABLE events_old
"""

if HVDAG.HVDAG.airflow_env != 'test':
    update_analytics_db = SubDagOperator(
        subdag=update_analytics_db.update_analytics_db(
            DAG_NAME,
            'update_analytics_db',
            default_args['start_date'],
            mdag.schedule_interval,
            {
                'sql_command_func' : lambda ds, k: sql_template
            }
        ),
        task_id='update_analytics_db',
        dag=mdag
    )


# addon
if HVDAG.HVDAG.airflow_env != 'test':
    fetch_transaction.set_upstream(validate_transaction)
    queue_up_for_matching.set_upstream(validate_deid)

    detect_move_normalize_dag.set_upstream(
        [queue_up_for_matching, split_transaction]
    )
    update_analytics_db.set_upstream(detect_move_normalize_dag)
else:
    detect_move_normalize_dag.set_upstream(split_transaction)

decrypt_transaction.set_upstream(fetch_transaction)
split_transaction.set_upstream(decrypt_transaction)

# cleanup
clean_up_workspace.set_upstream(split_transaction)
