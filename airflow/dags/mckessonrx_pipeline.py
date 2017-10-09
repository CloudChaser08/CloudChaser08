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

for m in [s3_validate_file, s3_fetch_file, decrypt_files,
          split_push_files, queue_up_for_matching, clean_up_tmp_dir,
          detect_move_normalize, decompression, HVDAG,
          update_analytics_db]:
    reload(m)

# Applies to all files
TMP_PATH_TEMPLATE = '/tmp/mckesson/pharmacyclaims/{}/'
DAG_NAME = 'mckessonrx_pipeline'

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2017, 6, 5, 12),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2)
}

mdag = HVDAG.HVDAG(
    dag_id=DAG_NAME,
    schedule_interval="0 12 * * *",
    default_args=default_args
)

# Applies to all transaction files
if HVDAG.HVDAG.airflow_env == 'test':
    S3_TRANSACTION_RAW_URL = 's3://salusv/testing/dewey/airflow/e2e/mckesson/pharmacyclaims/raw/'
    S3_TRANSACTION_PROCESSED_URL_TEMPLATE = 's3://salusv/testing/dewey/airflow/e2e/mckesson/pharmacyclaims/out/{}/{}/{}/'
    S3_PAYLOAD_DEST = 's3://salusv/testing/dewey/airflow/e2e/mckesson/pharmacyclaims/payload/'
else:
    S3_TRANSACTION_RAW_URL = 's3://healthverity/incoming/mckessonrx/'
    S3_TRANSACTION_PROCESSED_URL_TEMPLATE = 's3://salusv/incoming/pharmacyclaims/mckesson/{}/{}/{}/'
    S3_PAYLOAD_DEST = 's3://salusv/matching/payload/pharmacyclaims/mckesson/'

# Transaction Addon file
TRANSACTION_TMP_PATH_TEMPLATE = TMP_PATH_TEMPLATE + 'raw/'
TRANSACTION_FILE_DESCRIPTION = 'McKessonRX transaction file'
TRANSACTION_FILE_NAME_TEMPLATE = 'HVUnRes.Record.{}'

# Deid file
DEID_FILE_DESCRIPTION = 'McKessonRX deid file'
DEID_FILE_NAME_TEMPLATE = 'HVUnRes.DEID.{}'


def get_formatted_date(ds, kwargs):
    return kwargs['ds_nodash']


def insert_formatted_date_function(template):
    def out(ds, kwargs):
        return template.format(get_formatted_date(ds, kwargs))
    return out


def insert_todays_date_function(template):
    def out(ds, kwargs):
        return template.format(kwargs['ds_nodash'])
    return out


def insert_formatted_regex_function(template):
    def out(ds, kwargs):
        return template.format('\d{8}')
    return out


def insert_current_date(template, kwargs):
    return template.format(
        kwargs['ds_nodash'][0:4],
        kwargs['ds_nodash'][4:6],
        kwargs['ds_nodash'][6:8]
    )


def insert_current_date_function(template):
    def out(ds, kwargs):
        return insert_current_date(template, kwargs)
    return out


get_tmp_dir = insert_todays_date_function(TRANSACTION_TMP_PATH_TEMPLATE)


def get_transaction_file_paths(ds, kwargs):
    return [get_tmp_dir(ds, kwargs) + TRANSACTION_FILE_NAME_TEMPLATE.format(
        get_formatted_date(ds, kwargs)
    )]


def get_deid_file_urls(ds, kwargs):
    return [S3_TRANSACTION_RAW_URL + DEID_FILE_NAME_TEMPLATE.format(
        get_formatted_date(ds, kwargs)
    )]


def encrypted_decrypted_file_paths_function(ds, kwargs):
    file_dir = get_tmp_dir(ds, kwargs)
    encrypted_file_path = file_dir \
        + TRANSACTION_FILE_NAME_TEMPLATE.format(
            get_formatted_date(ds, kwargs)
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
                'expected_file_name_func' : insert_formatted_date_function(
                    path_template
                ),
                'file_name_pattern_func'  : insert_formatted_regex_function(
                    path_template
                ),
                'minimum_file_size'       : minimum_file_size,
                's3_prefix'               : '/'.join(S3_TRANSACTION_RAW_URL.split('/')[3:]),
                's3_bucket'               : 'healthverity',
                'file_description'        : 'McKessonRX ' + task_id + ' file'
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
            'expected_file_name_func': insert_formatted_date_function(
                TRANSACTION_FILE_NAME_TEMPLATE
            ),
            's3_prefix'              : '/'.join(S3_TRANSACTION_RAW_URL.split('/')[3:]),
            's3_bucket'              : 'salusv' if HVDAG.HVDAG.airflow_env == 'test' else 'healthverity'
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
            'file_name_pattern_func'   : insert_formatted_regex_function(
                TRANSACTION_FILE_NAME_TEMPLATE
            ),
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

if HVDAG.HVDAG.airflow_env != 'test':
    queue_up_for_matching = SubDagOperator(
        subdag=queue_up_for_matching.queue_up_for_matching(
            DAG_NAME,
            'queue_up_for_matching',
            default_args['start_date'],
            mdag.schedule_interval,
            {
                'source_files_func' : get_deid_file_urls
            }
        ),
        task_id='queue_up_for_matching',
        dag=mdag
    )

#
# Post-Matching
#
def norm_args(ds, k):
    base = ['--date', insert_current_date('{}-{}-{}', k), '--unrestricted']
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
            'expected_matching_files_func'      : lambda ds,k: [
                insert_formatted_date_function(
                    DEID_FILE_NAME_TEMPLATE
                )(ds, k)
            ],
            'file_date_func'                    : insert_current_date_function(
                '{}/{}/{}'
            ),
            's3_payload_loc_url'                : S3_PAYLOAD_DEST,
            'vendor_uuid'                       : 'f6b4eefd-988a-4ca3-9efd-0140607c8985',
            'pyspark_normalization_script_name' : '/home/hadoop/spark/providers/mckesson/pharmacyclaims/sparkNormalizeMcKessonRx.py',
            'pyspark_normalization_args_func'   : norm_args,
            'pyspark'                           : True
        }
    ),
    task_id='detect_move_normalize',
    dag=mdag
)

sql_template = """
    ALTER TABLE pharmacyclaims_20170602 ADD PARTITION (part_provider='mckesson', part_best_date='{0}-{1}')
    LOCATION 's3a://salusv/warehouse/parquet/pharmacyclaims/2017-06-02/part_provider=mckesson/part_best_date={0}-{1}/'
"""

if HVDAG.HVDAG.airflow_env != 'test':
    update_analytics_db = SubDagOperator(
        subdag=update_analytics_db.update_analytics_db(
            DAG_NAME,
            'update_analytics_db',
            default_args['start_date'],
            mdag.schedule_interval,
            {
                'sql_command_func' : lambda ds, k: insert_current_date(sql_template, k)
                if insert_current_date('{}-{}-{}', k).find('-01') == 7 else ''
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
