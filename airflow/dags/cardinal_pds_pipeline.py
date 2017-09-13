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
import subdags.decrypt_files as decrypt_files
import subdags.split_push_files as split_push_files
import subdags.queue_up_for_matching as queue_up_for_matching
import subdags.detect_move_normalize as detect_move_normalize
import subdags.clean_up_tmp_dir as clean_up_tmp_dir

for m in [s3_validate_file, s3_fetch_file, decrypt_files,
          split_push_files, queue_up_for_matching, clean_up_tmp_dir,
          detect_move_normalize, HVDAG]:
    reload(m)

# Applies to all files
DAG_NAME = 'cardinal_pds_pipeline'
TMP_PATH_TEMPLATE = '/tmp/cardinal_pds/pharmacyclaims/{}/'

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2017, 8, 25), # Unclear when this is going to start
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2)
}

mdag = HVDAG.HVDAG(
    dag_id = DAG_NAME,
    schedule_interval = '@weekly',
    default_args = default_args
)

if HVDAG.HVDAG.airflow_env == 'test':
    S3_TRANSACTION_RAW_URL = 's3://salusv/testing/dewey/airflow/e2e/cardinal_pds/pharmacyclaims/raw/'
    S3_TRANSACTION_PROCESSED_URL_TEMPLATE = 's3://salusv/testing/dewey/airflow/e2e/cardinal_pds/pharmacyclaims/out/{}/{}/{}/'
    S3_PAYLOAD_DEST = 's3://salusv/testing/dewey/airflow/e2e/cardinal_pds/pharmacyclaims/payload/'
else:
    S3_TRANSACTION_RAW_URL = 's3://hvincoming/cardinal_raintree/pds/'
    S3_TRANSACTION_PROCESSED_URL_TEMPLATE = 's3://salusv/incoming/pharmacyclaims/cardinal_pds/{}/{}/{}/'
    S3_PAYLOAD_DEST = 's3://salusv/matching/payload/pharmacyclaims/cardinal_pds/'

# Transaction Addon file
TRANSACTION_TMP_PATH_TEMPLATE = TMP_PATH_TEMPLATE + 'raw/'
TRANSACTION_FILE_DESCRIPTION = 'Cardinal PDS RX transaction file'
TRANSACTION_FILE_NAME_TEMPLATE = 'PDS_record_data_{}'

# Deid file
DEID_FILE_DESCRIPTION = 'Cardinal PDS RX deid file'
DEID_FILE_NAME_TEMPLATE = 'PDS_deid_data_{}'

def get_formatted_date(ds, kwargs):
    return kwargs['ds_nodash']


def insert_formatted_date_function(template):
    def out(ds, kwargs):
        return template.format(get_formatted_date(ds, kwargs))
    return out

def get_formatted_datetime(ds, kwargs):
    return kwargs['ti'].xcom_pull(dag_id = DAG_NAME, task_ids = 'get_datetime', key = 'file_datetime')


def insert_formatted_datetime_function(template):
    def out(ds, kwargs):
        return template.format(get_formatted_datetime(ds, kwargs))
    return out


def insert_todays_date_function(template):
    def out(ds, kwargs):
        return template.format(kwargs['ds_nodash'])
    return out


def insert_formatted_regex_function(template):
    def out(ds, kwargs):
        return template.format(kwargs['ds_nodash'] + '\d{6}')
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
        get_formatted_datetime(ds, kwargs)
    )]


def get_deid_file_urls(ds, kwargs):
    return [S3_TRANSACTION_RAW_URL + DEID_FILE_NAME_TEMPLATE.format(
        get_formated_datetime(ds, kwargs)
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
                'expected_file_name_func' : insert_formatted_date_function(
                    path_template
                ),
                'file_name_pattern_func'  : insert_formatted_regex_function(
                    path_template
                ),
                'minimum_file_size'       : minimum_file_size,
                's3_prefix'               : '/'.join(S3_TRANSACTION_RAW_URL.split('/')[3:]),
                's3_bucket'               : 'hvincoming',
                'file_description'        : 'Cardinal PDS RX ' + task_id + ' file'
            }
        ),
        task_id = 'validate_' + task_id + '_file',
        dag = mdag
    )


if HVDAG.HVDAG.airflow_env != 'test':
    validate_transaction = generate_file_validation_task(
        'transaction', TRANSACTION_FILE_NAME_TEMPLATE,
        1000000
    )
    validate_deid = generate_file_validation_task(
        'deid', DEID_FILE_NAME_TEMPLATE,
        1000000
    )

fetch_transaction = SubDagOperator(
    subdag = s3_fetch_file.s3_fetch_file(
        DAG_NAME,
        'fetch_transaction_file',
        default_args['start_date'],
        mdag.schedule_interval,
        {
            'tmp_path_template'         : TRANSACTION_TMP_PATH_TEMPLATE,
            'expected_file_name_func'   : insert_formatted_regex_function(
                TRANSACTION_FILE_NAME_TEMPLATE
            ),
            'regex_name_match'          : True,
            's3_prefix'                 : '/'.join(S3_TRANSACTION_RAW_URL.split('/')[3:]),
            's3_bucket'                 : 'salusv' if HVDAG.HVDAG.airflow_env == 'test' else 'hvincoming'
        }
    ),
    task_id = 'fetch_transaction_file',
    dag = mdag
)

def do_get_datetime(ds, **kwargs):
    expected_filename = kwargs['expected_file_name_func'](ds, kwargs)
    files = os.listdir(TRANSACTION_TMP_PATH_TEMPLATE.format(kwargs['ds_nodash']))

    expected_filename = filter(lambda k: re.search(expected_filename, k), files)[0]
    file_datetime = expected_filename[-14:]
    print 'file datetime: ' + file_datetime
    kwargs['ti'].xcom_push(key = 'file_datetime', value = file_datetime)


get_datetime = PythonOperator(
    task_id = 'get_datetime',
    python_callable = do_get_datetime,
    op_kwargs = {
        'expected_file_name_func' : insert_formatted_regex_function(
            TRANSACTION_FILE_NAME_TEMPLATE
        )
    },
    provide_context = True,
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
            'expected_matching_files_func'      : get_deid_file_urls,
            'file_date_func'                    : insert_current_date_function(
                '{}/{}/{}'
            ),
            's3_payload_loc_url'                : S3_PAYLOAD_DEST,
            'vendor_uuid'                       : 'cddbdc93-c3cf-42a0-915b-605333639602',
            'pyspark_normalization_script_name' : '/home/hadoop/spark/providers/cardinal_pds/pharmacyclaims/sparkNormalizeCardinalRx.py',
            'pyspark_normalization_args_func'   : norm_args,
            'pyspark'                           : True
        }
    ),
    task_id='detect_move_normalize',
    dag=mdag
)

### Dag Structure ###
if HVDAG.HVDAG.airflow_env != 'test':
    fetch_transaction.set_upstream(validate_transaction)
    queue_up_for_matching.set_upstream(validate_deid)

    detect_move_normalize_dag.set_upstream(
        [queue_up_for_matching, split_transaction]
    )
else:
    detect_move_normalize_dag.set_upstream(split_transaction)
    
get_datetime.set_upstream(fetch_transaction)
decrypt_transaction.set_upstream(get_datetime)
split_transaction.set_upstream(decrypt_transaction)
clean_up_workspace.set_upstream(split_transaction)
