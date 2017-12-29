from airflow.operators import SubDagOperator
from datetime import datetime, timedelta

# hv-specific modules
import common.HVDAG as HVDAG
import subdags.s3_validate_file as s3_validate_file
import subdags.s3_fetch_file as s3_fetch_file
import subdags.decrypt_files as decrypt_files
import subdags.split_push_files as split_push_files
import subdags.queue_up_for_matching as queue_up_for_matching
import subdags.detect_move_normalize as detect_move_normalize
import subdags.clean_up_tmp_dir as clean_up_tmp_dir

import util.decompression as decompression
import util.date_utils as date_utils

for m in [s3_validate_file, s3_fetch_file, decrypt_files,
          split_push_files, queue_up_for_matching, clean_up_tmp_dir,
          detect_move_normalize, decompression, date_utils, HVDAG]:
    reload(m)

# Applies to all files
TMP_PATH_TEMPLATE = '/tmp/cardinal_rcm/medicalclaims/{}{}{}/'
DAG_NAME = 'cardinal_pms_dx_pipeline'

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2017, 6, 5, 14),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2)
}

mdag = HVDAG.HVDAG(
    dag_id = DAG_NAME,
    schedule_interval = '0 14 * * *',
    default_args = default_args
)

# Applies to all transaction files
if HVDAG.HVDAG.airflow_env == 'test':
    S3_TRANSACTION_RAW_URL = 's3://salusv/testing/dewey/airflow/e2e/cardinal_pms/medicalclaims/raw/'
    S3_TRANSACTION_PROCESSED_URL_TEMPLATE = 's3://salusv/testing/dewey/airflow/e2e/cardinal_pms/medicalclaims/out/{}/{}/{}/'
else:
    S3_TRANSACTION_RAW_URL = 's3://healthverity/incoming/cardinal/'
    S3_TRANSACTION_PROCESSED_URL_TEMPLATE = 's3://salusv/incoming/medicalclaims/cardinal_pms/medicalclaims/out/{}/{}/{}'

# Transaction Addon file
TRANSACTION_TMP_PATH_TEMPLATE = TMP_PATH_TEMPLATE + 'raw/'
TRANSACTION_FILE_DESCRIPTION = 'Cardinal PMS DX transaction file'
TRANSACTION_FILE_NAME_TEMPLATE = '837.{}{}{}.dat'
S3_PAYLOAD_DEST = 's3://salusv/matching/payload/medicalclaims/cardinal_pms/'

get_tmp_dir = date_utils.generate_insert_date_into_template_function(TRANSACTION_TMP_PATH_TEMPLATE)


def get_transaction_file_paths(ds, kwargs):
    return [get_tmp_dir(ds, kwargs) +\
        date_utils.insert_date_into_template(TRANSACTION_FILE_NAME_TEMPLATE,kwargs)
    ]

def encrypted_decrypted_file_paths_function(ds, kwargs):
    file_dir = get_tmp_dir(ds, kwargs)
    encrypted_file_path = file_dir \
        + date_utils.insert_date_into_template(
            TRANSACTION_FILE_NAME_TEMPLATE,
            kwargs
        )
    return [
        [encrypted_file_path, encrypted_file_path + '.gz']
    ]


if HVDAG.HVDAG.airflow_env != 'test':
    validate_transaction = s3_validate_file.s3_validate_file(
        DAG_NAME,
        'validate_transaction_file',
        default_args['start_date'],
        mdag.schedule_interval,
        {
            'expected_file_name_func' : date_utils.generate_insert_date_into_template_function(
                TRANSACTION_FILE_NAME_TEMPLATE
            ),
            'file_name_pattern_func'  : date_utils.generate_insert_regex_into_template_function(
                TRANSACTION_FILE_NAME_TEMPLATE
            ),
            'minimum_file_size'       : 1000000,
            's3_prefix'               : '/'.join(S3_TRANSACTION_RAW_URL.split('/')[3:]),
            's3_bucket'               : 'healthverity',
            'file_description'        : 'Cardinal RCM DX ' + task_id + ' file'
        }
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
            's3_prefix_func'           : date_utils.generate_insert_date_into_template_function(
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

#
# Post-Matching
#
def norm_args(ds, k):
    base = ['--date', date_utils.insert_date_into_template('{}-{}-{}', k)]
    if HVDAG.HVDAG.airflow_env == 'test':
        base += ['--airflow_test']

    return base


#TODO: Create one w/o needing DeID file
detect_move_normalize_dag = SubDagOperator(
    subdag=detect_move_normalize.detect_move_normalize(
        DAG_NAME,
        'detect_move_normalize',
        default_args['start_date'],
        mdag.schedule_interval,
        {
            'expected_matching_files_func'      : lambda ds,k: [
                date_utils.generate_insert_date_into_template_function(
                    DEID_FILE_NAME_TEMPLATE
                )(ds, k)
            ],
            'file_date_func'                    : date_utils.generate_insert_date_into_template_function(
                '{}/{}/{}'
            ),
            's3_payload_loc_url'                : S3_PAYLOAD_DEST,
            'vendor_uuid'                       : 'c0110047-c269-49e7-b7f3-a2109179d4e4',
            'pyspark_normalization_script_name' : '/home/hadoop/spark/providers/cardinal_pms/medicalclaims/sparkNormalizeCardinalPMS.py',
            'pyspark_normalization_args_func'   : norm_args,
            'pyspark'                           : True,
            'no_matching_payload'               : True
        }
    ),
    task_id='detect_move_normalize',
    dag=mdag
)

# TODO: Update analytics database

### DAG Structure ###
if HVDAG.HVDAG.airflow_env != 'test':
    fetch_transaction.set_upstream(validate_transaction)

decrypt_transaction.set_upstream(fetch_transaction)
split_transaction.set_upstream(decrypt_transaction)
detect_move_normalize_dag.set_upstream(split_transaction)

# cleanup
clean_up_workspace.set_upstream(split_transaction)
