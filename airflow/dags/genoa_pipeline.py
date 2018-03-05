from airflow.operators import PythonOperator, SubDagOperator
from datetime import datetime, timedelta
from subprocess import check_call

import os
import re

# hv-specific modules
import common.HVDAG as HVDAG
import subdags.s3_validate_file as s3_validate_file
import subdags.s3_fetch_file as s3_fetch_file
import subdags.decrypt_files as decrypt_files
import subdags.split_push_files as split_push_files
import subdags.queue_up_for_matching as queue_up_for_matching
import subdags.detect_move_normalize as detect_move_normalize
import subdags.update_analytics_db as update_analytics_db

import util.decompression as decompression
import util.date_utils as date_utils

for m in [s3_validate_file, s3_fetch_file, decrypt_files,
          split_push_files, queue_up_for_matching,
          detect_move_normalize, decompression, HVDAG,
          update_analytics_db, date_utils]:
    reload(m)

# Applies to all files
TMP_PATH_TEMPLATE = '/tmp/genoa/pharmacyclaims/{}{}{}/'
DAG_NAME = 'genoa_pipeline'

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2018, 3, 1),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2)
}

mdag = HVDAG.HVDAG(
    dag_id=DAG_NAME,
    schedule_interval="0 0 1 * *",  # first of each month
    default_args=default_args
)

GENOA_MONTH_OFFSET = 1

# Applies to all transaction files
if HVDAG.HVDAG.airflow_env == 'test':
    S3_TRANSACTION_RAW_URL = 's3://salusv/testing/dewey/airflow/e2e/genoa/raw/'
    S3_TRANSACTION_PROCESSED_URL_TEMPLATE = 's3://salusv/testing/dewey/airflow/e2e/genoa/out/{}/{}/{}/transactions/'
    S3_PAYLOAD_DEST = 's3://salusv/testing/dewey/airflow/e2e/genoa/payload/'
else:
    S3_TRANSACTION_RAW_URL = 's3://healthverity/incoming/genoa/'
    S3_TRANSACTION_PROCESSED_URL_TEMPLATE = 's3://salusv/incoming/pharmacyclaims/genoa/{}/{}/{}/'
    S3_PAYLOAD_DEST = 's3://salusv/matching/payload/pharmacyclaims/genoa/'

# Zip file
ZIP_FILE_NAME_TEMPLATE = 'Genoa_HealthVerity_{}{}{}'

# Transaction file
TRANSACTION_S3_SPLIT_URL = S3_TRANSACTION_PROCESSED_URL_TEMPLATE
TRANSACTION_FILE_DESCRIPTION = 'Genoa transaction addon file'
TRANSACTION_FILE_NAME_TEMPLATE = 'Genoa_HealthVerity_DeID_Payload_{}{}{}'
MINIMUM_TRANSACTION_FILE_SIZE = 500

# Deid file
DEID_FILE_DESCRIPTION = 'Genoa deid file'
DEID_FILE_NAME_TEMPLATE = 'Genoa_HealthVerity_DeID_PHI_{}{}{}'
MINIMUM_DEID_FILE_SIZE = 500

get_tmp_dir = date_utils.generate_insert_date_into_template_function(
    TMP_PATH_TEMPLATE
)

get_tmp_unzipped_dir = date_utils.generate_insert_date_into_template_function(
    TMP_PATH_TEMPLATE + 'DeID_Output/'
)


def get_deid_file_urls(ds, kwargs):
    return [S3_TRANSACTION_RAW_URL +
            date_utils.insert_date_into_template(
                DEID_FILE_NAME_TEMPLATE,
                kwargs, month_offset=GENOA_MONTH_OFFSET
            )]


def get_files_that_match_pattern(file_path, file_prefix):
    def out(ds, kwargs):
        files = [
            f for f in os.listdir(file_path) if re.search(file_prefix, f)
        ]
        return files
    return out


def get_transaction_file_paths(ds, kwargs):
    file_dir = get_tmp_unzipped_dir(ds, kwargs)
    transaction_file = date_utils.insert_date_into_template(
        TRANSACTION_FILE_NAME_TEMPLATE,
        kwargs, month_offset=GENOA_MONTH_OFFSET
    )

    return [file_dir +
            get_files_that_match_pattern(file_dir,
                transaction_file
            )(ds, kwargs)[0]
            ]


def encrypted_decrypted_file_paths_function(ds, kwargs):
    file_dir = get_tmp_unzipped_dir(ds, kwargs)
    encrypted_filename_prefix = date_utils.insert_date_into_template(
        TRANSACTION_FILE_NAME_TEMPLATE, kwargs, month_offset=GENOA_MONTH_OFFSET
    )
    encrypted_file_path = file_dir \
        + get_files_that_match_pattern(file_dir, encrypted_filename_prefix)(ds, kwargs)[0]

    return [
        [encrypted_file_path, encrypted_file_path + '.gz']
    ]


def generate_file_validation_dag(
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
                    path_template + '_\d{{6}}',
                    month_offset=GENOA_MONTH_OFFSET
                ),
                'file_name_pattern_func': date_utils.generate_insert_regex_into_template_function(
                    path_template + '_\d{{6}}'
                ),
                'regex_name_match'        : True,
                'minimum_file_size'  : minimum_file_size,
                's3_prefix'          : '/'.join(S3_TRANSACTION_RAW_URL.split('/')[3:]),
                's3_bucket'          : 'healthverity',
                'file_description'   : 'Genoa ' + task_id + ' file'
            }
        ),
        task_id='validate_' + task_id + '_file',
        dag=mdag
    )


if HVDAG.HVDAG.airflow_env != 'test':
    validate_transaction = generate_file_validation_dag(
        'transaction', TRANSACTION_FILE_NAME_TEMPLATE,
        MINIMUM_TRANSACTION_FILE_SIZE
    )
    validate_deid = generate_file_validation_dag(
        'deid', DEID_FILE_NAME_TEMPLATE,
        MINIMUM_DEID_FILE_SIZE
    )

fetch_zip_file = SubDagOperator(
    subdag=s3_fetch_file.s3_fetch_file(
        DAG_NAME,
        'fetch_zip_file',
        default_args['start_date'],
        mdag.schedule_interval,
        {
            'tmp_path_template'      : TMP_PATH_TEMPLATE,
            'expected_file_name_func': date_utils.generate_insert_date_into_template_function(
                ZIP_FILE_NAME_TEMPLATE + '_\d{{6}}', month_offset=GENOA_MONTH_OFFSET
            ),
            'regex_name_match'        : True,
            's3_prefix'              : '/'.join(S3_TRANSACTION_RAW_URL.split('/')[3:]),
            's3_bucket'              : 'salusv' if HVDAG.HVDAG.airflow_env == 'test' else 'healthverity'
        }
    ),
    task_id='fetch_zip_file',
    dag=mdag
)


def do_unzip_file(task_id, file_name_template):
    def out(ds, **kwargs):
        file_name_prefix = date_utils.insert_date_into_template(
            file_name_template, kwargs, month_offset=GENOA_MONTH_OFFSET
        )

        tmp_dir = get_tmp_dir(ds, kwargs)
        file_name = get_files_that_match_pattern(tmp_dir, file_name_prefix)(ds, kwargs)[0]
        decompression.decompress_zip_file(
            tmp_dir + file_name, tmp_dir
        )
        os.remove(tmp_dir + file_name)

    return PythonOperator(
        task_id='unzip_file',
        provide_context=True,
        python_callable=out,
        dag=mdag
    )


unzip_transaction = do_unzip_file('transaction', ZIP_FILE_NAME_TEMPLATE)

decrypt_transaction = SubDagOperator(
    subdag=decrypt_files.decrypt_files(
        DAG_NAME,
        'decrypt_transaction_file',
        default_args['start_date'],
        mdag.schedule_interval,
        {
            'tmp_dir_func'                        : get_tmp_unzipped_dir,
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
            'tmp_dir_func'             : get_tmp_unzipped_dir,
            'file_paths_to_split_func' : get_transaction_file_paths,
            'file_name_pattern_func'   : date_utils.generate_insert_regex_into_template_function(
                TRANSACTION_FILE_NAME_TEMPLATE + '_\d{{6}}'
            ),
            's3_prefix_func'           : date_utils.generate_insert_date_into_template_function(
                TRANSACTION_S3_SPLIT_URL, month_offset=GENOA_MONTH_OFFSET
            ),
            'num_splits'               : 20
        }
    ),
    task_id='split_transaction_file',
    dag=mdag
)


def clean_up_workspace_step(template):
    def execute(ds, **kwargs):
        check_call([
            'rm', '-rf', get_tmp_unzipped_dir(ds, kwargs)
        ])
    return PythonOperator(
        task_id='clean_up_workspace',
        provide_context=True,
        python_callable=execute,
        trigger_rule='all_done',
        dag=mdag
    )


clean_up_workspace = clean_up_workspace_step(TMP_PATH_TEMPLATE)

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
    base = ['--date', date_utils.insert_date_into_template('{}-{}-{}', k, month_offset=GENOA_MONTH_OFFSET)]
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
            'expected_matching_files_func'      : lambda ds, k: [
                date_utils.insert_date_into_template(
                    DEID_FILE_NAME_TEMPLATE,
                    k,
                    month_offset=GENOA_MONTH_OFFSET
                )
            ],
            'file_date_func'                    : date_utils.generate_insert_date_into_template_function(
                '{}/{}/{}', month_offset=GENOA_MONTH_OFFSET
            ),
            's3_payload_loc_url'                : S3_PAYLOAD_DEST,
            'vendor_uuid'                       : '8ffacdf6-989e-46ac-93e6-6bb3559baa36',
            'pyspark_normalization_script_name' : '/home/hadoop/spark/providers/genoa/pharmacyclaims/sparkNormalizeGenoaRX.py',
            'pyspark_normalization_args_func'   : norm_args,
            'pyspark'                           : True
        }
    ),
    task_id='detect_move_normalize',
    dag=mdag
)

repair_table = "MSCK REPAIR TABLE pharmacyclaims_20180205"

if HVDAG.HVDAG.airflow_env != 'test':
    update_analytics_db = SubDagOperator(
        subdag=update_analytics_db.update_analytics_db(
            DAG_NAME,
            'update_analytics_db',
            default_args['start_date'],
            mdag.schedule_interval,
            {
                'sql_command_func' : lambda ds, k: repair_table
            }
        ),
        task_id='update_analytics_db',
        dag=mdag
    )


if HVDAG.HVDAG.airflow_env != 'test':
    fetch_zip_file.set_upstream(validate_transaction)

    # matching
    queue_up_for_matching.set_upstream(validate_deid)

    # post-matching
    detect_move_normalize_dag.set_upstream(
        [queue_up_for_matching, split_transaction]
    )
    update_analytics_db.set_upstream(detect_move_normalize_dag)
else:
    detect_move_normalize_dag.set_upstream(split_transaction)

unzip_transaction.set_upstream(fetch_zip_file)
decrypt_transaction.set_upstream(unzip_transaction)
split_transaction.set_upstream(decrypt_transaction)

# cleanup
clean_up_workspace.set_upstream(split_transaction)
