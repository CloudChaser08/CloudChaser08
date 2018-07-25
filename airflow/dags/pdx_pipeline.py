from airflow.operators import SubDagOperator
from datetime import datetime, timedelta
import os
import re
import logging

#hv-specific modules
import common.HVDAG as HVDAG
import subdags.s3_validate_file as s3_validate_file
import subdags.s3_fetch_file as s3_fetch_file
import subdags.decrypt_files as decrypt_files
import subdags.detect_move_normalize as detect_move_normalize
import subdags.queue_up_for_matching as queue_up_for_matching
import subdags.split_push_files as split_push_files
import subdags.clean_up_tmp_dir as clean_up_tmp_dir 
import subdags.update_analytics_db as update_analytics_db
import util.date_utils as date_utils

for m in [HVDAG, s3_validate_file, s3_fetch_file, detect_move_normalize,
          queue_up_for_matching, split_push_files, update_analytics_db,
          date_utils, clean_up_tmp_dir]:
    reload(m)

DAG_NAME = 'pdx_pipeline'

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2018, 6, 4),
    'end_date': datetime(2018, 6, 11),
    'retries': 3,
    'retry_delay': timedelta(minutes=2)
}

mdag = HVDAG.HVDAG(
    dag_id = DAG_NAME,
    schedule_interval = '0 11 * * 1',       #TODO: figure out
    default_args = default_args
)

PDX_DAY_OFFSET = 7

if HVDAG.HVDAG.airflow_env == 'test':
    test_loc = 's3://salusv/testing/dewey/airflow/e2e/pdx/'
    S3_TRANSACTION_RAW_URL = test_loc + 'raw/'
    S3_TRANSACTION_PROCESSED_URL_TEMPLATE = test_loc + 'out/{}/{}/{}/'
    S3_PAYLOAD_DEST = test_loc + 'payload/'
else:
    S3_TRANSACTION_RAW_URL = 's3://healthverity/incoming/pdx/'
    S3_TRANSACTION_PROCESSED_URL_TEMPLATE = 's3://salusv/incoming/pharmacyclaims/pdx/{}/{}/{}/'
    S3_PAYLOAD_DEST = 's3://salusv/matching/payload/pharmacyclaims/pdx/'

TMP_PATH_TEMPLATE = '/tmp/pdx/pharmacyclaims/{}{}{}/'
TRANSACTION_TMP_PATH_TEMPLATE = TMP_PATH_TEMPLATE + 'raw/transactions/'

TRANSACTION_FILE_NAME_TEMPLATE = 'hvfeedfile_po_record_deid_{}{}{}\d{{6}}.hvout$'
DEID_FILE_NAME_TEMPLATE = 'hvfeedfile_header_deid_{}{}{}\d{{6}}.hvout'

get_tmp_dir = date_utils.generate_insert_date_into_template_function(
    TRANSACTION_TMP_PATH_TEMPLATE
)

def get_transaction_file_paths(ds, kwargs):
    file_dir = get_tmp_dir(ds, kwargs)
    files = os.listdir(file_dir)
    logging.info("File dir: {}".format(file_dir))
    logging.info("Files found: {}".format(str(files)))
    transaction_files = filter(lambda x:
                               re.search(
                                   date_utils.insert_date_into_template(
                                       TRANSACTION_FILE_NAME_TEMPLATE,
                                       kwargs,
                                       day_offset=PDX_DAY_OFFSET
                                   ),
                                   x
                               ),
                               files
                              )
    logging.info("Filtered files: {}".format(str(transaction_files)))
    return map(lambda x: file_dir + x, transaction_files) 


def encrypted_decrypted_file_paths_function(ds, kwargs):
    file_dir = get_tmp_dir(ds, kwargs)
    files = os.listdir(file_dir)
    logging.info("File dir: {}".format(file_dir))
    logging.info("Files found: {}".format(str(files)))
    transaction_files = filter(lambda x:
                               re.search(
                                   date_utils.insert_date_into_template(
                                       TRANSACTION_FILE_NAME_TEMPLATE,
                                       kwargs,
                                       day_offset=PDX_DAY_OFFSET
                                   ),
                                   x
                               ),
                               files
                              )
    logging.info("Filtered files: {}".format(str(transaction_files)))
    return map(lambda x: [file_dir + x, file_dir + x + '.gz'], transaction_files)


def generate_file_validation_task(
        task_id, s3_path, path_template, minimum_file_size
):
    return SubDagOperator(
        subdag=s3_validate_file.s3_validate_file(
            DAG_NAME,
            'validate_' + task_id + '_file',
            default_args['start_date'],
            mdag.schedule_interval,
            {
                'expected_file_name_func' : date_utils.generate_insert_date_into_template_function(
                    path_template, 
                    day_offset = PDX_DAY_OFFSET
                ),
                'file_name_pattern_func'  : date_utils.generate_insert_regex_into_template_function(
                    path_template
                ),
                'regex_name_match'        : True,
                'minimum_file_size'       : minimum_file_size,
                's3_prefix'               : '/'.join(s3_path.split('/')[3:]),
                's3_bucket'               : 'healthverity',
                'file_description'        : 'PDX ' + task_id + ' file'          #TODO: update this if it's a manifest or not
            }
        ),
        task_id='validate_' + task_id + '_file',
        dag=mdag
    )


if HVDAG.HVDAG.airflow_env != 'test':
    validate_transaction = generate_file_validation_task(
        'transaction', S3_TRANSACTION_RAW_URL,
        TRANSACTION_FILE_NAME_TEMPLATE, 1000000
    )
    validate_deid = generate_file_validation_task(
        'deid', S3_TRANSACTION_RAW_URL,
        DEID_FILE_NAME_TEMPLATE, 1000000
    )

    queue_up_for_matching = SubDagOperator(
        subdag=queue_up_for_matching.queue_up_for_matching(
            DAG_NAME,
            'queue_up_for_matching',
            default_args['start_date'],
            mdag.schedule_interval,
            {
                'source_files_func' : lambda ds, k: [
                    S3_TRANSACTION_RAW_URL + date_utils.insert_date_into_template(
                        DEID_FILE_NAME_TEMPLATE, k, day_offset=PDX_DAY_OFFSET
                    )
                ],
                'regex_name_match'  : True
            }
        ),
        task_id='queue_up_for_matching',
        dag=mdag
    )

fetch_transaction_file = SubDagOperator(
    subdag=s3_fetch_file.s3_fetch_file(
        DAG_NAME,
        'fetch_transaction_file',
        default_args['start_date'],
        mdag.schedule_interval,
        {
            'tmp_path_template'      : TRANSACTION_TMP_PATH_TEMPLATE,
            'expected_file_name_func': date_utils.generate_insert_date_into_template_function(
                TRANSACTION_FILE_NAME_TEMPLATE,
                day_offset = PDX_DAY_OFFSET
            ),
            'regex_name_match'       : True,
            'multi_match'            : True,
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


split_transaction_file = SubDagOperator(
    subdag=split_push_files.split_push_files(
        DAG_NAME,
        'split_transaction_file',
        default_args['start_date'],
        mdag.schedule_interval,
        {
            'tmp_dir_func'             : get_tmp_dir,
            'file_paths_to_split_func' : get_transaction_file_paths,
            'file_name_pattern_func'   : date_utils.generate_insert_regex_into_template_function(
                TRANSACTION_FILE_NAME_TEMPLATE
            ),
            's3_prefix_func'           : date_utils.generate_insert_date_into_template_function(
                S3_TRANSACTION_PROCESSED_URL_TEMPLATE, 
                day_offset = PDX_DAY_OFFSET
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
    base = ['--date', date_utils.insert_date_into_template('{}-{}-{}', k, day_offset = PDX_DAY_OFFSET)]
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
                date_utils.generate_insert_date_into_template_function(
                    DEID_FILE_NAME_TEMPLATE, day_offset=PDX_DAY_OFFSET
                )(ds, k)
            ],
            'file_date_func'                    : date_utils.generate_insert_date_into_template_function(
                '{}/{}/{}',
                day_offset=PDX_DAY_OFFSET
            ),
            's3_payload_loc_url'                : S3_PAYLOAD_DEST,
            'vendor_uuid'                       : '3dac3f25-9e54-4ab6-82b4-3a12f7d01e85',
            'pyspark_normalization_script_name' : '/home/hadoop/spark/providers/pdx/pharmacyclaims/sparkNormalizePDX.py',
            'pyspark_normalization_args_func'   : norm_args,
            'pyspark'                           : True
        }
    ),
    task_id='detect_move_normalize',
    dag=mdag
)

if HVDAG.HVDAG.airflow_env != 'test':
    sql_template = """
        MSCK REPAIR TABLE pharmacyclaims_20180205 
    """
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

### DAG STRUCTURE ###
if HVDAG.HVDAG.airflow_env != 'test':
    fetch_transaction_file.set_upstream(validate_transaction)
    queue_up_for_matching.set_upstream(validate_deid)
    detect_move_normalize_dag.set_upstream(queue_up_for_matching)
    update_analytics_db.set_upstream(detect_move_normalize_dag)

decrypt_transaction.set_upstream(fetch_transaction_file)

split_transaction_file.set_upstream(decrypt_transaction)

clean_up_workspace.set_upstream(split_transaction_file)

detect_move_normalize_dag.set_upstream(split_transaction_file)
