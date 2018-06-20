from airflow.models import Variable
from airflow.operators import *
from datetime import datetime, timedelta
import sys

import common.HVDAG as HVDAG
import subdags.s3_validate_file as s3_validate_file
import subdags.s3_fetch_file as s3_fetch_file
import subdags.decrypt_files as decrypt_files
import subdags.split_push_files as split_push_files
import subdags.queue_up_for_matching as queue_up_for_matching
import subdags.detect_move_normalize as detect_move_normalize
import subdags.clean_up_tmp_dir as clean_up_tmp_dir
import subdags.update_analytics_db as update_analytics_db
import util.date_utils as date_utils

for m in [s3_validate_file, s3_fetch_file, decrypt_files, split_push_files,
        queue_up_for_matching, detect_move_normalize, clean_up_tmp_dir, HVDAG,
        update_analytics_db, date_utils]:
    reload(m)

# Applies to all files
TMP_PATH_TEMPLATE='/tmp/express_scripts/pharmacyclaims/{}{}{}/'
DAG_NAME='express_scripts_pipeline'

if HVDAG.HVDAG.airflow_env == 'test':
    S3_TRANSACTION_PREFIX='testing/dewey/airflow/e2e/express_scripts/pharmacyclaims/incoming/'
    S3_PAYLOAD_LOC_URL = 's3://salusv/testing/dewey/airflow/e2e/express_scripts/pharmacyclaims/matching/'
    S3_TRANSACTION_RAW_PATH = 'testing/dewey/airflow/e2e/express_scripts/pharmacyclaims/raw/'
    S3_DEID_RAW_PATH = 'testing/dewey/airflow/e2e/express_scripts/pharmacyclaims/raw/'
    S3_ORIGIN_BUCKET = 'salusv'
    S3_TEXT_EXPRESS_SCRIPTS_PREFIX = 'testing/dewey/airflow/e2e/express_scripts/pharmacyclaims/output/text/'
    S3_PARQUET_EXPRESS_SCRIPTS_PREFIX = 'testing/dewey/airflow/e2e/express_scripts/pharmacyclaims/output/parquet/'
    S3_TEXT_EXPRESS_SCRIPTS_WAREHOUSE = 's3://salusv/' + S3_TEXT_EXPRESS_SCRIPTS_PREFIX
else:
    S3_TRANSACTION_PREFIX='incoming/pharmacyclaims/esi/'
    S3_PAYLOAD_LOC_URL = 's3://salusv/matching/payload/pharmacyclaims/esi/'
    S3_TRANSACTION_RAW_PATH='incoming/esi/'
    S3_DEID_RAW_PATH='incoming/esi/'
    S3_ORIGIN_BUCKET = 'healthverity'
    S3_TEXT_EXPRESS_SCRIPTS_PREFIX = 'warehouse/text/pharmacyclaims/express_scripts/'
    S3_PARQUET_EXPRESS_SCRIPTS_PREFIX = 'warehouse/parquet/pharmacyclaims/express_scripts/'
    S3_TEXT_EXPRESS_SCRIPTS_WAREHOUSE = 's3://salusv/' + S3_TEXT_EXPRESS_SCRIPTS_PREFIX

# Transaction file
TRANSACTION_FILE_DESCRIPTION='Express Scripts transaction file'
ACCREDO_TRANSACTION_FILE_DESCRIPTION='Accredo transaction file'
S3_TRANSACTION_SPLIT_PATH='s3://salusv/' + S3_TRANSACTION_PREFIX
TRANSACTION_FILE_NAME_TEMPLATE='10130X001_HV_RX_Claims_D{}{}{}.txt'
ACCREDO_TRANSACTION_FILE_NAME_TEMPLATE='10130X001_HV_ODS_Claims_D{}{}{}.txt'
MINIMUM_TRANSACTION_FILE_SIZE=500

# Deid file
DEID_FILE_DESCRIPTION='Express Scripts deid file'
ACCREDO_DEID_FILE_DESCRIPTION='Accredo deid file'
DEID_FILE_NAME_TEMPLATE='10130X001_HV_RX_Claims_D{}{}{}_key.txt'
ACCREDO_DEID_FILE_NAME_TEMPLATE='10130X001_HV_ODS_Claims_D{}{}{}_key.txt'
MINIMUM_DEID_FILE_SIZE=500

EXPRESS_SCRIPTS_DAY_OFFSET = 6

get_tmp_dir = date_utils.generate_insert_date_into_template_function(TMP_PATH_TEMPLATE)

get_expected_transaction_file_name = date_utils.generate_insert_date_into_template_function(
        TRANSACTION_FILE_NAME_TEMPLATE, day_offset=EXPRESS_SCRIPTS_DAY_OFFSET
)
get_expected_accredo_transaction_file_name = date_utils.generate_insert_date_into_template_function(
        ACCREDO_TRANSACTION_FILE_NAME_TEMPLATE, day_offset=EXPRESS_SCRIPTS_DAY_OFFSET
)
get_expected_transaction_file_regex = date_utils.generate_insert_regex_into_template_function(
        TRANSACTION_FILE_NAME_TEMPLATE
)
get_expected_accredo_transaction_file_regex = date_utils.generate_insert_regex_into_template_function(
        ACCREDO_TRANSACTION_FILE_NAME_TEMPLATE
)
get_expected_deid_file_name = date_utils.generate_insert_date_into_template_function(
        DEID_FILE_NAME_TEMPLATE, day_offset=EXPRESS_SCRIPTS_DAY_OFFSET
)
get_expected_accredo_deid_file_name = date_utils.generate_insert_date_into_template_function(
        ACCREDO_DEID_FILE_NAME_TEMPLATE, day_offset=EXPRESS_SCRIPTS_DAY_OFFSET
)
get_expected_deid_file_regex = date_utils.generate_insert_regex_into_template_function(
        DEID_FILE_NAME_TEMPLATE
)
get_expected_accredo_deid_file_regex = date_utils.generate_insert_regex_into_template_function(
        ACCREDO_DEID_FILE_NAME_TEMPLATE
)

def get_encrypted_decrypted_file_paths(ds, kwargs):
    tmp_dir = get_tmp_dir(ds, kwargs)
    in1 = get_expected_transaction_file_name(ds, kwargs)
    in2 = get_expected_accredo_transaction_file_name(ds, kwargs)
    return [
        [tmp_dir + '/' + in1, tmp_dir + '/' + in1 + '.gz'],
        [tmp_dir + '/' + in2, tmp_dir + '/' + in2 + '.gz']
    ]

def get_transaction_files_paths(ds, kwargs):
    return [
        get_tmp_dir(ds, kwargs)
            + get_expected_transaction_file_name(ds, kwargs),
        get_tmp_dir(ds, kwargs)
            + get_expected_accredo_transaction_file_name(ds, kwargs)
    ]

def get_parquet_dates(ds, kwargs):
    date_path = date_utils.insert_date_into_template(
        '{}/{}/{}',
        kwargs,
        day_offset = EXPRESS_SCRIPTS_DAY_OFFSET
    )
    warehouse_files = check_output(['aws', 's3', 'ls', '--recursive', S3_TEXT_EXPRESS_SCRIPTS_WAREHOUSE]).split("\n")
    file_dates = map(lambda f: '/'.join(f.split(' ')[-1].replace(S3_TEXT_EXPRESS_SCRIPTS_PREFIX, '').split('/')[:-1]), warehouse_files)
    file_dates = filter(lambda d: len(d) == 10, file_dates)
    file_dates = sorted(list(set(file_dates)))
    return filter(lambda d: d < date_path, file_dates)[-2:] + [date_path]

def get_deid_file_urls(ds, kwargs):
    return [
        's3://healthverity/' + S3_DEID_RAW_PATH + f
        for f in get_expected_matching_files(ds, kwargs)
    ]

def get_expected_matching_files(ds, kwargs):
    return [
        get_expected_deid_file_name(ds, kwargs),
        get_expected_accredo_deid_file_name(ds, kwargs)
    ]

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2017, 4, 2),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'priority_weight': 5
}

mdag = HVDAG.HVDAG(
    dag_id=DAG_NAME,
    schedule_interval='0 0 * * 0',
    default_args=default_args
)

def generate_file_validation_dag(
        task_id, expected_file_name_func, file_name_pattern_func,
        minimum_file_size, s3_prefix, file_description
):
    return SubDagOperator(
        subdag=s3_validate_file.s3_validate_file(
            DAG_NAME,
            'validate_' + task_id + '_file',
            default_args['start_date'],
            mdag.schedule_interval,
            {
                'expected_file_name_func': expected_file_name_func,
                'file_name_pattern_func' : file_name_pattern_func,
                'minimum_file_size'      : minimum_file_size,
                's3_prefix'              : s3_prefix,
                's3_bucket'              : S3_ORIGIN_BUCKET,
                'file_description'       : file_description
            }
        ),
        task_id='validate_' + task_id + '_file',
        dag=mdag
    )

validate_transaction_file_dag = generate_file_validation_dag(
    'transaction', get_expected_transaction_file_name, get_expected_transaction_file_regex,
    MINIMUM_TRANSACTION_FILE_SIZE, S3_TRANSACTION_RAW_PATH, TRANSACTION_FILE_DESCRIPTION
)

validate_accredo_transaction_file_dag = generate_file_validation_dag(
    'accredo_transaction', get_expected_accredo_transaction_file_name, get_expected_accredo_transaction_file_regex,
    MINIMUM_TRANSACTION_FILE_SIZE, S3_TRANSACTION_RAW_PATH, ACCREDO_TRANSACTION_FILE_DESCRIPTION
)

validate_deid_file_dag = generate_file_validation_dag(
    'deid', get_expected_deid_file_name, get_expected_deid_file_regex,
    MINIMUM_DEID_FILE_SIZE, S3_DEID_RAW_PATH, DEID_FILE_DESCRIPTION
)

validate_accredo_deid_file_dag = generate_file_validation_dag(
    'accredo_deid', get_expected_accredo_deid_file_name, get_expected_accredo_deid_file_regex,
    MINIMUM_DEID_FILE_SIZE, S3_DEID_RAW_PATH, DEID_FILE_DESCRIPTION
)

def generate_fetch_file_dag(task_id, expected_file_name_func, s3_prefix):
    return SubDagOperator(
        subdag=s3_fetch_file.s3_fetch_file(
            DAG_NAME,
            'fetch_' + task_id + '_file',
            default_args['start_date'],
            mdag.schedule_interval,
            {
                'tmp_path_template'      : TMP_PATH_TEMPLATE,
                'expected_file_name_func': expected_file_name_func,
                's3_prefix'              : s3_prefix,
                's3_bucket'              : S3_ORIGIN_BUCKET
            }
        ),
        task_id='fetch_' + task_id + '_file',
        dag=mdag
    )

fetch_transaction_file_dag = generate_fetch_file_dag(
    'transaction', get_expected_transaction_file_name, S3_TRANSACTION_RAW_PATH
)

fetch_accredo_transaction_file_dag = generate_fetch_file_dag(
    'accredo_transaction', get_expected_accredo_transaction_file_name, S3_TRANSACTION_RAW_PATH
)

decrypt_transaction_files_dag = SubDagOperator(
    subdag=decrypt_files.decrypt_files(
        DAG_NAME,
        'decrypt_transaction_files',
        default_args['start_date'],
        mdag.schedule_interval,
        {
            'tmp_dir_func'                        : get_tmp_dir,
            'encrypted_decrypted_file_paths_func' : get_encrypted_decrypted_file_paths
        }
    ),
    task_id='decrypt_transaction_files',
    dag=mdag
)

split_push_transaction_files_dag = SubDagOperator(
    subdag=split_push_files.split_push_files(
        DAG_NAME,
        'split_push_transaction_files',
        default_args['start_date'],
        mdag.schedule_interval,
        {
            'tmp_dir_func'             : get_tmp_dir,
            'file_paths_to_split_func' : get_transaction_files_paths,
            'file_name_pattern_func'   : date_utils.generate_insert_regex_into_template_function(
                TRANSACTION_FILE_NAME_TEMPLATE
            ),
            's3_prefix_func'           : date_utils.generate_insert_date_into_template_function(
                S3_TRANSACTION_SPLIT_PATH + '{}/{}/{}/',
                day_offset = EXPRESS_SCRIPTS_DAY_OFFSET
            ),
            'num_splits'               : 100
        }
    ),
    task_id='split_push_transaction_files',
    dag=mdag
)

queue_up_for_matching_dag = SubDagOperator(
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

detect_move_normalize_dag = SubDagOperator(
    subdag=detect_move_normalize.detect_move_normalize(
        DAG_NAME,
        'detect_move_normalize',
        default_args['start_date'],
        mdag.schedule_interval,
        {
            'expected_matching_files_func'   : get_expected_matching_files,
            'file_date_func'                 : date_utils.generate_insert_date_into_template_function(
                '{}-{}-{}',
                day_offset = EXPRESS_SCRIPTS_DAY_OFFSET
            ),
            'incoming_path'                  : S3_TRANSACTION_PREFIX,
            'normalization_routine_directory': '/home/airflow/airflow/dags/providers/express_scripts/pharmacyclaims/',
            'normalization_routine_script'   : '/home/airflow/airflow/dags/providers/express_scripts/pharmacyclaims/rsNormalizeExpressScriptsRX.py',
            'parquet_dates_func'             : get_parquet_dates,
            's3_text_path_prefix'            : S3_TEXT_EXPRESS_SCRIPTS_PREFIX,
            's3_parquet_path_prefix'         : S3_PARQUET_EXPRESS_SCRIPTS_PREFIX,
            's3_payload_loc_url'             : S3_PAYLOAD_LOC_URL,
            'vendor_description'             : 'Express Scripts RX',
            'vendor_uuid'                    : 'f726747e-9dc0-4023-9523-e077949ae865',
            'feed_data_type'                 : 'pharmacy-old',
            'cluster_identifier'             : 'ESI-pharmacyclaims'
        }
    ),
    task_id='detect_move_normalize',
    dag=mdag
)

clean_up_tmp_dir_dag = SubDagOperator(
    subdag=clean_up_tmp_dir.clean_up_tmp_dir(
        DAG_NAME,
        'clean_up_tmp_dir',
        default_args['start_date'],
        mdag.schedule_interval,
        {
            'tmp_path_template'      : TMP_PATH_TEMPLATE,
        }
    ),
    task_id='clean_up_tmp_dir',
    dag=mdag
)

sql_old_template = """
    ALTER TABLE pharmacyclaims_old ADD PARTITION (part_provider='express_scripts', part_processdate='{0}/{1}/{2}')
    LOCATION 's3a://salusv/warehouse/parquet/pharmacyclaims/express_scripts/{0}/{1}/{2}/'
"""

update_analytics_db_old = SubDagOperator(
    subdag=update_analytics_db.update_analytics_db(
        DAG_NAME,
        'update_analytics_db_old',
        default_args['start_date'],
        mdag.schedule_interval,
        {
            'sql_command_func' : date_utils.generate_insert_date_into_template_function(
                sql_old_template,
                day_offset=EXPRESS_SCRIPTS_DAY_OFFSET
            )
        }
    ),
    task_id='update_analytics_db_old',
    dag=mdag
)

if HVDAG.HVDAG.airflow_env == 'test':
    for t in ['validate_transaction_file_dag', 'validate_accredo_file_dag',
            'validate_deid_file_dag', 'validate_accredo_deid_file_dag',
            'queue_up_for_matching_dag', 'update_analytics_db_old']:
        del mdag.task_dict[t]
        globals()[t] = DummyOperator(
            task_id = t,
            dag = mdag
        )

fetch_transaction_file_dag.set_upstream(validate_transaction_file_dag)
fetch_accredo_transaction_file_dag.set_upstream(validate_accredo_transaction_file_dag)
decrypt_transaction_files_dag.set_upstream([fetch_transaction_file_dag, fetch_accredo_transaction_file_dag])
split_push_transaction_files_dag.set_upstream(decrypt_transaction_files_dag)
queue_up_for_matching_dag.set_upstream([validate_deid_file_dag, validate_accredo_deid_file_dag])
detect_move_normalize_dag.set_upstream([queue_up_for_matching_dag, split_push_transaction_files_dag])
update_analytics_db_old.set_upstream(detect_move_normalize_dag)
clean_up_tmp_dir_dag.set_upstream(split_push_transaction_files_dag)
