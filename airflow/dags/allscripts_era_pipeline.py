from datetime import datetime, timedelta
import logging
import os
import re

from airflow.operators import SubDagOperator, PythonOperator, DummyOperator
from airflow.models import Variable

# hv-specific modules
import common.HVDAG as HVDAG
import subdags.s3_validate_file as s3_validate_file
import subdags.s3_fetch_file as s3_fetch_file
import subdags.s3_push_files as s3_push_files
import subdags.decrypt_files as decrypt_files
import subdags.split_push_files as split_push_files
import subdags.queue_up_for_matching as queue_up_for_matching
import subdags.detect_move_normalize as detect_move_normalize
import subdags.update_analytics_db as update_analytics_db
import subdags.clean_up_tmp_dir as clean_up_tmp_dir
import util.decompression as decompression
import util.date_utils as date_utils
import util.s3_utils as s3_utils

for m in [s3_validate_file, s3_fetch_file, decrypt_files,
          split_push_files, queue_up_for_matching, clean_up_tmp_dir,
          detect_move_normalize, decompression, date_utils, s3_utils, HVDAG]:
    reload(m)

# Applies to all files
TMP_PATH_TEMPLATE = '/tmp/allscripts/era/{}{}{}/'
DAG_NAME = 'allscripts_era_pipeline'

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2018, 6, 7, 8),      #TODO: change when figured out
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2)
}

mdag = HVDAG.HVDAG(
    dag_id = DAG_NAME,
    schedule_interval = '0 8 * * *',            #TODO: change when figured out
    default_args = default_args
)

# Applies to all transaction files
if HVDAG.HVDAG.airflow_env == 'test':
    S3_TRANSACTION_RAW_URL = 's3://salusv/testing/dewey/airflow/e2e/allscripts/era/raw/'
    S3_TRANSACTION_PROCESSED_URL_TEMPLATE = 's3://salusv/testing/dewey/airflow/e2e/allscripts/era/out/{}/{}/{}/'
else:
    S3_TRANSACTION_RAW_URL = 's3://healthverity/incoming/allscripts/'
    S3_TRANSACTION_PROCESSED_URL_TEMPLATE = 's3://salusv/incoming/allscripts/era/{}/{}/{}/'

# Transaction file
TRANSACTION_TMP_PATH_TEMPLATE = TMP_PATH_TEMPLATE + 'raw/out/'
TRANSACTION_FILE_DESCRIPTION = 'Allscripts ERA transaction file'
TRANSACTION_FILE_NAME_TEMPLATE = 'HV_REMITS_{}{}{}_[0-9].out.zip'

# Rest file
REST_TMP_PATH_TEMPLATE = TMP_PATH_TEMPLATE + 'raw/rest/'
REST_FILE_DESCRIPTION = 'Allscripts ERA rest file'
REST_FILE_NAME_TEMPLATE = 'HV_REMITS_{}{}{}_[0-9].rest.zip'

# Deid file
DEID_TMP_PATH_TEMPLATE = TMP_PATH_TEMPLATE + 'raw/deid/'
DEID_FILE_DESCRIPTION = 'Allscripts ERA deid file'
DEID_FILE_NAME_TEMPLATE = 'HV_REMITS_{}{}{}_[0-9].deid.zip'

S3_PAYLOAD_DEST = 's3://salusv/matching/payload/era/allscripts/'

ALLSCRIPTS_ERA_DAY_OFFSET = 1

get_t_tmp_dir = date_utils.generate_insert_date_into_template_function(
    TRANSACTION_TMP_PATH_TEMPLATE
)
get_r_tmp_dir = date_utils.generate_insert_date_into_template_function(
    REST_TMP_PATH_TEMPLATE
)
get_d_tmp_dir = date_utils.generate_insert_date_into_template_function(
    DEID_TMP_PATH_TEMPLATE
)


def get_file_paths_function(tmp_dir_func, filename_template):
    def out(ds, kwargs):
        file_dir = tmp_dir_func(ds, kwargs)
        expected_file_name_regex = date_utils.insert_date_into_template(
            filename_template, kwargs, day_offset=ALLSCRIPTS_ERA_DAY_OFFSET
        )
        return [
            file_dir + f for f in os.listdir(file_dir)
            if re.search(expected_file_name_regex, f)
        ]


def get_deid_file_paths(ds, **kwargs):
    deid_regex = date_utils.insert_date_into_template(DEID_FILE_NAME_TEMPLATE, kwargs, day_offset=ALLSCRIPTS_ERA_DAY_OFFSET)
    deid_files = [k.split('/')[-1] for k in s3_utils.list_s3_bucket(S3_TRANSACTION_RAW_URL) if re.search(deid_regex, k)]
    logging.info('Found deid files: {}'.format(str(deid_files)))
    kwargs['ti'].xcom_push(key='deid_files', value=deid_files)


def encrypted_decrypted_file_paths_function(ds, kwargs):
    file_dir = get_t_tmp_dir(ds, kwargs)

    expected_file_name_regex = date_utils.insert_date_into_template(
        TRANSACTION_FILE_NAME_TEMPLATE, kwargs, day_offset=ALLSCRIPTS_ERA_DAY_OFFSET
    ) + "$"

    return [
        (file_dir + f, file_dir + f + '.gz') for f in os.listdir(file_dir)
        if re.search(expected_file_name_regex, f)
    ]


def generate_file_validation_dag(task_id, file_name_template, file_description):
    return SubDagOperator(
        subdag=s3_validate_file.s3_validate_file(
            DAG_NAME,
            'validate_{}_file'.format(task_id),
            default_args['start_date'],
            mdag.schedule_interval,
            {
                'expected_file_name_func' : date_utils.generate_insert_date_into_template_function(
                    file_name_template, day_offset=ALLSCRIPTS_ERA_DAY_OFFSET
                ),
                'file_name_pattern_func'  : date_utils.generate_insert_regex_into_template_function(
                    file_name_template
                ),
                'minimum_file_size'       : 100,
                's3_prefix'               : '/'.join(S3_TRANSACTION_RAW_URL.split('/')[3:]),
                's3_bucket'               : S3_TRANSACTION_RAW_URL.split('/')[2],
                'file_description'        : file_description,
                'regex_name_match'        : True
            }
        ),
        task_id='validate_{}_file'.format(task_id),
        dag=mdag
    )


validate_transaction = generate_file_validation_dag('transaction', TRANSACTION_FILE_NAME_TEMPLATE, TRANSACTION_FILE_DESCRIPTION)
validate_rest = generate_file_validation_dag('rest', REST_FILE_NAME_TEMPLATE, REST_FILE_DESCRIPTION)
validate_deid = generate_file_validation_dag('deid', DEID_FILE_NAME_TEMPLATE, DEID_FILE_DESCRIPTION)

queue_up_for_matching = SubDagOperator(
    subdag=queue_up_for_matching.queue_up_for_matching(
        DAG_NAME,
        'queue_up_for_matching',
        default_args['start_date'],
        mdag.schedule_interval,
        {
            'source_files_func' : lambda ds, k: [
                S3_TRANSACTION_RAW_URL + date_utils.insert_date_into_template(
                    DEID_FILE_NAME_TEMPLATE, k, day_offset=ALLSCRIPTS_ERA_DAY_OFFSET
                )
            ],
            'regex_name_match'  : True
        }
    ),
    task_id='queue_up_for_matching',
    dag=mdag
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
                TRANSACTION_FILE_NAME_TEMPLATE, day_offset=ALLSCRIPTS_ERA_DAY_OFFSET
            ),
            's3_prefix'              : '/'.join(S3_TRANSACTION_RAW_URL.split('/')[3:]),
            's3_bucket'              : S3_TRANSACTION_RAW_URL.split('/')[2],
            'regex_name_match'       : True,
            'multi_match'            : True
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
            'tmp_dir_func'                        : get_t_tmp_dir,
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
            'tmp_dir_func'             : get_t_tmp_dir,
            'file_paths_to_split_func' : get_file_paths_function(
                get_t_tmp_dir, TRANSACTION_FILE_NAME_TEMPLATE
            ),
            's3_prefix_func'           : date_utils.generate_insert_date_into_template_function(
                S3_TRANSACTION_PROCESSED_URL_TEMPLATE, day_offset=ALLSCRIPTS_ERA_DAY_OFFSET
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

get_deid_files = PythonOperator(
    task_id='get_deid_files',
    provide_context=True,
    python_callable=get_deid_file_paths,
    dag=mdag
)

#
# Post-Matching
#
def norm_args(ds, k):
    base = ['--date', date_utils.insert_date_into_template('{}-{}-{}', k, day_offset=ALLSCRIPTS_ERA_DAY_OFFSET)]
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
            'expected_matching_files_func'      : lambda ds,k: k['ti'].xcom_pull(dag_id=DAG_NAME, task_ids='get_deid_files', key='deid_files'),
            'file_date_func'                    : date_utils.generate_insert_date_into_template_function(
                '{}/{}/{}', day_offset=ALLSCRIPTS_ERA_DAY_OFFSET
            ),
            's3_payload_loc_url'                : S3_PAYLOAD_DEST,
            'vendor_uuid'                       : '0b6cc05b-bff3-4365-b229-8d06480ad4a3',
            'pyspark_normalization_script_name' : '/home/hadoop/spark/providers/allscripts/custom_era//sparkNormalizeAllscriptsCustomERA.py',
            'pyspark_normalization_args_func'   : norm_args,
            'pyspark'                           : True
        }
    ),
    task_id='detect_move_normalize',
    dag=mdag
)


if HVDAG.HVDAG.airflow_env == 'test':
    for t in [
            'validate_transaction_file', 'validate_deid_file', 'validate_rest_file', 
            'queue_up_for_matching', 'detect_move_normalize'
    ]:
        del mdag.task_dict[t]
        globals()[t] = DummyOperator(
            task_id = t,
            dag = mdag
        )


fetch_rest.set_upstream(validate_rest)
fetch_transaction.set_upstream(validate_transaction)
fetch_deid.set_upstream(validate_deid)

unzip_rest.set_upstream(fetch_rest)
unzip_transaction.set_upstream(fetch_transaction)
unzip_deid.set_upstream(fetch_deid)

get_deid_files.set_upstream(unzip_deid)
queue_up_for_matching.set_upstream(validate_deid)

decrypt_rest.set_upstream(fetch_rest)
decrypt_transaction.set_upstream(fetch_transaction)

split_rest.set_upstream(decrypt_rest)
split_transaction.set_upstream(decrypt_transaction)
detect_move_normalize_dag.set_upstream(
    [split_transaction, get_deid_files, queue_up_for_matching]
)
clean_up_workspace.set_upstream([split_transaction, split_rest])
