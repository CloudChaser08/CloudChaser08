from airflow.models import Variable
from airflow.operators import PythonOperator, SubDagOperator, DummyOperator
from datetime import datetime, timedelta
from subprocess import check_call
import os
import re
import logging

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

import util.date_utils as date_utils
import util.s3_utils as s3_utils

for m in [s3_validate_file, s3_fetch_file, decrypt_files,
          split_push_files, queue_up_for_matching, clean_up_tmp_dir,
          detect_move_normalize, HVDAG,
          update_analytics_db, date_utils, s3_utils]:
    reload(m)

# Applies to all files
TMP_PATH_TEMPLATE = '/tmp/xifin/medicalclaims/{}{}{}/'
DAG_NAME = 'xifin_dx_pipeline'

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2018, 8, 10, 0),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2)
}

mdag = HVDAG.HVDAG(
    dag_id=DAG_NAME,
    schedule_interval=None, # TBD
    default_args=default_args
)

# Applies to all transaction files
if HVDAG.HVDAG.airflow_env == 'test':
    S3_TRANSACTION_RAW_URL = 's3://salusv/testing/dewey/airflow/e2e/xifin/medicalclaims/raw/'
    S3_TRANSACTION_PROCESSED_URL_TEMPLATE = 's3://salusv/testing/dewey/airflow/e2e/xifin/medicalclaims/out/{}/{}/{}/'
    S3_PAYLOAD_DEST = 's3://salusv/testing/dewey/airflow/e2e/xifin/medicalclaims/payload/'
else:
    S3_TRANSACTION_RAW_URL = 's3://healthverity/incoming/xifin/'
    S3_TRANSACTION_PROCESSED_URL_TEMPLATE = 's3://salusv/incoming/medicalclaims/xifin/{}/{}/{}/'
    S3_PAYLOAD_DEST = 's3://salusv/matching/payload/medicalclaims/xifin/'

XIFIN_FILE_TYPES = ['billed_procedures', 'payors', 'tests', 'demographics', 'diagnosis']

# Transaction Addon file
TRANSACTION_TMP_PATH_TEMPLATE = TMP_PATH_TEMPLATE + 'raw/'
TRANSACTION_FILE_DESCRIPTION = 'Xifin DX transactions file'
TRANSACTION_FILE_NAME_TEMPLATE = 'accn_{}_{{}}{{}}{{}}_[0-9]+.pout'

# Deid file
DEID_FILE_DESCRIPTION = 'Xifin DX deid file'
DEID_FILE_NAME_TEMPLATE = 'accn_{}_{{}}{{}}{{}}_[0-9]+.out'

XIFIN_DX_DAY_OFFSET = 0

S3_PROD_MATCHING_URL_TEMPLATE='s3://salusv/matching/{}/payload/5a1b4dc1-e88c-4c17-a653-ef15ea1e633a/'

get_tmp_dir = date_utils.generate_insert_date_into_template_function(
    TRANSACTION_TMP_PATH_TEMPLATE
)

def get_file_paths_func(template, zipped = False):
    def out(ds, kwargs):
        file_dir = get_tmp_dir(ds, kwargs)
        tmplt = template + '.zip' if zipped else template + '$'
        expected_file_name_regex = date_utils.insert_date_into_template(
            tmplt, kwargs, day_offset=XIFIN_DX_DAY_OFFSET
        )

        return [
            file_dir + f for f in os.listdir(file_dir)
            if re.search(expected_file_name_regex, f)
        ]

    return out

def get_deid_demographics_file_names(ds, kwargs):
    return [
        S3_TRANSACTION_RAW_URL + f + '.out' for f in 
        kwargs['ti'].xcom_pull(dag_id=DAG_NAME, task_ids='persist_batch_file_names', key='batch_file_names')
        if 'demographics' in f
    ]

def get_deid_not_demographics_file_names(ds, kwargs):
    return [
        S3_TRANSACTION_RAW_URL + f + '.out' for f in 
        kwargs['ti'].xcom_pull(dag_id=DAG_NAME, task_ids='persist_batch_file_names', key='batch_file_names')
        if 'demographics' not in f
    ]

def encrypted_decrypted_file_paths_function(ds, kwargs):
    file_names = kwargs['ti'].xcom_pull(dag_id=DAG_NAME, task_ids='persist_batch_file_names', key='batch_file_names'),
    return [[fname + '.out', fname + '.out.gz'] for fname in file_names]

def generate_file_validation_task(
        task_id, path_template, minimum_file_size
):
    return SubDagOperator(
        subdag=s3_validate_file.s3_validate_file(
            DAG_NAME,
            'validate_' + task_id,
            default_args['start_date'],
            mdag.schedule_interval,
            {
                'expected_file_name_func' : date_utils.generate_insert_date_into_template_function(
                    path_template, day_offset=XIFIN_DX_DAY_OFFSET
                ),
                'file_name_pattern_func'  : date_utils.generate_insert_regex_into_template_function(
                    path_template
                ),
                'minimum_file_size'       : minimum_file_size,
                's3_prefix'               : '/'.join(S3_TRANSACTION_RAW_URL.split('/')[3:]),
                's3_bucket'               : 'healthverity',
                'file_description'        : 'Xifin ' + task_id + ' file',
                'regex_name_match'        : True
            }
        ),
        task_id='validate_' + task_id,
        dag=mdag
    )

validate_transaction = {}
validate_deid = {}
for f_type in XIFIN_FILE_TYPES:
    validate_transaction[f_type] = generate_file_validation_task(
        'transaction_' + f_type, TRANSACTION_FILE_NAME_TEMPLATE.format(f_type),
        500
    )

    validate_deid[f_type] = generate_file_validation_task(
        'deid_' + f_type, DEID_FILE_NAME_TEMPLATE.format(f_type),
        500
    )

def generate_file_fetch_task(task_id, path_template):
    return SubDagOperator(
        subdag=s3_fetch_file.s3_fetch_file(
            DAG_NAME,
            'fetch_' + task_id,
            default_args['start_date'],
            mdag.schedule_interval,
            {
                'tmp_path_template'      : TRANSACTION_TMP_PATH_TEMPLATE,
                'expected_file_name_func': date_utils.generate_insert_date_into_template_function(
                    path_template, day_offset=XIFIN_DX_DAY_OFFSET
                ),
                's3_prefix'              : '/'.join(S3_TRANSACTION_RAW_URL.split('/')[3:]),
                's3_bucket'              : S3_TRANSACTION_RAW_URL.split('/')[2],
                'regex_name_match'       : True,
                'multi_match'            : True
            }
        ),
        task_id='fetch_' + task_id,
        dag=mdag
    )

fetch_transaction = {}
for f_type in XIFIN_FILE_TYPES:
    fetch_transaction[f_type] = generate_file_fetch_task(
        'transaction_' + f_type, TRANSACTION_FILE_NAME_TEMPLATE.format(f_type)
    )

def do_persist_batch_file_names(ds, **kwargs):
    file_names = []
    for f_type in XIFIN_FILE_TYPES:
        deid_regex  = date_utils.insert_date_into_template(DEID_FILE_NAME_TEMPLATE.format(f_type), kwargs, day_offset=XIFIN_DX_DAY_OFFSET)
        file_names += [k.split('/')[-1].replace('.out', '') for k in s3_utils.list_s3_bucket(S3_TRANSACTION_RAW_URL) if re.search(deid_regex, k)]

    logging.info('Found files: {}'.format(str(file_names)))
    kwargs['ti'].xcom_push(key='batch_file_names', value=file_names)

persist_batch_file_names = PythonOperator(
    task_id='persist_batch_file_names',
    provide_context=True,
    python_callable=do_persist_batch_file_names,
    dag=mdag
)

decrypt_transaction_files = SubDagOperator(
    subdag=decrypt_files.decrypt_files(
        DAG_NAME,
        'decrypt_transaction_files',
        default_args['start_date'],
        mdag.schedule_interval,
        {
            'tmp_dir_func'                        : get_tmp_dir,
            'encrypted_decrypted_file_paths_func' : encrypted_decrypted_file_paths_function
        }
    ),
    task_id='decrypt_transaction_files',
    dag=mdag
)

def generate_file_split_task(task_id, path_template, subdir):
    return SubDagOperator(
        subdag=split_push_files.split_push_files(
            DAG_NAME,
            'split_' + task_id + '_files',
            default_args['start_date'],
            mdag.schedule_interval,
            {
                'tmp_dir_func'             : get_tmp_dir,
                'parts_dir_func'           : lambda ds, k: subdir,
                'file_paths_to_split_func' : get_file_paths_func(path_template),
                'file_name_pattern_func'   :
                    date_utils.generate_insert_regex_into_template_function(
                        path_template
                    ),
                's3_prefix_func'           :
                    date_utils.generate_insert_date_into_template_function(
                        S3_TRANSACTION_PROCESSED_URL_TEMPLATE + subdir + '/', day_offset=XIFIN_DX_DAY_OFFSET
                    ),
                'split_size'               : '20M'
            }
        ),
        task_id='split_' + task_id + '_files',
        dag=mdag
    )

split_transaction = {}
for f_type in XIFIN_FILE_TYPES:
    split_transaction[f_type] = generate_file_split_task(
        'transaction_' + f_type, TRANSACTION_FILE_NAME_TEMPLATE.format(f_type), f_type
    )

queue_up_for_full_matching = SubDagOperator(
    subdag=queue_up_for_matching.queue_up_for_matching(
        DAG_NAME,
        'queue_up_for_full_matching',
        default_args['start_date'],
        mdag.schedule_interval,
        {
            'source_files_func' : get_deid_demographics_file_names
        }
    ),
    task_id='queue_up_for_full_matching',
    dag=mdag
)

queue_up_for_passthrough_matching = SubDagOperator(
    subdag=queue_up_for_matching.queue_up_for_matching(
        DAG_NAME,
        'queue_up_for_passthrough_matching',
        default_args['start_date'],
        mdag.schedule_interval,
        {
            'source_files_func' : get_deid_not_demographics_file_names,
            'passthrough_only'  : True,
            'matching-engine'   : 'xifin-passthrough'
        }
    ),
    task_id='queue_up_for_passthrough_matching',
    dag=mdag
)

#
# Post-Matching
#
def do_detect_full_matching_done(ds, **kwargs):
    deid_files = get_deid_demographics_file_names(ds, kwargs)
    s3_path_prefix = S3_PROD_MATCHING_URL_TEMPLATE.format('prod')
    template = '{}{}*DONE*'
    for deid_file in deid_files:
        s3_key = template.format(s3_path_prefix, deid_file)
        logging.info('Poking for key : {}'.format(s3_key))
        if not s3_utils.s3_key_exists(s3_key):
            raise ValueError('S3 key not found')

detect_full_matching_done = PythonOperator(
    task_id='detect_full_matching_done',
    provide_context=True,
    python_callable=do_detect_full_matching_done,
    retry_delay=timedelta(minutes=2),
    retries=180, #6 hours of retrying
    dag=mdag
)

def do_detect_passthrough_matching_done(ds, **kwargs):
    deid_files = get_deid_demographics_file_names(ds, kwargs)
    s3_path_prefix = S3_PROD_MATCHING_URL_TEMPLATE.format('xifin-passthrough')
    template = '{}{}*DONE*'
    for deid_file in deid_files:
        s3_key = template.format(s3_path_prefix, deid_file)
        logging.info('Poking for key : {}'.format(s3_key))
        if not s3_utils.s3_key_exists(s3_key):
            raise ValueError('S3 key not found')

detect_passthrough_matching_done = PythonOperator(
    task_id='detect_passthrough_matching_done',
    provide_context=True,
    python_callable=do_detect_passthrough_matching_done,
    retry_delay=timedelta(minutes=2),
    retries=180, #6 hours of retrying
    dag=mdag
)

def do_move_full_matching_payloads(ds, **kwargs):
    deid_files = get_deid_demographics_file_names(ds, kwargs)
    date_path = date_utils.insert_date_into_template_function('{}/{}/{}/', k, day_offset=XIFIN_DX_DAY_OFFSET),
    for deid_file in deid_files:
        s3_prefix = S3_PROD_MATCHING_URL.format('prod') + deid_file
        for payload_file in s3_utils.list_s3_bucket(s3_prefix):
            directory = [f_type for f_type in XIFIN_FILE_TYPES if f_type in payload_file][1]
            s3_utils.copy_file(payload_file, S3_PAYLOAD_DEST + date_path + directory + '/' + payload_file.split('/')[-1])

move_full_matching_payloads = PythonOperator(
    task_id='move_full_matching_payloads',
    provide_context=True,
    python_callable=do_move_full_matching_payloads,
    dag=mdag
)

def do_move_passthrough_matching_payloads(ds, **kwargs):
    deid_files = get_deid_not_demographics_file_names(ds, kwargs)
    date_path = date_utils.insert_date_into_template_function('{}/{}/{}/', k, day_offset=XIFIN_DX_DAY_OFFSET),
    for deid_file in deid_files:
        s3_prefix = S3_PROD_MATCHING_URL.format('xifin-passthrough') + deid_file
        for payload_file in s3_utils.list_s3_bucket(s3_prefix):
            directory = [f_type for f_type in XIFIN_FILE_TYPES if f_type in payload_file][0]
            s3_utils.copy_file(payload_file, S3_PAYLOAD_DEST + date_path + directory + '/' + payload_file.split('/')[-1])

move_passthrough_matching_payloads = PythonOperator(
    task_id='move_passthrough_matching_payloads',
    provide_context=True,
    python_callable=do_move_passthrough_matching_payloads,
    dag=mdag
)

def norm_args(ds, k):
    base = ['--date', date_utils.insert_date_into_template('{}-{}-{}', k, day_offset=XIFIN_DX_DAY_OFFSET)]
    if HVDAG.HVDAG.airflow_env == 'test':
        base += ['--airflow_test']

    return base

detect_move_normalize = SubDagOperator(
    subdag=detect_move_normalize.detect_move_normalize(
        DAG_NAME,
        'detect_move_normalize',
        default_args['start_date'],
        mdag.schedule_interval,
        {
            'expected_matching_files_func'      : lambda ds, k: [],
            'file_date_func'                    :
                date_utils.generate_insert_date_into_template_function('{}/{}/{}', day_offset=XIFIN_DX_DAY_OFFSET),
            's3_payload_loc_url'                : S3_PAYLOAD_DEST,
            'vendor_uuid'                       : '0b6cc05b-bff3-4365-b229-8d06480ad4a3',
            'pyspark_normalization_script_name' : '/home/hadoop/spark/providers/xifin/medicalclaims/sparkNormalizeXifin.py',
            'pyspark_normalization_args_func'   : norm_args,
            'pyspark'                           : True
        }
    ),
    task_id='detect_move_normalize',
    dag=mdag
)

sql_template = """
    MSCK REPAIR TABLE medicalclaims_20180606
"""

update_analytics_db = SubDagOperator(
    subdag=update_analytics_db.update_analytics_db(
        DAG_NAME,
        'update_analytics_db',
        default_args['start_date'],
        mdag.schedule_interval,
        {
            'sql_command_func' :
                date_utils.generate_insert_date_into_template_function(sql_template)
        }
    ),
    task_id='update_analytics_db',
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

if HVDAG.HVDAG.airflow_env == 'test':
    for f_type in XIFIN_FILE_TYPES:
        for t_prefix in ['validate_transaction', 'validate_deid']:
            t = t_prefix + '_' + f_type
            del mdag.task_dict[t]
            globals()[t] = DummyOperator(
                task_id = t,
                dag = mdag
            )

    for t in ['decrypt_transaction_files', 'queue_up_for_full_matching',
            'queue_up_for_passthrough_matching', 'detect_move_normalize']:
        del mdag.task_dict[t]
        globals()[t] = DummyOperator(
            task_id = t,
            dag = mdag
        )

# For now, don't normalize
for t in ['detect_move_normalize']:
    del mdag.task_dict[t]
    globals()[t] = DummyOperator(
        task_id = t,
        dag = mdag
    )

for f_type in XIFIN_FILE_TYPES:
    fetch_transaction[f_type].set_upstream(validate_transaction[f_type])
persist_batch_file_names.set_upstream([fetch_transaction[f_type] for f_type in XIFIN_FILE_TYPES] +
    [validate_deid[f_type] for f_type in XIFIN_FILE_TYPES])
decrypt_transaction_files.set_upstream(persist_batch_file_names)
for f_type in XIFIN_FILE_TYPES:
    split_transaction[f_type].set_upstream(decrypt_transaction_files)
queue_up_for_full_matching.set_upstream(persist_batch_file_names)
queue_up_for_passthrough_matching.set_upstream(persist_batch_file_names)
detect_full_matching_done.set_upstream(queue_up_for_full_matching)
detect_passthrough_matching_done.set_upstream(queue_up_for_passthrough_matching)
move_full_matching_payloads.set_upstream(detect_full_matching_done)
move_passthrough_matching_payloads.set_upstream(detect_passthrough_matching_done)
detect_move_normalize.set_upstream(
    [move_full_matching_payloads, move_passthrough_matching_payloads] +
    [split_transaction[f_type] for f_type in XIFIN_FILE_TYPES]
)
update_analytics_db.set_upstream(detect_move_normalize)
clean_up_workspace.set_upstream(detect_move_normalize)
