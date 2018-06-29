from airflow.models import Variable
from airflow.operators import PythonOperator, SubDagOperator, DummyOperator
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
import subdags.clean_up_tmp_dir as clean_up_tmp_dir

import util.decompression as decompression
import util.date_utils as date_utils

for m in [s3_validate_file, s3_fetch_file, decrypt_files,
          split_push_files, queue_up_for_matching, clean_up_tmp_dir,
          detect_move_normalize, decompression, HVDAG,
          update_analytics_db, date_utils]:
    reload(m)

# Applies to all files
TMP_PATH_TEMPLATE = '/tmp/allscripts/medicalclaims/{}{}{}/'
DAG_NAME = 'allscripts_dx_pipeline'

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2018, 6, 22, 6),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2)
}

mdag = HVDAG.HVDAG(
    dag_id=DAG_NAME,
    schedule_interval="0 6 * * *",
    default_args=default_args
)

# Applies to all transaction files
if HVDAG.HVDAG.airflow_env == 'test':
    S3_TRANSACTION_RAW_URL = 's3://salusv/testing/dewey/airflow/e2e/allscripts/medicalclaims/raw/'
    S3_TRANSACTION_PROCESSED_URL_TEMPLATE = 's3://salusv/testing/dewey/airflow/e2e/allscripts/medicalclaims/out/{}/{}/{}/'
    S3_PAYLOAD_DEST = 's3://salusv/testing/dewey/airflow/e2e/allscripts/medicalclaims/payload/'
else:
    S3_TRANSACTION_RAW_URL = 's3://healthverity/incoming/allscripts/'
    S3_TRANSACTION_PROCESSED_URL_TEMPLATE = 's3://salusv/incoming/medicalclaims/allscripts/{}/{}/{}/'
    S3_PAYLOAD_DEST = 's3://salusv/matching/payload/medicalclaims/allscripts/'

# Transaction Addon file
TRANSACTION_TMP_PATH_TEMPLATE = TMP_PATH_TEMPLATE + 'raw/'
TRANSACTION_CLAIM_FILE_DESCRIPTION = 'Allscripts DX claims files'
TRANSACTION_SVC_FILE_DESCRIPTION = 'Allscripts DX service lines files'
TRANSACTION_CLAIM_FILE_NAME_TEMPLATE = 'HV_CLAIMS_{}{}{}_[0-9]+.out'
TRANSACTION_SVC_FILE_NAME_TEMPLATE = 'HV_CLAIMS_{}{}{}_[0-9]+.rest'

# Deid file
DEID_FILE_DESCRIPTION = 'Allscripts DX deid file'
DEID_FILE_NAME_TEMPLATE = 'HV_CLAIMS_{}{}{}_[0-9]+.deid'

ALLSCRIPTS_DX_DAY_OFFSET = -1

get_tmp_dir = date_utils.generate_insert_date_into_template_function(
    TRANSACTION_TMP_PATH_TEMPLATE
)

def get_file_paths_func(template, zipped = False):
    def out(ds, kwargs):
        file_dir = get_tmp_dir(ds, kwargs)
        tmplt = template + '.zip' if zipped else template + '$'
        expected_file_name_regex = date_utils.insert_date_into_template(
            tmplt, kwargs, day_offset=ALLSCRIPTS_DX_DAY_OFFSET
        )

        return [
            file_dir + f for f in os.listdir(file_dir)
            if re.search(expected_file_name_regex, f)
        ]

    return out

def encrypted_decrypted_file_paths_function(ds, kwargs):
    encrypted_file_paths = get_file_paths_func(TRANSACTION_CLAIM_FILE_NAME_TEMPLATE)(ds, kwargs)
    return [[enc, enc + '.gz'] for enc in encrypted_file_paths]

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
                    path_template + '.zip', day_offset=ALLSCRIPTS_DX_DAY_OFFSET
                ),
                'file_name_pattern_func'  : date_utils.generate_insert_regex_into_template_function(
                    path_template + '.zip'
                ),
                'minimum_file_size'       : minimum_file_size,
                's3_prefix'               : '/'.join(S3_TRANSACTION_RAW_URL.split('/')[3:]),
                's3_bucket'               : 'healthverity',
                'file_description'        : 'Allscripts DX ' + task_id + ' file',
                'regex_name_match'        : True
            }
        ),
        task_id='validate_' + task_id,
        dag=mdag
    )


validate_claims = generate_file_validation_task(
    'claims', TRANSACTION_CLAIM_FILE_NAME_TEMPLATE,
    100000
)
validate_service_lines = generate_file_validation_task(
    'service_lines', TRANSACTION_SVC_FILE_NAME_TEMPLATE,
    50000
)
validate_deid = generate_file_validation_task(
    'deid', DEID_FILE_NAME_TEMPLATE,
    100000
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
                    path_template + '.zip', day_offset=ALLSCRIPTS_DX_DAY_OFFSET
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

fetch_claims = generate_file_fetch_task(
    'claims', TRANSACTION_CLAIM_FILE_NAME_TEMPLATE
)
fetch_service_lines = generate_file_fetch_task(
    'service_lines', TRANSACTION_SVC_FILE_NAME_TEMPLATE
)
fetch_deid = generate_file_fetch_task(
    'deid', DEID_FILE_NAME_TEMPLATE
)

def do_decompress_files(ds, **kwargs):
    tmp_dir = get_tmp_dir(ds, kwargs)
    files = get_file_paths_func(TRANSACTION_CLAIM_FILE_NAME_TEMPLATE, True)(ds, kwargs) + \
        get_file_paths_func(TRANSACTION_SVC_FILE_NAME_TEMPLATE, True)(ds, kwargs) + \
        get_file_paths_func(DEID_FILE_NAME_TEMPLATE, True)(ds, kwargs)
    for f in files:
        decompression.decompress_zip_file(f, tmp_dir)

decompress_files = PythonOperator(
    task_id='docompress_files',
    provide_context=True,
    python_callable=do_decompress_files,
    dag=mdag
)

decrypt_claims = SubDagOperator(
    subdag=decrypt_files.decrypt_files(
        DAG_NAME,
        'decrypt_claims',
        default_args['start_date'],
        mdag.schedule_interval,
        {
            'tmp_dir_func'                        : get_tmp_dir,
            'encrypted_decrypted_file_paths_func' : encrypted_decrypted_file_paths_function
        }
    ),
    task_id='decrypt_claims',
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
                        S3_TRANSACTION_PROCESSED_URL_TEMPLATE + subdir + '/', day_offset=ALLSCRIPTS_DX_DAY_OFFSET
                    ),
                'split_size'               : '20M'
            }
        ),
        task_id='split_' + task_id + '_files',
        dag=mdag
    )

split_claims = generate_file_split_task(
    'claims', TRANSACTION_CLAIM_FILE_NAME_TEMPLATE, 'header'
)
split_service_lines = generate_file_split_task(
    'service_lines', TRANSACTION_SVC_FILE_NAME_TEMPLATE, 'serviceline'
)

queue_up_for_matching = SubDagOperator(
    subdag=queue_up_for_matching.queue_up_for_matching(
        DAG_NAME,
        'queue_up_for_matching',
        default_args['start_date'],
        mdag.schedule_interval,
        {
            'source_files_func' : get_file_paths_func(DEID_FILE_NAME_TEMPLATE)
        }
    ),
    task_id='queue_up_for_matching',
    dag=mdag
)

#
# Post-Matching
#
def norm_args(ds, k):
    base = ['--date', date_utils.insert_date_into_template('{}-{}-{}', k, day_offset=ALLSCRIPTS_DX_DAY_OFFSET)]
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
                path.split('/')[-1] for path in get_file_paths_func(DEID_FILE_NAME_TEMPLATE)
            ],
            'file_date_func'                    :
                date_utils.generate_insert_date_into_template_function('{}/{}/{}'),
            's3_payload_loc_url'                : S3_PAYLOAD_DEST,
            'vendor_uuid'                       : '0b6cc05b-bff3-4365-b229-8d06480ad4a3',
            'pyspark_normalization_script_name' : '/home/hadoop/spark/providers/allscripts/medicalclaims/sparkNormalizeMcKessonRx.py',
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
    for t in ['validate_claims', 'validate_service_lines',
            'validate_deid', 'decrypt_claims', 'queue_up_for_matching']:
        del mdag.task_dict[t]
        globals()[t] = DummyOperator(
            task_id = t,
            dag = mdag
        )

fetch_claims.set_upstream(validate_claims)
fetch_service_lines.set_upstream(validate_service_lines)
fetch_deid.set_upstream(validate_deid)
decompress_files.set_upstream([fetch_claims, fetch_service_lines, fetch_deid])
decrypt_claims.set_upstream(decompress_files)
split_claims.set_upstream(decrypt_claims)
split_service_lines.set_upstream(decompress_files)
queue_up_for_matching.set_upstream(decompress_files)
detect_move_normalize_dag.set_upstream(
    [queue_up_for_matching, split_claims, split_service_lines]
)
update_analytics_db.set_upstream(detect_move_normalize_dag)
clean_up_workspace.set_upstream(detect_move_normalize_dag)
