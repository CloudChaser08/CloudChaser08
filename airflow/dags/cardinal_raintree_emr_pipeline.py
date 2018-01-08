from airflow.operators import SubDagOperator
from datetime import datetime, timedelta
import os
import re

# hv-specific modules
import common.HVDAG as HVDAG
import subdags.s3_validate_file as s3_validate_file
import subdags.s3_fetch_file as s3_fetch_file
import subdags.s3_push_files as s3_push_files
import subdags.decrypt_files as decrypt_files
import subdags.split_push_files as split_push_files
import subdags.detect_move_normalize as detect_move_normalize
import subdags.clean_up_tmp_dir as clean_up_tmp_dir

import util.decompression as decompression
import util.date_utils as date_utils

for m in [s3_validate_file, s3_fetch_file, s3_push_files,
          decrypt_files, split_push_files, clean_up_tmp_dir,
          detect_move_normalize, decompression, HVDAG,
          date_utils]:
    reload(m)

# Applies to all files
TMP_PATH_TEMPLATE = '/tmp/cardinal/emr/{}{}{}/'
DAG_NAME = 'cardinal_raintree_emr_pipeline'

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2017, 6, 5, 14),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2)
}

mdag = HVDAG.HVDAG(
    dag_id=DAG_NAME,
    schedule_interval="0 14 * * *",
    default_args=default_args
)

# Applies to all transaction files
if HVDAG.HVDAG.airflow_env == 'test':
    S3_TRANSACTION_RAW_URL = 's3://salusv/testing/dewey/airflow/e2e/cardinal/emr/raw/'
    S3_TRANSACTION_INTERIM_URL = 's3://salusv/testing/dewey/airflow/e2e/cardinal/emr/healthverity/incoming/'
    S3_TRANSACTION_PROCESSED_URL_TEMPLATE = 's3://salusv/testing/dewey/airflow/e2e/cardinal/emr/out/{}/{}/{}/'
else:
    S3_TRANSACTION_RAW_URL = 's3://hvincoming/cardinal_raintree/emr/'
    S3_TRANSACTION_INTERIM_URL = 's3://healthverity/incoming/cardinal/emr/'
    S3_TRANSACTION_PROCESSED_URL_TEMPLATE = 's3://salusv/incoming/emr/cardinal/{}/{}/{}/'

# Transaction Files
TRANSACTION_TMP_PATH_TEMPLATE = TMP_PATH_TEMPLATE + 'raw/'

# Transaction demographics
TRANSACTION_DEMO_FILE_DESCRIPTION = 'Cardinal Raintree EMR transaction demographics file'
TRANSACTION_DEMO_FILE_NAME_TEMPLATE = 'Demographic_records_{}{}{}[0-9]{{6}}.dat'

# Transaction diagnosis
TRANSACTION_DIAG_FILE_DESCRIPTION = 'Cardinal Raintree EMR transaction diagnosis file'
TRANSACTION_DIAG_FILE_NAME_TEMPLATE = 'Diagnosis_record_data_{}{}{}[0-9]{{6}}.dat'

# Transaction lab
TRANSACTION_LAB_FILE_DESCRIPTION = 'Cardinal Raintree EMR transaction lab file'
TRANSACTION_LAB_FILE_NAME_TEMPLATE = 'Lab_record_data_{}{}{}[0-9]{{6}}.dat'

# Transaction encounter
TRANSACTION_ENC_FILE_DESCRIPTION = 'Cardinal Raintree EMR transaction encounter file'
TRANSACTION_ENC_FILE_NAME_TEMPLATE = 'Encounter_record_data_{}{}{}[0-9]{{6}}.dat'

# Transaction dispense
TRANSACTION_DISP_FILE_DESCRIPTION = 'Cardinal Raintree EMR transaction dispense file'
TRANSACTION_DISP_FILE_NAME_TEMPLATE = 'Order_Dispense_record_data_{}{}{}[0-9]{{6}}.dat'

get_tmp_dir = date_utils.generate_insert_date_into_template_function(TRANSACTION_TMP_PATH_TEMPLATE)

CARDINAL_DAY_OFFSET = 1

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
                'expected_file_name_func' : date_utils.generate_insert_date_into_template_function(
                    path_template, day_offset=CARDINAL_DAY_OFFSET
                ),
                'file_name_pattern_func'  : date_utils.generate_insert_regex_into_template_function(
                    path_template, day_offset=CARDINAL_DAY_OFFSET
                ),
                'regex_name_match'        : True,
                'minimum_file_size'       : minimum_file_size,
                's3_prefix'               : '/'.join(S3_TRANSACTION_RAW_URL.split('/')[3:]),
                's3_bucket'               : 'healthverity',
                'file_description'        : 'Cardinal Raintree EMR ' + task_id + ' file'
            }
        ),
        task_id='validate_' + task_id + '_file',
        dag=mdag
    )


if HVDAG.HVDAG.airflow_env != 'test':
    validate_transaction_demo = generate_file_validation_dag(
        'transaction_demo', TRANSACTION_DEMO_FILE_NAME_TEMPLATE,
        10000
    )
    validate_transaction_diag = generate_file_validation_dag(
        'transaction_diag', TRANSACTION_DIAG_FILE_NAME_TEMPLATE,
        10000
    )
    validate_transaction_lab = generate_file_validation_dag(
        'transaction_lab', TRANSACTION_LAB_FILE_NAME_TEMPLATE,
        10000
    )
    validate_transaction_enc = generate_file_validation_dag(
        'transaction_enc', TRANSACTION_ENC_FILE_NAME_TEMPLATE,
        10000
    )
    validate_transaction_disp = generate_file_validation_dag(
        'transaction_disp', TRANSACTION_DISP_FILE_NAME_TEMPLATE,
        10000
    )


def generate_fetch_transaction_dag(task_id, transaction_file_name_template):
    return SubDagOperator(
        subdag=s3_fetch_file.s3_fetch_file(
            DAG_NAME,
            'fetch_{}_file'.format(task_id),
            default_args['start_date'],
            mdag.schedule_interval,
            {
                'tmp_path_template'      : TRANSACTION_TMP_PATH_TEMPLATE,
                'expected_file_name_func': date_utils.generate_insert_date_into_template_function(
                    transaction_file_name_template, day_offset=CARDINAL_DAY_OFFSET
                ),
                'regex_name_match'       : True,
                's3_prefix'              : '/'.join(S3_TRANSACTION_RAW_URL.split('/')[3:]),
                's3_bucket'              : S3_TRANSACTION_RAW_URL.split('/')[2]
            }
        ),
        task_id='fetch_{}_file'.format(task_id),
        dag=mdag
    )


fetch_transaction_demo = generate_fetch_transaction_dag(
    'transaction_demo', TRANSACTION_DEMO_FILE_NAME_TEMPLATE,
)
fetch_transaction_diag = generate_fetch_transaction_dag(
    'transaction_diag', TRANSACTION_DIAG_FILE_NAME_TEMPLATE,
)
fetch_transaction_lab = generate_fetch_transaction_dag(
    'transaction_lab', TRANSACTION_LAB_FILE_NAME_TEMPLATE,
)
fetch_transaction_enc = generate_fetch_transaction_dag(
    'transaction_enc', TRANSACTION_ENC_FILE_NAME_TEMPLATE,
)
fetch_transaction_disp = generate_fetch_transaction_dag(
    'transaction_disp', TRANSACTION_DISP_FILE_NAME_TEMPLATE,
)


def generate_push_to_incoming_dag():
    return SubDagOperator(
        subdag=s3_push_files.s3_push_files(
            DAG_NAME,
            'push_raw_transactions_to_incoming',
            default_args['start_date'],
            mdag.schedule_interval,
            {
                'file_paths_func'      : lambda ds, k: [
                    get_tmp_dir(ds, k) + f for f in os.listdir(get_tmp_dir(ds, k))
                ],
                's3_prefix_func'       : lambda ds, k: '/'.join(S3_TRANSACTION_INTERIM_URL.split('/')[3:]),
                's3_bucket'            : S3_TRANSACTION_INTERIM_URL.split('/')[2]
            }
        ),
        task_id='push_raw_transactions_to_incoming',
        dag=mdag
    )

push_raw_transactions_to_incoming = generate_push_to_incoming_dag()


def generate_decrypt_dag():
    return SubDagOperator(
        subdag=decrypt_files.decrypt_files(
            DAG_NAME,
            'decrypt_files',
            default_args['start_date'],
            mdag.schedule_interval,
            {
                'tmp_dir_func'                        : get_tmp_dir,
                'encrypted_decrypted_file_paths_func' : lambda ds, k: [
                    [get_tmp_dir(ds, k) + f, get_tmp_dir(ds, k) + f + '.gz']
                    for f in os.listdir(get_tmp_dir(ds, k)) if f.endswith('.dat')
                ]
            }
        ),
        task_id='decrypt_files',
        dag=mdag
    )


decrypt_files = generate_decrypt_dag()


def generate_split_dag(task_id, file_template, destination_template):
    return SubDagOperator(
        subdag=split_push_files.split_push_files(
            DAG_NAME,
            'split_{}_files'.format(task_id),
            default_args['start_date'],
            mdag.schedule_interval,
            {
                'tmp_dir_func'             : get_tmp_dir,
                'parts_dir_func'           : lambda ds, k: '{}_parts'.format(task_id),
                'file_paths_to_split_func' : lambda ds, k: [
                    get_tmp_dir(ds, k) + [
                        f for f in os.listdir(get_tmp_dir(ds, k)) if re.search(
                            date_utils.insert_date_into_template(file_template, k, day_offset=CARDINAL_DAY_OFFSET), f
                        )
                    ][0]
                ],
                's3_prefix_func'           : date_utils.generate_insert_date_into_template_function(
                    destination_template, day_offset=CARDINAL_DAY_OFFSET
                ),
                'num_splits'               : 20
            }
        ),
        task_id='split_{}_files'.format(task_id),
        dag=mdag
    )


split_transaction_demo = generate_split_dag(
    'transaction_demo', TRANSACTION_DEMO_FILE_NAME_TEMPLATE,
    S3_TRANSACTION_PROCESSED_URL_TEMPLATE + 'demographics/'
)
split_transaction_diag = generate_split_dag(
    'transaction_diag', TRANSACTION_DIAG_FILE_NAME_TEMPLATE,
    S3_TRANSACTION_PROCESSED_URL_TEMPLATE + 'diagnosis/'
)
split_transaction_lab = generate_split_dag(
    'transaction_lab', TRANSACTION_LAB_FILE_NAME_TEMPLATE,
    S3_TRANSACTION_PROCESSED_URL_TEMPLATE + 'lab/'
)
split_transaction_enc = generate_split_dag(
    'transaction_enc', TRANSACTION_ENC_FILE_NAME_TEMPLATE,
    S3_TRANSACTION_PROCESSED_URL_TEMPLATE + 'encounter/'
)
split_transaction_disp = generate_split_dag(
    'transaction_disp', TRANSACTION_DISP_FILE_NAME_TEMPLATE,
    S3_TRANSACTION_PROCESSED_URL_TEMPLATE + 'dispense/'
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


detect_move_normalize_dag = SubDagOperator(
    subdag=detect_move_normalize.detect_move_normalize(
        DAG_NAME,
        'detect_move_normalize',
        default_args['start_date'],
        mdag.schedule_interval,
        {
            'expected_matching_files_func'      : lambda ds, k: [],
            'file_date_func'                    : date_utils.generate_insert_date_into_template_function(
                '{}/{}/{}', day_offset=CARDINAL_DAY_OFFSET
            ),
            'vendor_uuid'                       : '46d06413-e37d-4978-9194-8623516223cc',
            'pyspark_normalization_script_name' : '/home/hadoop/spark/providers/cardinal/emr/sparkNormalizeCardinalEMR.py',
            'pyspark_normalization_args_func'   : norm_args,
            'pyspark'                           : True
        }
    ),
    task_id='detect_move_normalize',
    dag=mdag
)

# addon
if HVDAG.HVDAG.airflow_env != 'test':
    fetch_transaction_demo.set_upstream(validate_transaction_demo)
    fetch_transaction_diag.set_upstream(validate_transaction_diag)
    fetch_transaction_disp.set_upstream(validate_transaction_disp)
    fetch_transaction_enc.set_upstream(validate_transaction_enc)
    fetch_transaction_lab.set_upstream(validate_transaction_lab)

push_raw_transactions_to_incoming.set_upstream([
    fetch_transaction_demo,
    fetch_transaction_diag,
    fetch_transaction_disp,
    fetch_transaction_enc,
    fetch_transaction_lab
])

decrypt_files.set_upstream(push_raw_transactions_to_incoming)
decrypt_files.set_downstream([
    split_transaction_demo,
    split_transaction_diag,
    split_transaction_disp,
    split_transaction_enc,
    split_transaction_lab
])

clean_up_workspace.set_upstream([
    split_transaction_demo,
    split_transaction_diag,
    split_transaction_disp,
    split_transaction_enc,
    split_transaction_lab
])

detect_move_normalize_dag.set_upstream([
    split_transaction_demo,
    split_transaction_diag,
    split_transaction_disp,
    split_transaction_enc,
    split_transaction_lab
])
p
