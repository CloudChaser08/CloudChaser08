from airflow.models import Variable
from airflow.operators import PythonOperator, SubDagOperator
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

import util.decompression as decompression
import util.date_utils as date_utils
import util.s3_utils as s3_utils

for m in [s3_validate_file, s3_fetch_file, decrypt_files,
        split_push_files, queue_up_for_matching,
        detect_move_normalize, decompression, HVDAG,
        date_utils, s3_utils]:
    reload(m)

# Applies to all files
TMP_PATH_TEMPLATE = '/tmp/allscripts/emr/{}{}{}/'
DAG_NAME = 'allscripts_emr_pipeline'

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2017, 12, 22),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2)
}

mdag = HVDAG.HVDAG(
    dag_id=DAG_NAME,
    schedule_interval="0 0 22 * *",
    default_args=default_args
)

ALLSCRIPTS_EMR_MONTH_OFFSET = 0

# Applies to all transaction files
if HVDAG.HVDAG.airflow_env == 'test':
    S3_TRANSACTION_RAW_URL = 's3://salusv/testing/dewey/airflow/e2e/allscripts/emr/raw/'
    S3_TRANSACTION_PROCESSED_URL_TEMPLATE = 's3://salusv/testing/dewey/airflow/e2e/allscripts/emr/out/{}/{}/'
    S3_PAYLOAD_DEST = 's3://salusv/testing/dewey/airflow/e2e/allscripts/emr/payload/'
else:
    S3_TRANSACTION_RAW_URL = 's3://healthverity/incoming/allscripts/'
    S3_TRANSACTION_PROCESSED_URL_TEMPLATE = 's3://salusv/incoming/emr/allscripts/{}/{}/'
    S3_PAYLOAD_DEST = 's3://salusv/matching/payload/emr/allscripts/'

# Transaction ZIP
TRANSACTION_FILE_DESCRIPTION = 'Allscripts EMR transaction ZIP file'
TRANSACTION_FILE_NAME_TEMPLATE = 'Allscripts_{1}{0}_[0-9]{{7}}_01.zip'
MINIMUM_TRANSACTION_FILE_SIZE = 5000000000

# Deid file
DEID_FILE_DESCRIPTION = 'Allscripts EMR deid file'
DEID_FILE_NAME_TEMPLATE = 'Allscripts_HV_{1}{0}_[0-9]{{7}}_01.dat.zip'
DEID_FILE_NAME_UNZIPPED_TEMPLATE = 'Allscripts_HV_{1}{0}_[0-9]{{7}}_01.dat'
MINIMUM_DEID_FILE_SIZE = 5000000000

get_tmp_dir = date_utils.generate_insert_date_into_template_function(
    TMP_PATH_TEMPLATE
)


def get_deid_file_name(ds, k):
    deid_regex = date_utils.insert_date_into_template(
        DEID_FILE_NAME_TEMPLATE, k, month_format='%b', year_format='%y', month_offset = ALLSCRIPTS_EMR_MONTH_OFFSET
    )
    s3_keys = s3_utils.list_s3_bucket_files(
        S3_TRANSACTION_RAW_URL
    )
    return [
        S3_TRANSACTION_RAW_URL + k for k in s3_keys if re.search(deid_regex, k)
    ]


def generate_transaction_file_validation_dag(
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
                    path_template, month_format='%b', year_format='%y', month_offset = ALLSCRIPTS_EMR_MONTH_OFFSET
                ),
                'file_name_pattern_func'  : date_utils.generate_insert_date_into_template_function(
                    path_template, month_format='%b', year_format='%y'
                ),
                'minimum_file_size'       : minimum_file_size,
                's3_prefix'               : '/'.join(S3_TRANSACTION_RAW_URL.split('/')[3:]),
                's3_bucket'               : S3_TRANSACTION_RAW_URL.split('/')[2],
                'file_description'        : 'Allscripts EMR ' + task_id + ' file',
                'regex_name_match'        : True
            }
        ),
        task_id='validate_' + task_id + '_file',
        dag=mdag
    )

if HVDAG.HVDAG.airflow_env != 'test':
    validate_transaction = generate_transaction_file_validation_dag(
        'transaction', TRANSACTION_FILE_NAME_TEMPLATE, MINIMUM_TRANSACTION_FILE_SIZE
    )
    validate_deid = generate_transaction_file_validation_dag(
        'deid', DEID_FILE_NAME_TEMPLATE, MINIMUM_DEID_FILE_SIZE
    )

fetch_transaction = SubDagOperator(
    subdag=s3_fetch_file.s3_fetch_file(
        DAG_NAME,
        'fetch_transaction_file',
        default_args['start_date'],
        mdag.schedule_interval,
        {
            'tmp_path_template'      : TMP_PATH_TEMPLATE,
            'expected_file_name_func': date_utils.generate_insert_date_into_template_function(
                TRANSACTION_FILE_NAME_TEMPLATE, month_format='%b', year_format='%y', month_offset = ALLSCRIPTS_EMR_MONTH_OFFSET
            ),
            's3_prefix'              : '/'.join(S3_TRANSACTION_RAW_URL.split('/')[3:]),
            's3_bucket'              : S3_TRANSACTION_RAW_URL.split('/')[2],
            'regex_name_match'       : True
        }
    ),
    task_id='fetch_transaction_file',
    dag=mdag
)


def unzip_step():
    def execute(ds, **kwargs):
        tmp_dir = get_tmp_dir(ds, kwargs)
        file_name = os.listdir(tmp_dir)[0]
        decompression.decompress_7z_file(
            tmp_dir + file_name, tmp_dir, Variable.get("ALLSCRIPTS_EMR_ZIP_PASSWORD")
        )
        os.remove(tmp_dir + file_name)
    return PythonOperator(
        task_id='unzip_transaction_file',
        provide_context=True,
        python_callable=execute,
        dag=mdag
    )


unzip_transaction = unzip_step()


def split_step(task_id, filename, s3_destination_template):
    return SubDagOperator(
        subdag=split_push_files.split_push_files(
            DAG_NAME,
            'split_' + task_id + '_file',
            default_args['start_date'],
            mdag.schedule_interval,
            {
                'tmp_dir_func'             : get_tmp_dir,
                'parts_dir_func'           : lambda ds, k: task_id,
                'file_paths_to_split_func' : lambda ds, k: [
                    get_tmp_dir(ds, k) + '/' + filename
                ],
                'file_name_pattern_func'  : lambda ds, k: task_id,
                's3_prefix_func'           : date_utils.generate_insert_date_into_template_function(
                    s3_destination_template, month_offset = ALLSCRIPTS_EMR_MONTH_OFFSET
                ),
                'num_splits'               : 20
            }
        ),
        task_id='split_' + task_id + '_file',
        dag=mdag
    )


split_tasks = [
    split_step('allergies', 'Allergies.txt', S3_TRANSACTION_PROCESSED_URL_TEMPLATE + 'allergies/'),
    split_step('appointments', 'Appointments.txt', S3_TRANSACTION_PROCESSED_URL_TEMPLATE + 'appointments/'),
    split_step('clients', 'Clients.txt', S3_TRANSACTION_PROCESSED_URL_TEMPLATE + 'clients/'),
    split_step('encounters', 'Encounters.txt', S3_TRANSACTION_PROCESSED_URL_TEMPLATE + 'encounters/'),
    split_step('fillrates', 'FillRates.txt', S3_TRANSACTION_PROCESSED_URL_TEMPLATE + 'fillrates/'),
    split_step('medications', 'Medications.txt', S3_TRANSACTION_PROCESSED_URL_TEMPLATE + 'medications/'),
    split_step('orders', 'Orders.txt', S3_TRANSACTION_PROCESSED_URL_TEMPLATE + 'orders/'),
    split_step('patientdemographics', 'PatientDemographics.txt', S3_TRANSACTION_PROCESSED_URL_TEMPLATE + 'patientdemographics/'),
    split_step('problems', 'Problems.txt', S3_TRANSACTION_PROCESSED_URL_TEMPLATE + 'problems/'),
    split_step('providers', 'Providers.txt', S3_TRANSACTION_PROCESSED_URL_TEMPLATE + 'providers/'),
    split_step('results', 'Results.txt', S3_TRANSACTION_PROCESSED_URL_TEMPLATE + 'results/'),
    split_step('rowcounts', 'RowCounts.txt', S3_TRANSACTION_PROCESSED_URL_TEMPLATE + 'rowcounts/'),
    split_step('vaccines', 'Vaccines.txt', S3_TRANSACTION_PROCESSED_URL_TEMPLATE + 'vaccines/'),
    split_step('vitals', 'Vitals.txt', S3_TRANSACTION_PROCESSED_URL_TEMPLATE + 'vitals/')
]


def clean_up_workspace_step():
    def execute(ds, **kwargs):
        check_call([
            'rm', '-rf', get_tmp_dir(ds, kwargs)
        ])
    return PythonOperator(
        task_id='clean_up_workspace',
        provide_context=True,
        python_callable=execute,
        trigger_rule='all_done',
        dag=mdag
    )


clean_up_workspace = clean_up_workspace_step()

if HVDAG.HVDAG.airflow_env != 'test':
    queue_up_for_matching = SubDagOperator(
        subdag=queue_up_for_matching.queue_up_for_matching(
            DAG_NAME,
            'queue_up_for_matching',
            default_args['start_date'],
            mdag.schedule_interval,
            {
                'source_files_func' : get_deid_file_name
            }
        ),
        task_id='queue_up_for_matching',
        dag=mdag
    )


#
# Post-Matching
#
def norm_args(ds, k):
    base = ['--date', date_utils.insert_date_into_template('{}-{}-{}', k, month_offset = ALLSCRIPTS_EMR_MONTH_OFFSET)]
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
                    '_'.join(DEID_FILE_NAME_UNZIPPED_TEMPLATE.split('_')[:3]), k, month_format='%b', year_format='%y',
                    month_offset = ALLSCRIPTS_EMR_MONTH_OFFSET
                )
            ],
            'file_date_func'                    : date_utils.generate_insert_date_into_template_function(
                '{}/{}', month_offset = ALLSCRIPTS_EMR_MONTH_OFFSET
            ),
            's3_payload_loc_url'                : S3_PAYLOAD_DEST,
            'vendor_uuid'                       : '0b6cc05b-bff3-4365-b229-8d06480ad4a3',
            'pyspark_normalization_script_name' : '/home/hadoop/spark/providers/allscripts/emr/sparkNormalizeAllscriptsEMR.py',
            'pyspark_normalization_args_func'   : norm_args,
            'pyspark'                           : True,
            'cluster_identifier'                : '0b6cc05b-bff3-4365-b229-8d06480ad4a3-emr',
            'emr_node_type': 'm4.4xlarge'
        }
    ),
    task_id='detect_move_normalize',
    dag=mdag
)


if HVDAG.HVDAG.airflow_env != 'test':
    fetch_transaction.set_upstream(validate_transaction)
    queue_up_for_matching.set_upstream(validate_deid)
    detect_move_normalize_dag.set_upstream(
        split_tasks + [queue_up_for_matching]
    )
else:
    detect_move_normalize_dag.set_upstream(
        split_tasks
    )

unzip_transaction.set_upstream(fetch_transaction)
unzip_transaction.set_downstream(split_tasks)
clean_up_workspace.set_upstream(
    split_tasks
)
