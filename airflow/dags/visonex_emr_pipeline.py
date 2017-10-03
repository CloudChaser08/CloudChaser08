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
import subdags.s3_push_files as s3_push_files
import subdags.decrypt_files as decrypt_files
import subdags.split_push_files as split_push_files
import subdags.queue_up_for_matching as queue_up_for_matching
import subdags.detect_move_normalize as detect_move_normalize
import subdags.clean_up_tmp_dir as clean_up_tmp_dir
import subdags.update_analytics_db as update_analytics_db
import util.s3_utils as s3_utils

for m in [s3_validate_file, s3_fetch_file, decrypt_files,
          split_push_files, queue_up_for_matching, clean_up_tmp_dir,
          detect_move_normalize, HVDAG, s3_utils, update_analytics_db]:
    reload(m)

# Applies to all files
DAG_NAME = 'visonex_pipeline'
TMP_PATH_TEMPLATE = '/tmp/visonex/emr/{}{}{}/'

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2017, 9, 15, 12), # Not sure when this will start yet
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2)
}

mdag = HVDAG.HVDAG(
    dag_id = DAG_NAME,
    schedule_interval = '0 12 * * 5',   # Not sure about schedule yet
    default_args = default_args
)

if HVDAG.HVDAG.airflow_env == 'test':
    S3_TRANSACTION_RAW_URL = 's3://salusv/testing/dewey/airflow/e2e/visonex/emr/raw/'
    S3_TRANSACTION_PROCESSED_URL_TEMPLATE = 's3://salusv/testing/dewey/airflow/e2e/visonex/emr/out/{}/{}/{}/'
    S3_PAYLOAD_DEST = 's3://salusv/testing/dewey/airflow/e2e/visonex/emr/payload/'
else:
    S3_TRANSACTION_RAW_URL = 's3://healthverity/incoming/visonex/'
    S3_TRANSACTION_PROCESSED_URL_TEMPLATE = 's3://salusv/incoming/emr/visonex/{}/{}/{}/'
    S3_PAYLOAD_DEST = 's3://salusv/matching/payload/emr/visonex/'

# Transaction Addon file
TRANSACTION_TMP_PATH_TEMPLATE = TMP_PATH_TEMPLATE + 'raw/'
TRANSACTION_FILE_DESCRIPTION = 'Visonex EMR transaction file'
TRANSACTION_FILE_NAME_TEMPLATE = 'HealthVerity-{}{}{}.zip'

# Deid file
DEID_FILE_DESCRIPTION = 'Visonex EMR deid file'
DEID_FILE_NAME_TEMPLATE = 'HealthVerity-{}{}{}.zip'

def get_file_date_nodash(kwargs):
    return (kwargs['execution_date'] + timedelta(days=7)).strftime('%Y%m%d') # Interval unclear

def insert_file_date_function(template):
    def out(ds, kwargs)
        ds_nodash = get_file_date_nodash(kwargs)
        return template.format(
            ds_nodash[0:4],
            ds_nodash[4:6],
            ds_nodash[6:8]
        )
    return out

def insert_file_date(template, ds, kwargs):
    return insert_file_date_function(template)(ds, kwargs)

def insert_execution_date_function(template):
    def out(ds, kwargs):
        return template.format(
            kwargs['ds_nodash'][0:4],
            kwargs['ds_nodash'][4:6],
            kwargs['ds_nodash'][6:8]
        )
    return out

def insert_execution_date(template, ds, kwargs):
    return insert_execution_date_function(template)(ds, kwargs)

get_tmp_dir = insert_execution_date_function(TRANSACTION_TMP_PATH_TEMPLATE)

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
                'expected_file_name_func' : insert_file_date_function(
                    path_template
                ),
                'file_name_pattern_func'  : lambda ds, kwargs: path_template.format('\\d{8}'),
                'minimum_file_size'       : minimum_file_size,
                's3_prefix'               : '/'.join(S3_TRANSACTION_RAW_URL.split('/')[3:]),
                's3_bucket'               : 'healthverity',
                'file_description'        : 'Visonex EMR ' + task_id + ' file'
            }
        ),
        task_id = 'validate_' + task_id + '_file',
        retries = 3,
        retry_delay = timedelta(minutes = 15),
        dag = mdag
    )


if HVDAG.HVDAG.airflow_env != 'test':
    validate_transaction = generate_file_validation_task(
        'transaction', TRANSACTION_FILE_NAME_TEMPLATE,
        10000
    )
    validate_deid = generate_file_validation_task(
        'deid', DEID_FILE_NAME_TEMPLATE,
        10000
    )

fetch_transaction = SubDagOperator(
    subdag = s3_fetch_file.s3_fetch_file(
        DAG_NAME,
        'fetch_transaction_file',
        default_args['start_date'],
        mdag.schedule_interval,
        {
            'tmp_path_template'         : TRANSACTION_TMP_PATH_TEMPLATE,
            'expected_file_name_func'   : insert_file_date_function(
                TRANSACTION_FILE_NAME_TEMPLATE
            ),
            'regex_name_match'          : True,
            's3_prefix'                 : '/'.join(S3_TRANSACTION_RAW_URL.split('/')[3:]),
            's3_bucket'                 : 'salusv' if HVDAG.HVDAG.airflow_env == 'test' else 'healthverity'
        }
    ),
    task_id = 'fetch_transaction_file',
    dag = mdag
)

def get_transaction_file_paths(ds, kwargs):
    return [get_tmp_dir(ds, kwargs) + insert_file_date(TRANSACTION_FILE_NAME_TEMPLATE, ds, kwargs)]

def do_decompress_transactions(ds, **kwargs):
    decompression.decompress_7z_file(get_transaction_file_paths[0], get_tmp_dir() + 'decompressed/')

decompress_transactions = PythonOperator(
    task_id='decompress_transactions',
    python_callable=do_decompress_transactions,
    provide_context=True,
    dag=mdag
)

def get_decompressed_transaction_file_paths(ds, kwargs):
    file_dir = get_tmp_dir(ds, kwargs) + 'decompressed/'
    return [file_dir + f for f in os.listdir(file_dir)]

# file to push format Xport_HealthVerity.dbo.{table}.csv.*
def get_s3_prefix(ds, kwargs):
    FILE_PREFIX = 'Xport_HealthVerity.dbo.'
    table = kwargs['file_to_push'].replace(FILE_PREFIX, '').split('.')[0]
    return insert_file_date(
        S3_TRANSACTION_PROCESSED_URL_TEMPLATE + table.lower() + '/', ds, kwargs
    )

split_transactions = SubDagOperator(
    subdag=split_push_files.split_push_files(
        DAG_NAME,
        'split_transaction_files',
        default_args['start_date'],
        mdag.schedule_interval,
        {
            'tmp_dir_func'             : lambda ds, kwargs: get_tmp_dir() + 'decompressed/',
            'file_paths_to_split_func' : get_decompressed_transaction_file_paths,
            's3_prefix_func'           : get_s3_prefix,
            'num_splits'               : 1
        }
    ),
    task_id='split_transaction_files',
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

def get_deid_file_urls(ds, kwargs):
    return [S3_TRANSACTION_RAW_URL + insert_file_date(DEID_FILE_NAME_TEMPLATE, ds, kwargs)]

if HVDAG.HVDAG.airflow_env != 'test':
    queue_up_for_matching = SubDagOperator(
        subdag=queue_up_for_matching.queue_up_for_matching(
            DAG_NAME,
            'queue_up_for_matching',
            default_args['start_date'],
            mdag.schedule_interval,
            {
                'source_files_func' : get_deid_file_urls,
                'priority'          : 'priority3'
            }
        ),
        task_id='queue_up_for_matching',
        dag=mdag
    )

#
# Post-Matching
#
def get_deid_file_names(ds, kwargs):
    return [insert_file_date(DEID_FILE_NAME_TEMPLATE, ds, kwargs)]

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
            'expected_matching_files_func'      : get_deid_file_names,
            'file_date_func'                    : insert_file_date_function(
                '{}/{}/{}'
            ),
            's3_payload_loc_url'                : S3_PAYLOAD_DEST,
            'vendor_uuid'                       : 'ef178614-a48e-4ff1-9f8b-8d8833891364',
            'pyspark_normalization_script_name' : 'spark/providers/visonex/sparkCleanUpVisonex.py',
            'pyspark_normalization_args_func'   : norm_args,
            'pyspark'                           : True
        }
    ),
    task_id='detect_move_normalize',
    dag=mdag
)

TABLES = ['address', 'clinicpreference', 'dialysistraining', 'dialysistreatment',
        'facilityadmitdischarge', 'hospitalization', 'immunization', 'insurance',
        'labidlist', 'labpanelsdrawn', 'labresult', 'medication', 'medicationgroup',
        'modalitychangehistorycrownweb', 'nursinghomehistory', 'patientaccess',
        'patientaccess_examproc', 'patientaccess_otheraccessevent',
        'patientaccess_placedrecorded', 'patientaccess_removed', 'patientallergy',
        'patientcms2728', 'patientcomorbidityandtransplantstate', 'patientdata',
        'patientdiagcodes', 'patientdialysisprescription', 'patientdialysisrxhemo',
        'patientdialysisrxpd', 'patientdialysisrxpdexchanges', 'patientevent',
        'patientfluidweightmanagement', 'patientheighthistory', 'patientinfection',
        'patientinfection_laborganism', 'patientinfection_laborganismdrug',
        'patientinfection_labresultculture', 'patientinfection_medication',
        'patientinstabilityhistory', 'patientmasterscheduleheader',
        'patientmedadministered', 'patientmednotgiven', 'patientmedprescription',
        'patientstatushistory', 'problemlist', 'sodiumufprofile', 'stategeo',
        'zipgeo']

def get_visonex_sql_commands(ds, kwargs):
    drop_template = "ALTER TABLE visonex.{} DROP PARTITION (part_best_date > '')"
    add_template = """
        ALTER TABLE visonex.{{table}} ADD PARTITION (part_best_date='{0}-{1}')
        LOCATION 's3a://salusv/warehouse/parquet/custom/2017-09-27/visonex/{{table}}/part_best_date={0}-{1}/'
    """

    return [drop_template.format(table) for table in TABLES] + \
            [insert_file_date(add_template, ds, kwargs).format(table=table) for table in TABLES]

if HVDAG.HVDAG.airflow_env != 'test':

    update_analytics_db = SubDagOperator(
        subdag=update_analytics_db.update_analytics_db(
            DAG_NAME,
            'update_analytics_db',
            default_args['start_date'],
            mdag.schedule_interval,
            {
                'sql_command_func'  : lambda ds, kwargs : '',
                'sql_commands_func' : get_visonex_sql_commands
            }
        ),
        task_id='update_analytics_db',
        dag=mdag
    )

### Dag Structure ###
if HVDAG.HVDAG.airflow_env != 'test':
    fetch_transaction.set_upstream(validate_transaction)
    queue_up_for_matching.set_upstream(validate_deid)
    detect_move_normalize_dag.set_upstream(
        [queue_up_for_matching, split_transactions]
    )
    fetch_normalized_data.set_upstream(detect_move_normalize_dag)
    update_analytics_db.set_upstream(detect_move_normalize_dag)
else:
    detect_move_normalize_dag.set_upstream(split_transaction)

decompress_transactions.set_upstream(fetch_transaction)
split_transactions.set_upstream(decompress_transactions)
clean_up_workspace.set_upstream(split_transactions)

