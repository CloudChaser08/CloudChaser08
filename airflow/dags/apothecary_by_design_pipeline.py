from airflow.models import Variable
from airflow.operators import PythonOperator, SubDagOperator
from datetime import datetime, timedelta

#hv-specific modules
import common.HVDAG as HVDAG
import subdags.s3_validate_file as s3_validate_file
import subdags.s3_fetch_file as s3_fetch_file
import subdags.decrypt_files as decrypt_files
import subdags.detect_move_normalize as detect_move_normalize
import subdags.queue_up_for_matching as queue_up_for_matching

for m in [HVDAG, s3_validate_file, s3_fetch_file, detect_move_normalize,
          queue_up_for_matching]:
    reload(m)

DAG_NAME = 'apothecary_by_design_pipeline'

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2017, 10, 8),    #TODO: determine when we start
    'retries': 3,
    'retry_delay': timedelta(minutes=2)
}

mdag = HVDAG.HVDAG(
    dag_id = DAG_NAME,
    schedule_interval = None,               #TODO: determine when we start
    default_args = default_args
)

if HVDAG.HVDAG.airflow_env == 'test':
    test_loc  = 's3://salusv/testing/dewey/airflow/e2e/apothecarybydesign/'
    S3_TRANSACTION_RAW_TXN_URL = test_loc + 'raw/transactions/'
    S3_TRANSACTION_RAW_ADD_URL = test_loc + 'raw/additionaldata/'
    S3_TRANSACTION_PROCESSED_URL_TXN_TEMPLATE = test_loc + 'out/{}/{}/{}/transactions/'
    S3_TRANSACTION_PROCESSED_URL_ADD_TEMPLATE = test_loc + 'out/{}/{}/{}/additionaldata/'
    S3_PAYLOAD_DEST = test_loc + 'payload/'
else:
    S3_TRANSACTION_RAW_TXN_URL = 's3://healthverity/incoming/pharmacyclaims/apothecarybydesign/transactions/'
    S3_TRANSACTION_RAW_ADD_URL = 's3://healthverity/incoming/pharmacyclaims/apothecarybydesign/additionaldata/'
    S3_TRANSACTION_PROCESSED_URL_TXN_TEMPLATE = 's3://salusv/incoming/pharmacyclaims/apothecarybydesign/{}/{}/{}/transactions/'
    S3_TRANSACTION_PROCESSED_URL_ADD_TEMPLATE = 's3://salusv/incoming/pharmacyclaims/apothecarybydesign/{}/{}/{}/additionaldata/'
    S3_PAYLOAD_DEST = 's3://salusv/matching/payload/pharmacyclaims/apothecarybydesign/'

TMP_PATH_TEMPLATE = '/tmp/apothecary_by_design/pharmacyclaims/{}/'
TRANSACTION_TMP_PATH_TEMPLATE = TMP_PATH_TEMPLATE + 'raw/additionaldata/'
DEID_TMP_PATH_TEMPLATE = TMP_PATH_TEMPLATE + 'raw/transactions/'

TRANSACTION_FILE_NAME_TEMPLATE = 'hv_export_data_{}.txt'   #TODO: replace when know their format
DEID_FILE_NAME_TEMPLATE = 'hv_export_po_deid_{}.txt'       #TODO: replace when know their format

def get_formatted_date(ds, kwargs):
    return kwargs['ds_nodash']


def insert_formatted_date_function(template):
    def out(ds, kwargs):
        return template.format(get_formatted_date(ds, kwargs))

    return out


def insert_current_date(template, kwargs):
    return template.format(
        kwargs['ds_nodash'][0:4],
        kwargs['ds_nodash'][4:6],
        kwargs['ds_nodash'][6:8]
    )


def insert_current_date_function(template):
    def out(ds, kwargs):
        return insert_current_date(template, kwargs)

    return out


get_tmp_dir = insert_formatted_date_function(TRANSACTION_TMP_PATH_TEMPLATE)
get_deid_tmp_dir = insert_formatted_date_function(DEID_TMP_PATH_TEMPLATE)

def get_transaction_file_paths(ds, kwargs):
    return [get_tmp_dir(ds, kwargs) + TRANSACTION_FILE_NAME_TEMPLATE.format(
        get_formatted_date(ds, kwargs)
    )]


def get_deid_file_urls(ds, kwargs):
    return [S3_TRANSACTION_RAW_TXN_URL + DEID_FILE_NAME_TEMPLATE.format(
        get_formatted_date(ds, kwargs)
    )]


def encrypted_decrypted_file_paths_function(ds, kwargs):
    file_dir = get_tmp_dir(ds, kwargs)
    encrypted_file_path = file_dir \
        + TRANSACTION_FILE_NAME_TEMPLATE.format(
            get_formatted_date(ds, kwargs)
        )
    return [
        [encrypted_file_path, encrypted_file_path + '.gz']
    ]


def encrypted_decrypted_deid_file_paths_function(ds, kwargs):
    file_dir = get_deid_tmp_dir(ds, kwargs)
    encrypted_file_path = file_dir \
            + DEID_FILE_NAME_TEMPLATE.format(
            get_formatted_date(ds, kwargs)
        )
    return [
        [encrypted_file_path, encrypted_file_path + '.gz']
    ]


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
                'expected_file_name_func' : insert_formatted_date_function(
                    path_template
                ),
                'file_name_pattern_func'  : insert_formatted_regex_function(
                    path_template
                ),
                'minimum_file_size'       : minimum_file_size,
                's3_prefix'               : '/'.join(s3_path.split('/')[3:]),
                's3_bucket'               : 'healthverity',
                'file_description'        : 'Apothecary by Design ' + task_id + ' file'
            }
        ),
        task_id='validate_' + task_id + '_file',
        dag=mdag
    )


if HVDAG.HVDAG.airflow_env != 'test':
    validate_transaction = generate_file_validation_task(
        'transaction', S3_TRANSACTION_PROCESSED_URL_TXN_TEMPLAT,
        ES3_TRANSACTION_FILE_NAME_TEMPLATE, 1000000
    )
    validate_deid = generate_file_validation_task(
        'deid', S3_TRANSACTION_PROCESSED_URL_ADD_TEMPLATE,
        DEID_FILE_NAME_TEMPLATE, 1000000
    )

fetch_deid_file = SubDagOperator(
    subdag=s3_fetch_file.s3_fetch_file(
        DAG_NAME,
        'fetch_deid_file',
        default_args['start_date'],
        mdag.schedule_interval,
        {
            'tmp_path_template'      : DEID_TMP_PATH_TEMPLATE,
            'expected_file_name_func': insert_formatted_date_function(
                DEID_FILE_NAME_TEMPLATE
            ),
            's3_prefix'              : '/'.join(S3_TRANSACTION_RAW_TXN_URL.split('/')[3:]),
            's3_bucket'              : 'salusv' if HVDAG.HVDAG.airflow_env == 'test' else 'healthverity'
        }
    ),
    task_id='fetch_deid_file',
    dag=mdag
)

fetch_additionaldata_file = SubDagOperator(
    subdag=s3_fetch_file.s3_fetch_file(
        DAG_NAME,
        'fetch_additionaldata_file',
        default_args['start_date'],
        mdag.schedule_interval,
        {
            'tmp_path_template'     : TRANSACTION_TMP_PATH_TEMPLATE,
            'expected_file_name_func'   : insert_formatted_date_function(
                TRANSACTION_FILE_NAME_TEMPLATE
            ),
            's3_prefix'                 : '/'.join(S3_TRANSACTION_RAW_ADD_URL.split('/')[3:]),
            's3_bucket'                 : 'salusv' if HVDAG.HVDAG.airflow_env == 'test' else 'healthverity'
        }
    ),
    task_id = 'fetch_additionaldata_file',
    dag=mdag
)

decrypt_deid = SubDagOperator(
    subdag=decrypt_files.decrypt_files(
        DAG_NAME,
        'decrypt_deid_file',
        default_args['start_date'],
        mdag.schedule_interval,
        {
            'tmp_dir_func'                        : get_deid_tmp_dir,
            'encrypted_decrypted_file_paths_func' : encrypted_decrypted_deid_file_paths_function
        }
    ),
    task_id='decrypt_deid_file',
    dag=mdag
)

decrypt_additionaldata = SubDagOperator(
    subdag=decrypt_files.decrypt_files(
        DAG_NAME,
        'decrypt_additionaldata_file',
        default_args['start_date'],
        mdag.schedule_interval,
        {
            'tmp_dir_func'                        : get_tmp_dir,
            'encrypted_decrypted_file_paths_func' : encrypted_decrypted_file_paths_function
        }
    ),
    task_id='decrypt_additionaldata_file',
    dag=mdag
)

### DAG STRUCTURE ###
if HVDAG.HVDAG.airflow_env != 'test':
    fetch_additionaldata_file.set_upstream(validate_transaction)
    fetch_deid_file.set_upstream(validate_deid)
    queue_up_for_matching.set_upstream(validate_deid)

decrypt_deid.set_upstream(fetch_deid_file)
decrypt_additionaldata.set_upstream(fetch_additionaldata_file)

#
#
#split_transaction = SubDagOperator(
#    subdag=split_push_files.split_push_files(
#        DAG_NAME,
#        'split_transaction_file',
#        default_args['start_date'],
#        mdag.schedule_interval,
#        {
#            'tmp_dir_func'             : get_tmp_dir,
#            'file_paths_to_split_func' : get_transaction_file_paths,
#            'file_name_pattern_func'   : insert_formatted_regex_function(
#                TRANSACTION_FILE_NAME_TEMPLATE
#            ),
#            's3_prefix_func'           : insert_current_date_function(
#                S3_TRANSACTION_PROCESSED_URL_TEMPLATE
#            ),
#            'num_splits'               : 20
#        }
#    ),
#    task_id='split_transaction_file',
#    dag=mdag
#)
#
#clean_up_workspace = SubDagOperator(
#    subdag=clean_up_tmp_dir.clean_up_tmp_dir(
#        DAG_NAME,
#        'clean_up_workspace',
#        default_args['start_date'],
#        mdag.schedule_interval,
#        {
#            'tmp_path_template': TMP_PATH_TEMPLATE
#        }
#    ),
#    task_id='clean_up_workspace',
#    dag=mdag
#)
#
#if HVDAG.HVDAG.airflow_env != 'test':
#    queue_up_for_matching = SubDagOperator(
#        subdag=queue_up_for_matching.queue_up_for_matching(
#            DAG_NAME,
#            'queue_up_for_matching',
#            default_args['start_date'],
#            mdag.schedule_interval,
#            {
#                'source_files_func' : get_deid_file_urls
#            }
#        ),
#        task_id='queue_up_for_matching',
#        dag=mdag
#    )
#
##
## Post-Matching
##
#def norm_args(ds, k):
#    base = ['--date', insert_current_date('{}-{}-{}', k)]
#    if HVDAG.HVDAG.airflow_env == 'test':
#        base += ['--airflow_test']
#
#    return base
#
#
#detect_move_normalize_dag = SubDagOperator(
#    subdag=detect_move_normalize.detect_move_normalize(
#        DAG_NAME,
#        'detect_move_normalize',
#        default_args['start_date'],
#        mdag.schedule_interval,
#        {
#            'expected_matching_files_func'      : insert_formatted_date_function(
#                DEID_FILE_NAME_TEMPLATE
#            ),
#            'file_date_func'                    : insert_current_date_function(
#                '{}/{}/{}'
#            ),
#            's3_payload_loc_url'                : S3_PAYLOAD_DEST,
#            'vendor_uuid'                       : '51ca8f88-040a-47f1-b78a-491c8632fedd',
#            'pyspark_normalization_script_name' : '/home/hadoop/spark/providers/apothecary_by_design/pharmacyclaims/sparkNormalizeApothecaryByDesign.py',
#            'pyspark_normalization_args_func'   : norm_args,
#            'pyspark'                           : True
#        }
#    ),
#    task_id='detect_move_normalize',
#    dag=mdag
#)
#
#sql_template = """
#    ALTER TABLE pharmacyclaims_20170602 ADD PARTITION (part_provider='apothecary_by_design', part_best_date='{0}-{1}')
#    LOCATION 's3a://salusv/warehouse/parquet/pharmacyclaims/2017-06-02/part_provider=apothecary_by_design/part_best_date={0}-{1}/'
#"""
#
#if HVDAG.HVDAG.airflow_env != 'test':
#    update_analytics_db = SubDagOperator(
#        subdag=update_analytics_db.update_analytics_db(
#            DAG_NAME,
#            'update_analytics_db',
#            default_args['start_date'],
#            mdag.schedule_interval,
#            {
#                'sql_command_func' : lambda ds, k: insert_current_date(sql_template, k)
#                if insert_current_date('{}-{}-{}', k).find('-01') == 7 else ''
#            }
#        ),
#        task_id='update_analytics_db',
#        dag=mdag
#    )
#
#
#if HVDAG.HVDAG.airflow_env != 'test':
#    fetch_transaction.set_upstream(validate_transaction)
#    queue_up_for_matching.set_upstream(validate_deid)
#
#    detect_move_normalize_dag.set_upstream(
#        [queue_up_for_matching, split_transaction]
#    )
#    update_analytics_db.set_upstream(detect_move_normalize_dag)
#else:
#    detect_move_normalize_dag.set_upstream(split_transaction)
#
#decrypt_transaction.set_upstream(fetch_transaction)
#split_transaction.set_upstream(decrypt_transaction)
#
## cleanup
#clean_up_workspace.set_upstream(split_transaction)
