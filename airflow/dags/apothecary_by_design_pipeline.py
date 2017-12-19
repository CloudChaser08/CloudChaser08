from airflow.models import Variable
from airflow.operators import PythonOperator, SubDagOperator
from datetime import datetime, timedelta
from subprocess import check_call

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

import util.s3_utils as s3_utils

for m in [HVDAG, s3_validate_file, s3_fetch_file, detect_move_normalize,
          queue_up_for_matching, split_push_files, update_analytics_db,
          s3_utils, clean_up_tmp_dir]:
    reload(m)

DAG_NAME = 'apothecary_by_design_pipeline'

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2017, 11, 13, 16),
    'retries': 3,
    'retry_delay': timedelta(minutes=2)
}

mdag = HVDAG.HVDAG(
    dag_id = DAG_NAME,
    schedule_interval = '0 16 * * 1',
    default_args = default_args
)

if HVDAG.HVDAG.airflow_env == 'test':
    test_loc  = 's3://salusv/testing/dewey/airflow/e2e/apothecarybydesign/'
    S3_TRANSACTION_RAW_URL = test_loc + 'raw/'
    S3_TRANSACTION_PROCESSED_URL_TXN_TEMPLATE = test_loc + 'out/{}/{}/{}/transactions/'
    S3_TRANSACTION_PROCESSED_URL_ADD_TEMPLATE = test_loc + 'out/{}/{}/{}/additionaldata/'
    S3_PAYLOAD_DEST = test_loc + 'payload/'
    S3_NORMALIZED_DATA_URL = test_loc + 'spark-output/'
    S3_BACKUP_NORMALIZED_DATA_URL = test_loc + 'spark-backup/'
else:
    S3_TRANSACTION_RAW_URL = 's3://healthverity/incoming/abd/'
    S3_TRANSACTION_PROCESSED_URL_TXN_TEMPLATE = 's3://salusv/incoming/pharmacyclaims/apothecarybydesign/{}/{}/{}/transactions/'
    S3_TRANSACTION_PROCESSED_URL_ADD_TEMPLATE = 's3://salusv/incoming/pharmacyclaims/apothecarybydesign/{}/{}/{}/additionaldata/'
    S3_PAYLOAD_DEST = 's3://salusv/matching/payload/pharmacyclaims/apothecarybydesign/'
    S3_NORMALIZED_DATA_URL = 's3://salusv/warehouse/parquet/pharmacyclaims/2017-06-02/part_provider=apothecary_by_design/'
    S3_BACKUP_NORMALIZED_DATA_URL = 's3://salusv/warehouse/parquet/pharmacyclaims/_archive/part_provider=apothecary_by_design/{}/'

TMP_PATH_TEMPLATE = '/tmp/apothecary_by_design/pharmacyclaims/{}/'
ADDITIONALDATA_TMP_PATH_TEMPLATE = TMP_PATH_TEMPLATE + 'raw/additionaldata/'
TRANSACTION_TMP_PATH_TEMPLATE = TMP_PATH_TEMPLATE + 'raw/transactions/'

ADDITIONALDATA_FILE_NAME_TEMPLATE = 'hv_export_data_{}.txt'
TRANSACTION_FILE_NAME_TEMPLATE = 'hv_export_po_deid_{}.txt'
DEID_FILE_NAME_TEMPLATE = 'hv_export_o_deid_{}.txt'

def get_start_date(ds, kwargs):
    return kwargs['ds_nodash']


def insert_formatted_start_date_function(template):
    def out(ds, kwargs):
        return template.format(get_start_date(ds, kwargs))

    return out


def get_formatted_date(ds, kwargs):
    return (kwargs['execution_date'] + timedelta(days=7)).strftime('%Y%m%d')


def insert_formatted_date_function(template):
    def out(ds, kwargs):
        return template.format(get_formatted_date(ds, kwargs))

    return out


def insert_formatted_regex_function(template):
    def out(ds, kwargs):
        return template.format(get_formatted_date(ds, kwargs))

    return out


def insert_current_date(template, kwargs):
    ds_nodash = (kwargs['execution_date'] + timedelta(days=7)).strftime('%Y%m%d')
    return template.format(
        ds_nodash[0:4],
        ds_nodash[4:6],
        ds_nodash[6:8]
    )


def insert_current_date_function(template):
    def out(ds, kwargs):
        return insert_current_date(template, kwargs)

    return out


get_tmp_dir = insert_formatted_start_date_function(ADDITIONALDATA_TMP_PATH_TEMPLATE)
get_deid_tmp_dir = insert_formatted_start_date_function(TRANSACTION_TMP_PATH_TEMPLATE)

def get_additionaldata_file_paths(ds, kwargs):
    return [get_tmp_dir(ds, kwargs) + ADDITIONALDATA_FILE_NAME_TEMPLATE.format(
        get_formatted_date(ds, kwargs)
    )]


def get_transaction_file_paths(ds, kwargs):
    return [get_deid_tmp_dir(ds, kwargs) + TRANSACTION_FILE_NAME_TEMPLATE.format(
        get_formatted_date(ds, kwargs)
    )]


def get_deid_file_urls(ds, kwargs):
    return [DEID_FILE_NAME_TEMPLATE.format(
        get_formatted_date(ds, kwargs)
    )]


def encrypted_decrypted_deid_file_paths_function(ds, kwargs):
    file_dir = get_deid_tmp_dir(ds, kwargs)
    encrypted_file_path = file_dir \
            + TRANSACTION_FILE_NAME_TEMPLATE.format(
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
    validate_additionaldata = generate_file_validation_task(
        'additionaldata', S3_TRANSACTION_RAW_URL,
        ADDITIONALDATA_FILE_NAME_TEMPLATE, 1000000
    )
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
                'source_files_func' : lambda ds, k: [S3_TRANSACTION_RAW_URL + f for f in get_deid_file_urls(ds, k)]
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
            'expected_file_name_func': insert_formatted_date_function(
                TRANSACTION_FILE_NAME_TEMPLATE
            ),
            's3_prefix'              : '/'.join(S3_TRANSACTION_RAW_URL.split('/')[3:]),
            's3_bucket'              : 'salusv' if HVDAG.HVDAG.airflow_env == 'test' else 'healthverity'
        }
    ),
    task_id='fetch_transaction_file',
    dag=mdag
)

fetch_additionaldata_file = SubDagOperator(
    subdag=s3_fetch_file.s3_fetch_file(
        DAG_NAME,
        'fetch_additionaldata_file',
        default_args['start_date'],
        mdag.schedule_interval,
        {
            'tmp_path_template'     : ADDITIONALDATA_TMP_PATH_TEMPLATE,
            'expected_file_name_func'   : insert_formatted_date_function(
                ADDITIONALDATA_FILE_NAME_TEMPLATE
            ),
            's3_prefix'                 : '/'.join(S3_TRANSACTION_RAW_URL.split('/')[3:]),
            's3_bucket'                 : 'salusv' if HVDAG.HVDAG.airflow_env == 'test' else 'healthverity'
        }
    ),
    task_id = 'fetch_additionaldata_file',
    dag=mdag
)

decrypt_transaction = SubDagOperator(
    subdag=decrypt_files.decrypt_files(
        DAG_NAME,
        'decrypt_transaction_file',
        default_args['start_date'],
        mdag.schedule_interval,
        {
            'tmp_dir_func'                        : get_deid_tmp_dir,
            'encrypted_decrypted_file_paths_func' : encrypted_decrypted_deid_file_paths_function
        }
    ),
    task_id='decrypt_transaction_file',
    dag=mdag
)


split_additionaldata_file = SubDagOperator(
    subdag=split_push_files.split_push_files(
        DAG_NAME,
        'split_additionaldata_file',
        default_args['start_date'],
        mdag.schedule_interval,
        {
            'tmp_dir_func'             : get_tmp_dir,
            'file_paths_to_split_func' : get_additionaldata_file_paths,
            'file_name_pattern_func'   : insert_formatted_regex_function(
                ADDITIONALDATA_FILE_NAME_TEMPLATE
            ),
            's3_prefix_func'           : insert_current_date_function(
                S3_TRANSACTION_PROCESSED_URL_ADD_TEMPLATE
            ),
            'num_splits'               : 20
        }
    ),
    task_id='split_additionaldata_file',
    dag=mdag
)


split_transaction_file = SubDagOperator(
    subdag=split_push_files.split_push_files(
        DAG_NAME,
        'split_transaction_file',
        default_args['start_date'],
        mdag.schedule_interval,
        {
            'tmp_dir_func'             : get_deid_tmp_dir,
            'file_paths_to_split_func' : get_transaction_file_paths,
            'file_name_pattern_func'   : insert_formatted_regex_function(
                TRANSACTION_FILE_NAME_TEMPLATE
            ),
            's3_prefix_func'           : insert_current_date_function(
                S3_TRANSACTION_PROCESSED_URL_TXN_TEMPLATE
            ),
            'num_splits'               : 20
        }
    ),
    task_id='split_transaction_file',
    dag=mdag
)


def do_backup_existing_data(ds, **kwargs):
    s3_utils.delete_path_recursive(kwargs['BACKUP_NORMALIZED_DATA_URL'])
    s3_utils.move_path_recursive(kwargs['NORMALIZED_DATA_URL'],
                                 kwargs['BACKUP_NORMALIZED_DATA_URL'])


backup_existing_data = PythonOperator(
    task_id = 'backup_existing_data',
    python_callable = do_backup_existing_data,
    op_kwargs = {
        'NORMALIZED_DATA_URL'           : S3_NORMALIZED_DATA_URL,
        'BACKUP_NORMALIZED_DATA_URL'    : S3_BACKUP_NORMALIZED_DATA_URL
    },
    provide_context = True,
    dag = mdag
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
            'expected_matching_files_func'      : get_deid_file_urls,
            'file_date_func'                    : insert_current_date_function(
                '{}/{}/{}'
            ),
            's3_payload_loc_url'                : S3_PAYLOAD_DEST,
            'vendor_uuid'                       : '51ca8f88-040a-47f1-b78a-491c8632fedd',
            'pyspark_normalization_script_name' : '/home/hadoop/spark/providers/apothecary_by_design/pharmacyclaims/sparkNormalizeApothecaryByDesign.py',
            'pyspark_normalization_args_func'   : norm_args,
            'pyspark'                           : True
        }
    ),
    task_id='detect_move_normalize',
    dag=mdag
)

if HVDAG.HVDAG.airflow_env != 'test':
    sql_template = """
        MSCK REPAIR TABLE pharmacyclaims_20170602 
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
    fetch_additionaldata_file.set_upstream(validate_additionaldata)
    fetch_transaction_file.set_upstream(validate_transaction)
    queue_up_for_matching.set_upstream(validate_deid)
    detect_move_normalize_dag.set_upstream(queue_up_for_matching)
    update_analytics_db.set_upstream(detect_move_normalize_dag)

decrypt_transaction.set_upstream(fetch_transaction_file)

split_additionaldata_file.set_upstream(fetch_additionaldata_file)
split_transaction_file.set_upstream(decrypt_transaction)

backup_existing_data.set_upstream([split_additionaldata_file, split_transaction_file])
clean_up_workspace.set_upstream([split_additionaldata_file, split_transaction_file])

detect_move_normalize_dag.set_upstream(backup_existing_data)
