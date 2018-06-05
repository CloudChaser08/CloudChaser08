from airflow.models import Variable
from airflow.operators import PythonOperator, SubDagOperator, DummyOperator
from datetime import datetime, timedelta
import os

# hv-specific modules
import common.HVDAG as HVDAG
import subdags.s3_validate_file as s3_validate_file
import subdags.s3_fetch_file as s3_fetch_file
import subdags.decrypt_files as decrypt_files
import subdags.split_push_files as split_push_files
import subdags.queue_up_for_matching as queue_up_for_matching
import subdags.detect_move_normalize as detect_move_normalize
import subdags.clean_up_tmp_dir as clean_up_tmp_dir

import util.decompression as decompression
import util.date_utils as date_utils

for m in [s3_validate_file, s3_fetch_file, decrypt_files,
          split_push_files, queue_up_for_matching, clean_up_tmp_dir,
          detect_move_normalize, decompression, HVDAG,
          date_utils]:
    reload(m)

# Applies to all files
TMP_PATH_TEMPLATE = '/tmp/liquidhub/custom/{}{}{}/'
DAG_NAME = 'liquidhub_pipeline'

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2018, 6, 5, 16),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2)
}

mdag = HVDAG.HVDAG(
    dag_id=DAG_NAME,
    schedule_interval="0 16 * * *",
    default_args=default_args
)

# Applies to all transaction files
if HVDAG.HVDAG.airflow_env == 'test':
    S3_TRANSACTION_RAW_URL = 's3://salusv/testing/dewey/airflow/e2e/lhv2/custom/raw/'
    S3_TRANSACTION_PROCESSED_URL_TEMPLATE = 's3://salusv/testing/dewey/airflow/e2e/lhv2/custom/out/{}/{{}}/{{}}/{{}}/'
    S3_PAYLOAD_DEST = 's3://salusv/testing/dewey/airflow/e2e/lhv2/custom/payload/'
    S3_NORMALIZED_FILE_URL_TEMPLATE = 's3://salusv/testing/dewey/airflow/e2e/lhv2/custom/spark-out/{}/{{}}/{{}}/{{}}/'
else:
    S3_TRANSACTION_RAW_URL = 's3://healthverity/incoming/lh_amgen_hv/'
    S3_TRANSACTION_PROCESSED_URL_TEMPLATE = 's3://salusv/incoming/custom/lhv2/{}/{{}}/{{}}/{{}}/'
    S3_PAYLOAD_DEST = 's3://salusv/matching/payload/custom/lhv2/'
    S3_NORMALIZED_FILE_URL_TEMPLATE = 's3://salusv/deliverable/lhv2/{}/{{}}/{{}}/{{}}/'

# Transaction Addon file
TRANSACTION_TMP_PATH_TEMPLATE = TMP_PATH_TEMPLATE + 'raw/'
TRANSACTION_FILE_DESCRIPTION = 'Liquidhub transaction file'
TRANSACTION_FILE_NAME_TEMPLATE = 'LH_{}_LiquidHub_{{}}{{}}{{}}_plainoutput.txt'

# Deid file
DEID_FILE_DESCRIPTION = 'Liquidhub deid file'
DEID_FILE_NAME_TEMPLATE = 'LH_{}_LiquidHub_{{}}{{}}{{}}_output.txt'

# Return file
RETURN_FILE_NAME_TEMPLATE = 'LH_Amgen_{}_{{}}{{}}{{}}_FILEID.txt.gz'

get_tmp_dir = date_utils.generate_insert_date_into_template_function(
    TRANSACTION_TMP_PATH_TEMPLATE
)

def get_pharmacy_id(ds, kwargs):
    return kwargs['ti'].xcom_pull(key='pharmacy_id', dag_id=DAG_NAME, task_ids='parse_pharmacy_id')

def get_transaction_file_paths(ds, kwargs):
    pharmacy_id = get_pharmacy_id(ds, kwargs)
    return [get_tmp_dir(ds, kwargs) +\
        date_utils.insert_date_into_template(TRANSACTION_FILE_NAME_TEMPLATE.format(pharmacy_id), kwargs)
    ]


def get_deid_file_urls(ds, kwargs):
    pharmacy_id = get_pharmacy_id(ds, kwargs)
    return [S3_TRANSACTION_RAW_URL +\
        date_utils.insert_date_into_template(DEID_FILE_NAME_TEMPLATE.format(pharmacy_id), kwargs)
    ]


def encrypted_decrypted_file_paths_function(ds, kwargs):
    pharmacy_id = get_pharmacy_id(ds, kwargs)
    file_dir = get_tmp_dir(ds, kwargs)
    encrypted_file_path = file_dir \
        + date_utils.insert_date_into_template(
            TRANSACTION_FILE_NAME_TEMPLATE.format(pharmacy_id),
            kwargs
        )
    return [
        [encrypted_file_path, encrypted_file_path + '.gz']
    ]


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
                    path_template
                ),
                'file_name_pattern_func'  : date_utils.generate_insert_regex_into_template_function(
                    path_template
                ),
                'minimum_file_size'       : minimum_file_size,
                's3_prefix'               : '/'.join(S3_TRANSACTION_RAW_URL.split('/')[3:]),
                's3_bucket'               : 'healthverity',
                'file_description'        : 'Liquidhub ' + task_id + ' file',
                'regex_name_match'        : True
            }
        ),
        task_id='validate_' + task_id,
        dag=mdag
    )


validate_transaction = generate_file_validation_task(
    'transaction', TRANSACTION_FILE_NAME_TEMPLATE.format('.*'),
    1000000
)
validate_deid = generate_file_validation_task(
    'deid', DEID_FILE_NAME_TEMPLATE.format('.*'),
    10000000
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
                TRANSACTION_FILE_NAME_TEMPLATE.format('.*')
            ),
            's3_prefix'              : '/'.join(S3_TRANSACTION_RAW_URL.split('/')[3:]),
            's3_bucket'              : 'salusv' if HVDAG.HVDAG.airflow_env == 'test' else 'healthverity',
            'regex_name_match'       : True
        }
    ),
    task_id='fetch_transaction_file',
    dag=mdag
)


def do_parse_pharmacy_id(ds, **kwargs):
    file_dir = get_tmp_dir(ds, kwargs)
    files = os.listdir(file_dir)
    transactions_file = [f for f in files if 'plainoutput.txt' in f][0]
    
    pharmacy_id = '_'.join(transactions_file.split('_')[1:-3])
    kwargs['ti'].xcom_push(key='pharmacy_id', value=pharmacy_id)

parse_pharmacy_id = PythonOperator(
    task_id='parse_pharmacy_id',
    provide_context=True,
    python_callable=do_parse_pharmacy_id,
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


split_transaction = SubDagOperator(
    subdag=split_push_files.split_push_files(
        DAG_NAME,
        'split_transaction_file',
        default_args['start_date'],
        mdag.schedule_interval,
        {
            'tmp_dir_func'             : get_tmp_dir,
            'file_paths_to_split_func' : get_transaction_file_paths,
            'file_name_pattern_func'   :
                date_utils.generate_insert_regex_into_template_function(
                    TRANSACTION_FILE_NAME_TEMPLATE.format('.*')
                ),
            's3_prefix_func'           : lambda ds, k:
                date_utils.insert_date_into_template(
                    S3_TRANSACTION_PROCESSED_URL_TEMPLATE.format(get_pharmacy_id(ds, k)), k
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

queue_up_for_matching = SubDagOperator(
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

#
# Post-Matching
#
def norm_args(ds, k):
    pharmacy_id = get_pharmacy_id(d, k)
    base = ['--date', date_utils.insert_date_into_template('{}-{}-{}', k), '--pharmacy_id', pharmacy_id]
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
                date_utils.insert_date_into_template(DEID_FILE_NAME_TEMPLATE.format(get_pharmacy_id(d, k)), k)
            ],
            'dest_dir_func'                     : lambda ds, k:
                get_pharmacy_id(ds, k) + date_utils.insert_date_into_template('/{}/{}/{}', k),
            's3_payload_loc_url'                : S3_PAYLOAD_DEST,
            'vendor_uuid'                       : 'e46667a2-27d7-4014-9382-2f91661999aa',
            'pyspark_normalization_script_name' : '/home/hadoop/spark/providers/liquidhub/custom/sparkNormalizeLiquidhub.py',
            'pyspark_normalization_args_func'   : norm_args,
            'pyspark'                           : True
        }
    ),
    task_id='detect_move_normalize',
    dag=mdag
)

def do_fetch_return_file(ds, **kwargs):
    pid = get_pharmacy_id(ds, kwargs)

    return_file = date_utils.insert_date_into_template(
        RETURN_FILE_NAME_TEMPLATE.format(pid), kwargs
    )

    s3_utils.fetch_file_from_s3(
        date_utils.insert_date_into_template(
                S3_NORMALIZED_FILE_URL_TEMPLATE.format(pid), kwargs
            ) + return_file,
        get_tmp_dir(ds, kwargs) + return_file
    )

fetch_return_file = PythonOperator(
    task_id='fetch_return_file',
    provide_context=True,
    python_callable=do_fetch_return_file,
    dag=mdag
)

def do_deliver_return_file(ds, **kwargs):
    sftp_config = json.loads(Variable.get('_sftp_configuration'))
    path = sftp_config['path']
    del sftp_config['path']
    pid = get_pharmacy_id(ds, kwargs)

    sftp_utils.upload_file(
        get_tmp_dir(ds, kwargs) + RETURN_FILE_NAME_TEMPLATE.format(pid),
        path,
        ignore_host_key=True,
         **sftp_config
    )

deliver_return_file = PythonOperator(
    task_id='deliver_return_file',
    provide_context=True,
    python_callable=do_deliver_return_file,
    dag=mdag
)

if HVDAG.HVDAG.airflow_env == 'test':
    for t in ['validate_transaction', 'validate_deid', 'queue_up_for_matching',
            'fetch_return_file', 'deliver_return_file']:
        del mdag.task_dict[t]
        globals()[t] = DummyOperator(
            task_id = t,
            dag = mdag
        )

fetch_transaction.set_upstream(validate_transaction)
decrypt_transaction.set_upstream(fetch_transaction)
split_transaction.set_upstream(decrypt_transaction)
queue_up_for_matching.set_upstream(validate_deid)

detect_move_normalize_dag.set_upstream(
    [queue_up_for_matching, split_transaction]
)
fetch_return_file.set_upstream(detect_move_normalize_dag)
deliver_return_file.set_upstream(fetch_return_file)

# cleanup
clean_up_workspace.set_upstream(deliver_return_file)
