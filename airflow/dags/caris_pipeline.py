from airflow import DAG
from airflow.models import Variable
from airflow.operators import PythonOperator, SubDagOperator
from datetime import datetime, timedelta
from subprocess import check_call

# hv-specific modules
import subdags.s3_validate_file as s3_validate_file
import subdags.s3_fetch_file as s3_fetch_file
import subdags.decrypt_files as decrypt_files
import subdags.split_push_files as split_push_files
import subdags.queue_up_for_matching as queue_up_for_matching
import subdags.detect_move_normalize as detect_move_normalize

# import util.decompression as decompression

for m in [s3_validate_file, s3_fetch_file, decrypt_files,
          split_push_files, queue_up_for_matching,
          detect_move_normalize]:
    reload(m)

# Applies to all files
TMP_PATH_TEMPLATE = '/tmp/caris/labtests/{}/'
DAG_NAME = 'caris_pipeline'

# Applies to all transaction files
S3_TRANSACTION_RAW_URL = 's3://healthverity/incoming/caris/'
S3_TRANSACTION_PROCESSED_URL_TEMPLATE = 's3://salusv/incoming/labtests/caris/{}/{}/'

# Transaction Addon file
TRANSACTION_FILE_NAME_TEMPLATE = 'DATA_{}{}01*'

# Deid file
DEID_FILE_NAME_TEMPLATE = 'DEID_{}{}01*'

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2017, 3, 16, 12),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2)
}

mdag = DAG(
    dag_id=DAG_NAME,
    schedule_interval="0 12 * * *" if Variable.get(
        "AIRFLOW_ENV", default_var=''
    ).find('prod') != -1 else None,
    default_args=default_args
)


def insert_todays_date_function(template):
    def out(ds, kwargs):
        return template.format(kwargs['ds_nodash'])
    return out


def insert_formatted_regex_function(template):
    def out(ds, kwargs):
        return template.format('\d{2}', '\d{2}')
    return out


def insert_current_date_function(template):
    def out(ds, kwargs):
        return template.format(
            kwargs['ds_nodash'][0:4],
            kwargs['ds_nodash'][4:6],
        )
    return out


def insert_current_date(template, kwargs):
    insert_current_date_function(template)(None, kwargs)


# get_tmp_dir = insert_todays_date_function(TMP_PATH_TEMPLATE)

def get_deid_file_urls(ds, kwargs):
    return [S3_TRANSACTION_RAW_URL + insert_current_date(
        DEID_FILE_NAME_TEMPLATE, kwargs
    )]


def encrypted_decrypted_file_paths_function(ds, kwargs):
    file_dir = insert_todays_date_function(TMP_PATH_TEMPLATE)(ds, kwargs)
    encrypted_file_path = file_dir \
        + insert_current_date(TRANSACTION_FILE_NAME_TEMPLATE, kwargs)
    return [
        [encrypted_file_path, encrypted_file_path + '.gz']
    ]


def get_unzipped_file_paths(ds, kwargs):
    file_dir = insert_todays_date_function(TMP_PATH_TEMPLATE)(ds, kwargs)
    return [file_dir
            + insert_current_date_function(TRANSACTION_FILE_NAME_TEMPLATE)
    ]

# def get_trunk_unzipped_file_paths(ds, kwargs):
#     file_dir = get_trunk_tmp_dir(ds, kwargs)
#     return [file_dir
#             + TRANSACTION_TRUNK_UNZIPPED_FILE_NAME_TEMPLATE.format(
#                 get_formatted_date(ds, kwargs)
#             )]


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
                'expected_file_name_func': insert_current_date_function(
                    path_template
                ),
                'file_name_pattern_func': insert_formatted_regex_function(
                    path_template
                ),
                'minimum_file_size': minimum_file_size,
                's3_prefix': '/'.join(S3_TRANSACTION_RAW_URL.split('/')[3:]),
                's3_bucket': 'healthverity',
                'file_description': 'Caris ' + task_id + 'file'
            }
        ),
        task_id='validate_' + task_id + '_file',
        dag=mdag
    )


validate_transactional = generate_transaction_file_validation_dag(
    'transaction', TRANSACTION_FILE_NAME_TEMPLATE,
    1000000
)
validate_deid = generate_transaction_file_validation_dag(
    'deid', DEID_FILE_NAME_TEMPLATE,
    1000000
)


def generate_fetch_dag(
        task_id, s3_path_template, local_path_template, file_name_template
):
    return SubDagOperator(
        subdag=s3_fetch_file.s3_fetch_file(
            DAG_NAME,
            'fetch_' + task_id + '_file',
            default_args['start_date'],
            mdag.schedule_interval,
            {
                'tmp_path_template': local_path_template,
                'expected_file_name_func': insert_current_date_function(
                    file_name_template
                ),
                's3_prefix': s3_path_template,
                's3_bucket': 'healthverity'
            }
        ),
        task_id='fetch_' + task_id + '_file',
        dag=mdag
    )


fetch_transactional = generate_fetch_dag(
    "transaction",
    '/'.join(S3_TRANSACTION_RAW_URL.split('/')[3:]),
    TMP_PATH_TEMPLATE, TRANSACTION_FILE_NAME_TEMPLATE
)

decrypt_transactional = SubDagOperator(
    subdag=decrypt_files.decrypt_files(
        DAG_NAME,
        'decrypt_transactional_file',
        default_args['start_date'],
        mdag.schedule_interval,
        {
            'tmp_dir_func': insert_todays_date_function(TMP_PATH_TEMPLATE),
            'encrypted_decrypted_file_paths_func':
            encrypted_decrypted_file_paths_function
        }
    ),
    task_id='decrypt_transactional_file',
    dag=mdag
)


def split_step(task_id, tmp_dir_func, file_paths_to_split_func, s3_destination, num_splits):
    return SubDagOperator(
        subdag=split_push_files.split_push_files(
            DAG_NAME,
            'split_' + task_id + '_file',
            default_args['start_date'],
            mdag.schedule_interval,
            {
                'tmp_dir_func': insert_todays_date_function(TMP_PATH_TEMPLATE),
                'file_paths_to_split_func': file_paths_to_split_func,
                's3_prefix_func': insert_current_date_function(
                    s3_destination
                ),
                'num_splits': num_splits
            }
        ),
        task_id='split_' + task_id + '_file',
        dag=mdag
    )


split_transactional = split_step(
    "transactional", insert_todays_date_function(TMP_PATH_TEMPLATE),
    get_unzipped_file_paths, S3_TRANSACTION_PROCESSED_URL_TEMPLATE, 20
)


def clean_up_workspace_step(task_id, template):
    def execute(ds, **kwargs):
        check_call([
            'rm', '-rf', template.format(kwargs['ds_nodash'])
        ])
    return PythonOperator(
        task_id='clean_up_workspace_' + task_id,
        provide_context=True,
        python_callable=execute,
        trigger_rule='all_done',
        dag=mdag
    )


clean_up_workspace = clean_up_workspace_step("all", TMP_PATH_TEMPLATE)

queue_up_for_matching = SubDagOperator(
    subdag=queue_up_for_matching.queue_up_for_matching(
        DAG_NAME,
        'queue_up_for_matching',
        default_args['start_date'],
        mdag.schedule_interval,
        {
            'source_files_func': get_deid_file_urls
        }
    ),
    task_id='queue_up_for_matching',
    dag=mdag
)

#
# Post-Matching
#
S3_PAYLOAD_DEST = 's3://salusv/matching/payload/labtests/caris/'
TEXT_WAREHOUSE = "s3a://salusv/warehouse/text/labtests/2017-02-16/part_provider=caris/"
PARQUET_WAREHOUSE = "s3://salusv/warehouse/parquet/labtests/2017-02-16/part_provider=caris/"

detect_move_normalize_dag = SubDagOperator(
    subdag=detect_move_normalize.detect_move_normalize(
        DAG_NAME,
        'detect_move_normalize',
        default_args['start_date'],
        mdag.schedule_interval,
        {
            'expected_matching_files_func': lambda ds, k: [
                insert_current_date_function(
                    DEID_FILE_NAME_TEMPLATE
                )(ds, k)
            ],
            'file_date_func': insert_current_date_function(
                '{}/{}/{}'
            ),
            's3_payload_loc_url': S3_PAYLOAD_DEST,
            'vendor_uuid': 'd701240c-35be-4e71-94fc-9460b85b1515',
            'pyspark_normalization_script_name': '/home/hadoop/spark/providers/caris/sparkNormalizeCaris.py',
            'pyspark_normalization_args_func': lambda ds, k: [
                '--date', insert_current_date('{}-{}-{}', k)
            ],
            'text_warehouse': TEXT_WAREHOUSE,
            'parquet_warehouse': PARQUET_WAREHOUSE,
            'part_file_prefix_func': insert_current_date_function('{}-{}-{}'),
            'data_feed_type': 'lab',
            'pyspark': True
        }
    ),
    task_id='detect_move_normalize',
    dag=mdag
)

# addon
fetch_transactional.set_upstream(validate_transactional)
decrypt_transactional.set_upstream(fetch_transactional)
split_transactional.set_upstream(decrypt_transactional)

# cleanup
clean_up_workspace.set_upstream(
    split_transactional
)

# matching
queue_up_for_matching.set_upstream(validate_deid)

# post-matching
detect_move_normalize_dag.set_upstream(
    [queue_up_for_matching, split_transactional]
)
