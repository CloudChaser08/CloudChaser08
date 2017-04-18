from airflow import DAG
from airflow.models import Variable
from airflow.operators import PythonOperator, SubDagOperator
from datetime import datetime, timedelta
from subprocess import check_call
import os

# hv-specific modules
import subdags.s3_validate_file as s3_validate_file
import subdags.s3_fetch_file as s3_fetch_file
import subdags.decrypt_files as decrypt_files
import subdags.split_push_files as split_push_files
import subdags.queue_up_for_matching as queue_up_for_matching
import subdags.detect_move_normalize as detect_move_normalize

import util.decompression as decompression

for m in [s3_validate_file, s3_fetch_file, decrypt_files,
          split_push_files, queue_up_for_matching,
          detect_move_normalize, decompression]:
    reload(m)

# Applies to all files
TMP_PATH_TEMPLATE = '/tmp/practice_insight/medicalclaims/{}/'
DAG_NAME = 'practice_insight_pipeline'

# Applies to all transaction files
S3_TRANSACTION_RAW_URL = 's3://healthverity/incoming/practiceinsight/'
S3_TRANSACTION_PROCESSED_URL_TEMPLATE = 's3://salusv/incoming/medicalclaims/practice_insight/{}/{}/'
TRANSACTION_UNZIPPED_FILE_NAME_TEMPLATE = 'HV.data.837.{}.{}.csv'
TRANSACTION_FILE_NAME_TEMPLATE = TRANSACTION_UNZIPPED_FILE_NAME_TEMPLATE + '.gz'

# Deid file
DEID_FILE_NAME_TEMPLATE = 'HV.phi.{}.{}.o'

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
        return template.format('\d{4}', '[a-z]{3}')
    return out


def insert_current_plaintext_date_function(template):
    def out(ds, kwargs):
        return template.format(
            kwargs['ds_nodash'][0:4],
            datetime.strptime(
                kwargs['ds_nodash'], '%Y%m%d'
            ).strftime('%b').lower()
        )
    return out


def insert_current_plaintext_date(template, kwargs):
    return insert_current_plaintext_date_function(template)(None, kwargs)


def insert_current_date_function(template):
    def out(ds, kwargs):
        return template.format(
            kwargs['ds_nodash'][0:4],
            kwargs['ds_nodash'][4:6]
        )
    return out


def insert_current_date(template, kwargs):
    return insert_current_date_function(template)(None, kwargs)


get_tmp_dir = insert_todays_date_function(TMP_PATH_TEMPLATE)


def get_deid_file_urls(ds, kwargs):
    return [
        S3_TRANSACTION_RAW_URL
        + insert_current_plaintext_date(
            DEID_FILE_NAME_TEMPLATE, kwargs
        )
    ]


def get_unzipped_file_paths(ds, kwargs):
    return [
        insert_current_plaintext_date(
            TRANSACTION_UNZIPPED_FILE_NAME_TEMPLATE, kwargs
        )
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
                'expected_file_name_func':
                insert_current_plaintext_date_function(
                    path_template
                ),
                'file_name_pattern_func': insert_formatted_regex_function(
                    path_template
                ),
                'minimum_file_size': minimum_file_size,
                's3_prefix': '/'.join(S3_TRANSACTION_RAW_URL.split('/')[3:]),
                's3_bucket': 'healthverity',
                'file_description': 'Practice Insight ' + task_id + ' file'
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
    10000000
)

fetch_transactional = SubDagOperator(
    subdag=s3_fetch_file.s3_fetch_file(
        DAG_NAME,
        'fetch_transaction_file',
        default_args['start_date'],
        mdag.schedule_interval,
        {
            'tmp_path_template': TMP_PATH_TEMPLATE,
            'expected_file_name_func': insert_current_plaintext_date_function(
                TRANSACTION_FILE_NAME_TEMPLATE
            ),
            's3_prefix': '/'.join(S3_TRANSACTION_RAW_URL.split('/')[3:]),
            's3_bucket': 'healthverity'
        }
    ),
    task_id='fetch_transaction_file',
    dag=mdag
)


def gunzip_transactional_func(ds, **kwargs):
    tmp_dir = get_tmp_dir(ds, kwargs)
    decompression.decompress_gzip_file(
        tmp_dir + insert_current_plaintext_date(
            TRANSACTION_FILE_NAME_TEMPLATE, kwargs
        ),
    )


gunzip_transactional = PythonOperator(
    task_id='gunzip_transaction_file',
    provide_context=True,
    python_callable=gunzip_transactional_func,
    dag=mdag
)


def split_transaction_into_parts_func(ds, **kwargs):
    check_call([
        'mkdir', '-p', get_tmp_dir(ds, kwargs) + 'presplit/'
    ])
    check_call([
        'split', '-n', 'l/4', get_tmp_dir(ds, kwargs)
        + insert_current_plaintext_date(
            TRANSACTION_UNZIPPED_FILE_NAME_TEMPLATE, kwargs
        ),
        get_tmp_dir(ds, kwargs) + 'presplit/'
        + insert_current_plaintext_date(
            TRANSACTION_UNZIPPED_FILE_NAME_TEMPLATE, kwargs
        ) + '.'
    ])
    for i in range(1, 5):
        os.mkdir(get_tmp_dir(ds, kwargs) + 'presplit/' + str(i))
        check_call([
            'mkdir', '-p', get_tmp_dir(ds, kwargs) + 'presplit/' + str(i)
        ])
        os.rename(
            get_tmp_dir(ds, kwargs) + 'presplit/'
            + filter(
                lambda f: f.startswith(
                    insert_current_plaintext_date(
                        TRANSACTION_UNZIPPED_FILE_NAME_TEMPLATE, kwargs
                    )
                ),
                os.listdir(get_tmp_dir(ds, kwargs) + 'presplit/')
            )[0],
            get_tmp_dir(ds, kwargs) + 'presplit/' + str(i) + '/'
            + insert_current_plaintext_date(
                TRANSACTION_UNZIPPED_FILE_NAME_TEMPLATE, kwargs
            )
            )


split_transaction_into_parts = PythonOperator(
    task_id='split_transaction_into_parts',
    provide_context=True,
    python_callable=split_transaction_into_parts_func,
    dag=mdag
)


def split_step(
        task_id, part_number, tmp_dir_func, file_paths_to_split_func,
        s3_destination, num_splits
):
    return SubDagOperator(
        subdag=split_push_files.split_push_files(
            DAG_NAME,
            'split_' + task_id + '_' + str(part_number) + '_file',
            default_args['start_date'],
            mdag.schedule_interval,
            {
                'tmp_dir_func': tmp_dir_func,
                'file_paths_to_split_func': lambda ds, k: map(
                    lambda f: tmp_dir_func(ds, k) + f,
                    file_paths_to_split_func(ds, k)
                ),
                's3_prefix_func': lambda ds, k: insert_current_date_function(
                    s3_destination
                )(ds, k) + part_number + '/',
                'num_splits': num_splits
            }
        ),
        task_id='split_' + task_id + '_' + part_number + '_file',
        dag=mdag
    )


split_transactional_steps = map(
    lambda i: split_step(
        "transaction", str(i),
        lambda ds, k: get_tmp_dir(ds, k) + 'presplit/' + str(i) + '/',
        get_unzipped_file_paths,
        S3_TRANSACTION_PROCESSED_URL_TEMPLATE, 20
    ),
    range(1, 5)
)


def clean_up_workspace_func(ds, **kwargs):
    check_call([
        'rm', '-rf', TMP_PATH_TEMPLATE.format(kwargs['ds_nodash'])
    ])


clean_up_workspace = PythonOperator(
    task_id='clean_up_workspace',
    provide_context=True,
    python_callable=clean_up_workspace_func,
    trigger_rule='all_done',
    dag=mdag
)


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
S3_PAYLOAD_DEST = 's3://salusv/matching/payload/medicalclaims/practice_insight/'
TEXT_WAREHOUSE = "s3a://salusv/warehouse/text/medicalclaims/2017-02-24/"
PARQUET_WAREHOUSE = "s3a://salusv/warehouse/parquet/medicalclaims/2017-02-24/"

detect_move_normalize_dag = SubDagOperator(
    subdag=detect_move_normalize.detect_move_normalize(
        DAG_NAME,
        'detect_move_normalize',
        default_args['start_date'],
        mdag.schedule_interval,
        {
            'expected_matching_files_func': lambda ds, k: [
                insert_current_plaintext_date_function(
                    DEID_FILE_NAME_TEMPLATE
                )(ds, k)
            ],
            'file_date_func': insert_current_date_function(
                '{}/{}'
            ),
            's3_payload_loc_url': S3_PAYLOAD_DEST,
            'vendor_uuid': 'b29eb316-a398-4fdc-b8da-2cff26f86bad',
            'pyspark_normalization_script_name':
            '/home/hadoop/spark/providers/practice_insight/sparkNormalizePracticeInsight.py',
            'pyspark_normalization_args_func': lambda ds, k: [
                '--date', insert_current_date('{}-{}-01', k)
            ],
            'text_warehouse': TEXT_WAREHOUSE,
            'parquet_warehouse': PARQUET_WAREHOUSE,
            'data_feed_type': 'lab',
            'pyspark': True
        }
    ),
    task_id='detect_move_normalize',
    dag=mdag
)

# transaction
fetch_transactional.set_upstream(validate_transactional)
gunzip_transactional.set_upstream(fetch_transactional)

split_transaction_into_parts.set_upstream(gunzip_transactional)

for step in split_transactional_steps:
    step.set_upstream(split_transaction_into_parts)

# cleanup
clean_up_workspace.set_upstream(split_transactional_steps)

# matching
queue_up_for_matching.set_upstream(validate_deid)

# post-matching
detect_move_normalize_dag.set_upstream(
    [queue_up_for_matching] + split_transactional_steps
)
