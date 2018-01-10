# Note:
#
# Practice Insight's monthly files are large and their normalization
# routine is complex, so we split up their monthly updates into 4
# equal parts before ingesting them.

from airflow.operators import PythonOperator, SubDagOperator
from datetime import datetime, timedelta
from subprocess import check_call
import os

# hv-specific modules
import common.HVDAG as HVDAG
import subdags.s3_validate_file as s3_validate_file
import subdags.s3_fetch_file as s3_fetch_file
import subdags.decrypt_files as decrypt_files
import subdags.split_push_files as split_push_files
import subdags.queue_up_for_matching as queue_up_for_matching
import subdags.detect_move_normalize as detect_move_normalize
import subdags.update_analytics_db as update_analytics_db
import util.date_utils as date_utils

import util.decompression as decompression

for m in [s3_validate_file, s3_fetch_file, decrypt_files,
          split_push_files, queue_up_for_matching,
          detect_move_normalize, decompression,
          update_analytics_db, date_utils]:
    reload(m)

# Applies to all files
TMP_PATH_TEMPLATE = '/tmp/practice_insight/medicalclaims/{}{}{}/'
DAG_NAME = 'practice_insight_pipeline'

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2018, 1, 16, 8),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2)
}

mdag = HVDAG.HVDAG(
    dag_id=DAG_NAME,
    schedule_interval="0 8 16 * *",
    default_args=default_args
)

PRACTICE_INSIGHT_OFFSET = 1

# Applies to all transaction files
if HVDAG.HVDAG.airflow_env == 'test':
    S3_TRANSACTION_RAW_URL = 's3://salusv/testing/dewey/airflow/e2e/practice_insight/medicalclaims/raw/'
    S3_TRANSACTION_PROCESSED_837_URL_TEMPLATE = 's3://salusv/testing/dewey/airflow/e2e/practice_insight/medicalclaims/out/{}/{}/'
    S3_PAYLOAD_DEST = 's3://salusv/testing/dewey/airflow/e2e/practice_insight/medicalclaims/payload/'
else:
    S3_TRANSACTION_RAW_URL = 's3://healthverity/incoming/practiceinsight/'
    S3_TRANSACTION_PROCESSED_837_URL_TEMPLATE = 's3://salusv/incoming/medicalclaims/practice_insight/{}/{}/'
    S3_PAYLOAD_DEST = 's3://salusv/matching/payload/medicalclaims/practice_insight/'

TRANSACTION_UNZIPPED_837_FILE_NAME_TEMPLATE = 'HV.data.837.{}.{}.csv'
TRANSACTION_837_FILE_NAME_TEMPLATE = TRANSACTION_UNZIPPED_837_FILE_NAME_TEMPLATE + '.gz'
S3_TRANSACTION_PROCESSED_835_URL_TEMPLATE = 's3://salusv/incoming/era/practice_insight/{}/{}/'
TRANSACTION_UNZIPPED_835_FILE_NAME_TEMPLATE = 'HV.data.835.{}.{}.csv'
TRANSACTION_835_FILE_NAME_TEMPLATE = TRANSACTION_UNZIPPED_835_FILE_NAME_TEMPLATE + '.gz'

# Deid file
DEID_FILE_NAME_TEMPLATE = 'HV.phi.{}.{}.o'


get_tmp_dir = date_utils.generate_insert_date_into_template_function(TMP_PATH_TEMPLATE)


def generate_pi_insert_date_function(template):
    return lambda ds, k: template.split('.')[0] + '.' + date_utils.insert_date_into_template(
        '.'.join(template.split('.')[1:]), k, month_format='%b', month_offset=PRACTICE_INSIGHT_OFFSET
    ).lower()


def get_deid_file_urls(ds, kwargs):
    return [
        S3_TRANSACTION_RAW_URL
        + generate_pi_insert_date_function(
            DEID_FILE_NAME_TEMPLATE
        )(ds, kwargs)
    ]


def get_837_part_tmp_dir(part):
    def out(ds, kwargs):
        return get_tmp_dir(ds, kwargs) + 'presplit/' + str(part) + '/'
    return out


def get_unzipped_837_file_paths(part):
    """
    Function for getting the filepath for the given part of an 837 file
    """
    def out(ds, kwargs):
        return [
            get_837_part_tmp_dir(part)(ds, kwargs) + generate_pi_insert_date_function(
                TRANSACTION_UNZIPPED_837_FILE_NAME_TEMPLATE
            )(ds, kwargs)
        ]
    return out


def get_unzipped_835_file_paths(ds, kwargs):
    return [
        get_tmp_dir(ds, kwargs) + generate_pi_insert_date_function(
            TRANSACTION_UNZIPPED_835_FILE_NAME_TEMPLATE
        )(ds, kwargs)
    ]


def generate_transaction_file_validation_task(
        task_id, path_template, minimum_file_size
):
    return SubDagOperator(
        subdag=s3_validate_file.s3_validate_file(
            DAG_NAME,
            'validate_' + task_id + '_file',
            default_args['start_date'],
            mdag.schedule_interval,
            {
                'expected_file_name_func': generate_pi_insert_date_function(path_template),
                'file_name_pattern_func': date_utils.generate_insert_regex_into_template_function(
                    path_template, month_regex='[a-z]{3}'
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


if HVDAG.HVDAG.airflow_env != 'test':
    validate_transactional_837 = generate_transaction_file_validation_task(
        'transaction', TRANSACTION_837_FILE_NAME_TEMPLATE,
        1000000
    )
    validate_deid = generate_transaction_file_validation_task(
        'deid', DEID_FILE_NAME_TEMPLATE,
        10000000
    )
    validate_transactional_835 = generate_transaction_file_validation_task(
        'transaction_835', TRANSACTION_835_FILE_NAME_TEMPLATE,
        1000000
    )


def generate_fetch_transaction_task(task_id, file_name_template):
    return SubDagOperator(
        subdag=s3_fetch_file.s3_fetch_file(
            DAG_NAME,
            'fetch_' + task_id + '_file',
            default_args['start_date'],
            mdag.schedule_interval,
            {
                'tmp_path_template': TMP_PATH_TEMPLATE,
                'expected_file_name_func': generate_pi_insert_date_function(
                    file_name_template
                ),
                's3_prefix': '/'.join(S3_TRANSACTION_RAW_URL.split('/')[3:]),
                's3_bucket': 'salusv' if HVDAG.HVDAG.airflow_env == 'test' else 'healthverity'
            }
        ),
        task_id='fetch_' + task_id + '_file',
        dag=mdag
    )


fetch_transactional_837 = generate_fetch_transaction_task(
    'transaction_837', TRANSACTION_837_FILE_NAME_TEMPLATE
)
fetch_transactional_835 = generate_fetch_transaction_task(
    'transaction_835', TRANSACTION_835_FILE_NAME_TEMPLATE
)


def generate_gunzip_task(task_id, file_name_template):
    def execute(ds, **kwargs):
        tmp_dir = get_tmp_dir(ds, kwargs)
        decompression.decompress_gzip_file(
            tmp_dir + generate_pi_insert_date_function(
                file_name_template
            )(ds, kwargs)
        )

    return PythonOperator(
        task_id='gunzip_' + task_id + '_file',
        provide_context=True,
        python_callable=execute,
        dag=mdag
    )


gunzip_transactional_837 = generate_gunzip_task(
    'transaction_837', TRANSACTION_837_FILE_NAME_TEMPLATE
)
gunzip_transactional_835 = generate_gunzip_task(
    'transaction_835', TRANSACTION_835_FILE_NAME_TEMPLATE
)


def split_transaction_into_parts_func(ds, **kwargs):
    """
    This function will split the transaction file into 4 parts to make
    each month more easily manageable by the normalization routine.

    This function also then moves each part (i) into a new directory
    presplit/i

    This step only applies to 837 transactions
    """
    check_call([
        'mkdir', '-p', get_tmp_dir(ds, kwargs) + 'presplit/'
    ])
    check_call([
        'split', '-n', 'l/4', get_tmp_dir(ds, kwargs)
        + generate_pi_insert_date_function(
            TRANSACTION_UNZIPPED_837_FILE_NAME_TEMPLATE
        )(ds, kwargs),
        get_tmp_dir(ds, kwargs) + 'presplit/'
        + generate_pi_insert_date_function(
            TRANSACTION_UNZIPPED_837_FILE_NAME_TEMPLATE
        )(ds, kwargs) + '.'
    ])
    for i in range(1, 5):
        os.mkdir(get_837_part_tmp_dir(i)(ds, kwargs))
        check_call([
            'mkdir', '-p', get_837_part_tmp_dir(i)(ds, kwargs)
        ])
        os.rename(
            get_tmp_dir(ds, kwargs) + 'presplit/'
            + filter(
                lambda f: f.startswith(
                    generate_pi_insert_date_function(
                        TRANSACTION_UNZIPPED_837_FILE_NAME_TEMPLATE
                    )(ds, kwargs)
                ),
                os.listdir(get_tmp_dir(ds, kwargs) + 'presplit/')
            )[0],
            get_837_part_tmp_dir(i)(ds, kwargs)
            + generate_pi_insert_date_function(
                TRANSACTION_UNZIPPED_837_FILE_NAME_TEMPLATE
            )(ds, kwargs)
        )


split_transaction_into_parts = PythonOperator(
    task_id='split_transaction_into_parts',
    provide_context=True,
    python_callable=split_transaction_into_parts_func,
    dag=mdag
)


def split_step(
        task_id, tmp_dir_func, file_paths_to_split_func,
        s3_destination, num_splits, filename_template
):
    return SubDagOperator(
        subdag=split_push_files.split_push_files(
            DAG_NAME,
            'split_' + task_id + '_file',
            default_args['start_date'],
            mdag.schedule_interval,
            {
                'tmp_dir_func': tmp_dir_func,
                'file_paths_to_split_func': file_paths_to_split_func,
                'file_name_pattern_func': date_utils.generate_insert_regex_into_template_function(
                    filename_template, month_regex='[a-z]{3}'
                ),
                's3_prefix_func': date_utils.generate_insert_date_into_template_function(
                    s3_destination, month_offset=PRACTICE_INSIGHT_OFFSET
                ),
                'num_splits': num_splits
            }
        ),
        task_id='split_' + task_id + '_file',
        dag=mdag
    )


split_transactional_837_steps = map(
    lambda i: split_step(
        "transaction_837_" + str(i),
        get_837_part_tmp_dir(i),
        get_unzipped_837_file_paths(i),
        S3_TRANSACTION_PROCESSED_837_URL_TEMPLATE + str(i) + '/', 20,
        TRANSACTION_837_FILE_NAME_TEMPLATE
    ),
    range(1, 5)
)
split_transactional_835 = split_step(
    'transaction_835', get_tmp_dir, get_unzipped_835_file_paths,
    S3_TRANSACTION_PROCESSED_835_URL_TEMPLATE, 20,
    TRANSACTION_835_FILE_NAME_TEMPLATE
)


def clean_up_workspace_func(ds, **kwargs):
    check_call([
        'rm', '-rf', get_tmp_dir(ds, kwargs)
    ])


clean_up_workspace = PythonOperator(
    task_id='clean_up_workspace',
    provide_context=True,
    python_callable=clean_up_workspace_func,
    trigger_rule='all_done',
    dag=mdag
)

if HVDAG.HVDAG.airflow_env != 'test':
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


def norm_args(ds, k):
    base = ['--date', date_utils.insert_date_into_template('{}-{}-01', k, month_offset=PRACTICE_INSIGHT_OFFSET)]
    if HVDAG.HVDAG.airflow_env == 'test':
        base += ['--airflow_test']

    return base


#
# Post-Matching
#
def generate_detect_move_normalize_dag():
    return SubDagOperator(
        subdag=detect_move_normalize.detect_move_normalize(
            DAG_NAME,
            'detect_move_normalize',
            default_args['start_date'],
            mdag.schedule_interval,
            {
                'expected_matching_files_func': lambda ds, k: [
                    generate_pi_insert_date_function(
                        DEID_FILE_NAME_TEMPLATE
                    )(ds, k)
                ],
                'file_date_func': date_utils.generate_insert_date_into_template_function(
                    '{}/{}', month_offset=PRACTICE_INSIGHT_OFFSET
                ),
                's3_payload_loc_url': S3_PAYLOAD_DEST,
                'vendor_uuid': 'b29eb316-a398-4fdc-b8da-2cff26f86bad',
                'pyspark_normalization_script_name':
                '/home/hadoop/spark/providers/practice_insight/medicalclaims/sparkNormalizePracticeInsight.py',
                'pyspark_normalization_args_func': norm_args,
                'pyspark': True,
                'emr_node_type': 'm4.2xlarge'
            }
        ),
        task_id='detect_move_normalize',
        dag=mdag
    )


detect_move_normalize_dag = generate_detect_move_normalize_dag()

sql_template = """
    MSCK REPAIR TABLE medicalclaims_new
"""

sql_template_835 = """
    ALTER TABLE era_practiceinsight ADD PARTITION (part_processdate='{0}/{1}')
    LOCATION 's3a://salusv/incoming/era/practice_insight/{0}/{1}/'
"""

if HVDAG.HVDAG.airflow_env != 'test':
    update_analytics_db_837 = SubDagOperator(
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
    update_analytics_db_835 = SubDagOperator(
        subdag=update_analytics_db.update_analytics_db(
            DAG_NAME,
            'update_analytics_db_835',
            default_args['start_date'],
            mdag.schedule_interval,
            {
                'sql_command_func' : date_utils.generate_insert_date_into_template_function(
                    sql_template_835, month_offset=PRACTICE_INSIGHT_OFFSET
                )
            }
        ),
        task_id='update_analytics_db_835',
        dag=mdag
    )


# transaction
if HVDAG.HVDAG.airflow_env != 'test':
    fetch_transactional_837.set_upstream(validate_transactional_837)
    fetch_transactional_835.set_upstream(validate_transactional_835)
    queue_up_for_matching.set_upstream(validate_deid)

    post_norm_steps = split_transactional_837_steps
    post_norm_steps.append(queue_up_for_matching)
    detect_move_normalize_dag.set_upstream(post_norm_steps)
    update_analytics_db_837.set_upstream(detect_move_normalize_dag)
    update_analytics_db_835.set_upstream(split_transactional_835)
else:
    detect_move_normalize_dag.set_upstream(
        split_transactional_837_steps
    )

# transaction 837
gunzip_transactional_837.set_upstream(fetch_transactional_837)
split_transaction_into_parts.set_upstream(gunzip_transactional_837)
split_transaction_into_parts.set_downstream(split_transactional_837_steps)

# transaction 835
gunzip_transactional_835.set_upstream(fetch_transactional_835)
split_transactional_835.set_upstream(gunzip_transactional_835)

# cleanup
pre_processing_steps = split_transactional_837_steps
pre_processing_steps.append(split_transactional_835)
clean_up_workspace.set_upstream(pre_processing_steps)
