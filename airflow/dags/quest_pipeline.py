from airflow.models import Variable
from airflow.operators import PythonOperator, SubDagOperator
from datetime import datetime, timedelta
from subprocess import check_call

# hv-specific modules
import common.HVDAG as HVDAG
import subdags.s3_validate_file as s3_validate_file
import subdags.s3_fetch_file as s3_fetch_file
import subdags.decrypt_files as decrypt_files
import subdags.split_push_files as split_push_files
import subdags.queue_up_for_matching as queue_up_for_matching
import subdags.detect_move_normalize as detect_move_normalize
import subdags.update_analytics_db as update_analytics_db

import util.decompression as decompression
import util.date_utils as date_utils

for m in [s3_validate_file, s3_fetch_file, decrypt_files,
        split_push_files, queue_up_for_matching,
        detect_move_normalize, decompression, HVDAG,
        update_analytics_db, date_utils]:
    reload(m)

# Applies to all files
TMP_PATH_TEMPLATE = '/tmp/quest/labtests/{}{}{}/'
DAG_NAME = 'quest_pipeline'

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2017, 4, 13, 14),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2)
}

mdag = HVDAG.HVDAG(
    dag_id=DAG_NAME,
    schedule_interval="0 14 * * *",
    default_args=default_args
)

QUEST_DAY_OFFSET = -3

# Applies to all transaction files
if HVDAG.HVDAG.airflow_env == 'test':
    S3_TRANSACTION_RAW_URL = 's3://salusv/testing/dewey/airflow/e2e/quest/labtests/raw/'
    S3_TRANSACTION_PROCESSED_URL_TEMPLATE = 's3://salusv/testing/dewey/airflow/e2e/quest/labtests/out/{}/{}/{}/'
    S3_PAYLOAD_DEST = 's3://salusv/testing/dewey/airflow/e2e/quest/labtests/payload/'
else:
    S3_TRANSACTION_RAW_URL = 's3://healthverity/incoming/quest/'
    S3_TRANSACTION_PROCESSED_URL_TEMPLATE = 's3://salusv/incoming/labtests/quest/{}/{}/{}/'
    S3_PAYLOAD_DEST = 's3://salusv/matching/payload/labtests/quest/'

# Transaction Addon file
TRANSACTION_ADDON_TMP_PATH_TEMPLATE = TMP_PATH_TEMPLATE + 'raw/addon/'
TRANSACTION_ADDON_S3_SPLIT_URL = S3_TRANSACTION_PROCESSED_URL_TEMPLATE + 'addon/'
TRANSACTION_ADDON_FILE_DESCRIPTION = 'Quest transaction addon file'
TRANSACTION_ADDON_FILE_NAME_TEMPLATE = 'HealthVerity_{}_1_PlainTxt.txt.zip'
TRANSACTION_ADDON_UNZIPPED_FILE_NAME_TEMPLATE = 'HealthVerity_{}_1_PlainTxt.txt'
TRANSACTION_ADDON_DAG_NAME = 'validate_fetch_transaction_addon_file'
MINIMUM_TRANSACTION_FILE_SIZE = 500

# Transaction Trunk file
TRANSACTION_TRUNK_TMP_PATH_TEMPLATE = TMP_PATH_TEMPLATE + 'raw/trunk/'
TRANSACTION_TRUNK_S3_SPLIT_URL = S3_TRANSACTION_PROCESSED_URL_TEMPLATE + 'trunk/'
TRANSACTION_TRUNK_FILE_DESCRIPTION = 'Quest transaction trunk file'
TRANSACTION_TRUNK_UNZIPPED_FILE_NAME_TEMPLATE = 'HealthVerity_{}_2'
TRANSACTION_TRUNK_FILE_NAME_TEMPLATE = 'HealthVerity_{}_2.gz.zip'
TRANSACTION_TRUNK_DAG_NAME = 'validate_fetch_transaction_trunk_file'

# Deid file
DEID_FILE_DESCRIPTION = 'Quest deid file'
DEID_FILE_NAME_TEMPLATE = 'HealthVerity_{}_1_DeID.txt.zip'
DEID_UNZIPPED_FILE_NAME_TEMPLATE = 'HealthVerity_{}_1_DeID.txt'
MINIMUM_DEID_FILE_SIZE = 500

def get_formatted_date(ds, kwargs):
    return date_utils.generate_insert_date_into_template_function('{}{}{}',
        day_offset = QUEST_DAY_OFFSET)(ds,kwargs) \
            + date_utils.generate_insert_date_into_template_function('{1}{2}',
                day_offset = QUEST_DAY_OFFSET + 1)(ds,kwargs)

def insert_formatted_date_function(template):
    def out(ds, kwargs):
        return template.format(get_formatted_date(ds, kwargs))
    return out

def insert_formatted_regex_function(template):
    def out(ds, kwargs):
        return template.format('\d{12}')
    return out

get_tmp_dir = date_utils.generate_insert_date_into_template_function(
    TMP_PATH_TEMPLATE
    )
get_addon_tmp_dir = date_utils.generate_insert_date_into_template_function(
    TRANSACTION_ADDON_TMP_PATH_TEMPLATE
    )
get_trunk_tmp_dir = date_utils.generate_insert_date_into_template_function(
    TRANSACTION_TRUNK_TMP_PATH_TEMPLATE
    )

def get_deid_file_urls(ds, kwargs):
    return [S3_TRANSACTION_RAW_URL + DEID_FILE_NAME_TEMPLATE.format(
        get_formatted_date(ds, kwargs)
    )]

def encrypted_decrypted_file_paths_function(ds, kwargs):
    file_dir = get_addon_tmp_dir(ds, kwargs)
    encrypted_file_path = file_dir \
        + TRANSACTION_ADDON_UNZIPPED_FILE_NAME_TEMPLATE.format(
            get_formatted_date(ds, kwargs)
        )
    return [
        [encrypted_file_path, encrypted_file_path + '.gz']
    ]

def get_addon_unzipped_file_paths(ds, kwargs):
    file_dir = get_addon_tmp_dir(ds, kwargs)
    return [file_dir
            + TRANSACTION_ADDON_UNZIPPED_FILE_NAME_TEMPLATE.format(
                get_formatted_date(ds, kwargs)
            )]

def get_trunk_unzipped_file_paths(ds, kwargs):
    file_dir = get_trunk_tmp_dir(ds, kwargs)
    return [file_dir
            + TRANSACTION_TRUNK_UNZIPPED_FILE_NAME_TEMPLATE.format(
                get_formatted_date(ds, kwargs)
            )]

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
                'expected_file_name_func' : insert_formatted_date_function(
                    path_template
                ),
                'file_name_pattern_func'  : insert_formatted_regex_function(
                    path_template
                ),
                'minimum_file_size'       : minimum_file_size,
                's3_prefix'               : '/'.join(S3_TRANSACTION_RAW_URL.split('/')[3:]),
                's3_bucket'               : 'healthverity',
                'file_description'        : 'Quest ' + task_id + ' file'
            }
        ),
        task_id='validate_' + task_id + '_file',
        dag=mdag
    )

if HVDAG.HVDAG.airflow_env != 'test':
    validate_addon = generate_transaction_file_validation_dag(
        'addon', TRANSACTION_ADDON_FILE_NAME_TEMPLATE,
        MINIMUM_TRANSACTION_FILE_SIZE
    )
    validate_trunk = generate_transaction_file_validation_dag(
        'trunk', TRANSACTION_TRUNK_FILE_NAME_TEMPLATE,
        MINIMUM_TRANSACTION_FILE_SIZE
    )
    validate_deid = generate_transaction_file_validation_dag(
        'deid', DEID_FILE_NAME_TEMPLATE,
        MINIMUM_DEID_FILE_SIZE
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
                'tmp_path_template'      : local_path_template,
                'expected_file_name_func': insert_formatted_date_function(
                    file_name_template
                ),
                's3_prefix'              : s3_path_template,
                's3_bucket'              : 'salusv' if HVDAG.HVDAG.airflow_env == 'test' else 'healthverity'
            }
        ),
        task_id='fetch_' + task_id + '_file',
        dag=mdag
    )


fetch_addon = generate_fetch_dag(
    "addon",
    '/'.join(S3_TRANSACTION_RAW_URL.split('/')[3:]),
    TRANSACTION_ADDON_TMP_PATH_TEMPLATE,
    TRANSACTION_ADDON_FILE_NAME_TEMPLATE
)
fetch_trunk = generate_fetch_dag(
    "trunk",
    '/'.join(S3_TRANSACTION_RAW_URL.split('/')[3:]),
    TRANSACTION_TRUNK_TMP_PATH_TEMPLATE,
    TRANSACTION_TRUNK_FILE_NAME_TEMPLATE
)


def unzip_step(task_id, tmp_path_template, filename_template, tmp_dir_func):
    def execute(ds, **kwargs):
        tmp_dir = tmp_dir_func(ds, kwargs)
        decompression.decompress_zip_file(
            tmp_dir + filename_template.format(
                get_formatted_date(ds, kwargs)
            ), tmp_dir
        )
    return PythonOperator(
        task_id='unzip_' + task_id + '_file',
        provide_context=True,
        python_callable=execute,
        dag=mdag
    )


unzip_addon = unzip_step(
    "addon", TRANSACTION_ADDON_TMP_PATH_TEMPLATE,
    TRANSACTION_ADDON_FILE_NAME_TEMPLATE,
    get_addon_tmp_dir
)
unzip_trunk = unzip_step(
    "trunk", TRANSACTION_TRUNK_TMP_PATH_TEMPLATE,
    TRANSACTION_TRUNK_FILE_NAME_TEMPLATE,
    get_trunk_tmp_dir
)


decrypt_addon = SubDagOperator(
    subdag=decrypt_files.decrypt_files(
        DAG_NAME,
        'decrypt_addon_file',
        default_args['start_date'],
        mdag.schedule_interval,
        {
            'tmp_dir_func'                        : get_addon_tmp_dir,
            'encrypted_decrypted_file_paths_func' : encrypted_decrypted_file_paths_function
        }
    ),
    task_id='decrypt_addon_file',
    dag=mdag
)


def gunzip_step(task_id, tmp_path_template, tmp_file_template):
    def execute(ds, **kwargs):
        # insert exectuion date into tmp_path_template and insert formatted
        # date into tmp_file_template
        decompression.decompress_gzip_file(
            date_utils.insert_date_into_template(tmp_path_template, kwargs)
            +
            tmp_file_template.format(get_formatted_date(ds, kwargs))
        )
    return PythonOperator(
        task_id='gunzip_' + task_id + '_file',
        provide_context=True,
        python_callable=execute,
        dag=mdag
    )


gunzip_trunk = gunzip_step(
    "trunk", TRANSACTION_TRUNK_TMP_PATH_TEMPLATE,
    TRANSACTION_TRUNK_UNZIPPED_FILE_NAME_TEMPLATE
)


def split_step(task_id, tmp_dir_func, file_paths_to_split_func, s3_destination, num_splits, path_template):
    return SubDagOperator(
        subdag=split_push_files.split_push_files(
            DAG_NAME,
            'split_' + task_id + '_file',
            default_args['start_date'],
            mdag.schedule_interval,
            {
                'tmp_dir_func'             : tmp_dir_func,
                'file_paths_to_split_func' : file_paths_to_split_func,
                'file_name_pattern_func'  : insert_formatted_regex_function(
                    path_template
                ),
                's3_prefix_func'           : date_utils.generate_insert_date_into_template_function(
                    s3_destination, day_offset = QUEST_DAY_OFFSET
                ),
                'num_splits'               : num_splits
            }
        ),
        task_id='split_' + task_id + '_file',
        dag=mdag
    )


split_addon = split_step(
    "addon", get_addon_tmp_dir, get_addon_unzipped_file_paths,
    TRANSACTION_ADDON_S3_SPLIT_URL, 20, TRANSACTION_ADDON_FILE_NAME_TEMPLATE
)
split_trunk = split_step(
    "trunk", get_trunk_tmp_dir, get_trunk_unzipped_file_paths,
    TRANSACTION_TRUNK_S3_SPLIT_URL, 20, TRANSACTION_TRUNK_FILE_NAME_TEMPLATE
)


def clean_up_workspace_step(task_id, template):
    def execute(ds, **kwargs):
        check_call([
            'rm', '-rf', date_utils.insert_date_into_template(template, kwargs)
        ])
    return PythonOperator(
        task_id='clean_up_workspace_' + task_id,
        provide_context=True,
        python_callable=execute,
        trigger_rule='all_done',
        dag=mdag
    )


clean_up_workspace = clean_up_workspace_step("all", TMP_PATH_TEMPLATE)

if HVDAG.HVDAG.airflow_env != 'test':
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
    base = ['--date', date_utils.insert_date_into_template('{}-{}-{}', k, day_offset = QUEST_DAY_OFFSET)]
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
                insert_formatted_date_function(
                    DEID_UNZIPPED_FILE_NAME_TEMPLATE
                )(ds, k)
            ],
            'file_date_func'                    : date_utils.generate_insert_date_into_template_function(
                '{}/{}/{}', day_offset = QUEST_DAY_OFFSET
                ),
            's3_payload_loc_url'                : S3_PAYLOAD_DEST,
            'vendor_uuid'                       : '1b3f553d-7db8-43f3-8bb0-6e0b327320d9',
            'pyspark_normalization_script_name' : '/home/hadoop/spark/providers/quest/sparkNormalizeQuest.py',
            'pyspark_normalization_args_func'   : norm_args,
            'pyspark'                           : True
        }
    ),
    task_id='detect_move_normalize',
    dag=mdag
)

sql_new_template = """
    MSCK REPAIR TABLE labtests_20170216
"""

if HVDAG.HVDAG.airflow_env != 'test':
    update_analytics_db = SubDagOperator(
        subdag=update_analytics_db.update_analytics_db(
            DAG_NAME,
            'update_analytics_db',
            default_args['start_date'],
            mdag.schedule_interval,
            {
                'sql_command_func' : lambda ds, k: sql_new_template
            }
        ),
        task_id='update_analytics_db',
        dag=mdag
    )

if HVDAG.HVDAG.airflow_env != 'test':
    fetch_addon.set_upstream(validate_addon)
    fetch_trunk.set_upstream(validate_trunk)

    # matching
    queue_up_for_matching.set_upstream(validate_deid)

    # post-matching
    detect_move_normalize_dag.set_upstream(
        [queue_up_for_matching, split_trunk, split_addon]
    )
    update_analytics_db.set_upstream(detect_move_normalize_dag)
else:
    detect_move_normalize_dag.set_upstream(
        [split_trunk, split_addon]
    )

unzip_addon.set_upstream(fetch_addon)
decrypt_addon.set_upstream(unzip_addon)
split_addon.set_upstream(decrypt_addon)

unzip_trunk.set_upstream(fetch_trunk)
gunzip_trunk.set_upstream(unzip_trunk)
split_trunk.set_upstream(gunzip_trunk)

# cleanup
clean_up_workspace.set_upstream(
    [split_trunk, split_addon]
)
