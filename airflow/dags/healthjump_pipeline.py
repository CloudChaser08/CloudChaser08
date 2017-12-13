from airflow.operators import SubDagOperator, PythonOperator
from datetime import datetime, timedelta

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
          detect_move_normalize, decompression, HVDAG, date_utils]:
    reload(m)

# Applies to all files
TMP_PATH_TEMPLATE = '/tmp/healthjump/emr/{}{}{}/'
DAG_NAME = 'healthjump_pipeline'

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
    S3_TRANSACTION_RAW_URL = 's3://salusv/testing/dewey/airflow/e2e/healthjump/emr/raw/'
    S3_TRANSACTION_PROCESSED_URL_TEMPLATE = 's3://salusv/testing/dewey/airflow/e2e/healthjump/emr/out/{}/{}/{}/'
    S3_PAYLOAD_DEST = 's3://salusv/testing/dewey/airflow/e2e/healthjump/emr/payload/'
else:
    S3_TRANSACTION_RAW_URL = 's3://healthverity/incoming/cardinal/'
    S3_TRANSACTION_PROCESSED_URL_TEMPLATE = 's3://salusv/incoming/emr/healthjump/{}/{}/{}/'
    S3_PAYLOAD_DEST = 's3://salusv/matching/payload/emr/healthjump/'

# Transaction Files
TRANSACTION_TMP_PATH_TEMPLATE = TMP_PATH_TEMPLATE + 'raw/'

# Transaction demographics
TRANSACTION_DEMO_FILE_DESCRIPTION = 'HealthJump transaction demographics file'
TRANSACTION_DEMO_FILE_NAME_ZIP_TEMPLATE = 'HV_HJ_demographics_{}{}{}.zip'
TRANSACTION_DEMO_FILE_NAME_DEID_TEMPLATE = 'HV_HJ_demo_deid'
TRANSACTION_DEMO_FILE_NAME_TEMPLATE = 'HV_HJ_demo_record'

# Transaction cpt
TRANSACTION_CPT_FILE_DESCRIPTION = 'HealthJump transaction cpt file'
TRANSACTION_CPT_FILE_NAME_TEMPLATE = 'HV_HV_cpt_{}{}{}.txt'

# Transaction dx
TRANSACTION_DX_FILE_DESCRIPTION = 'HealthJump transaction dx file'
TRANSACTION_DX_FILE_NAME_TEMPLATE = 'HV_HV_dx_{}{}{}.txt'

# Transaction loinc
TRANSACTION_LOINC_FILE_DESCRIPTION = 'HealthJump transaction loinc file'
TRANSACTION_LOINC_FILE_NAME_TEMPLATE = 'HV_HV_loinc_{}{}{}.txt'

# Transaction ndc
TRANSACTION_NDC_FILE_DESCRIPTION = 'HealthJump transaction ndc file'
TRANSACTION_NDC_FILE_NAME_TEMPLATE = 'HV_HV_ndc_{}{}{}.txt'

get_tmp_dir = date_utils.generate_insert_date_into_template_function(TRANSACTION_TMP_PATH_TEMPLATE)

def get_transaction_file_paths(ds, kwargs):
    return [
        get_tmp_dir(ds, kwargs) + date_utils.insert_date_into_template(template, kwargs)
        for template in [
            TRANSACTION_CPT_FILE_NAME_TEMPLATE,
            TRANSACTION_DEMO_FILE_NAME_TEMPLATE,
            TRANSACTION_DX_FILE_NAME_TEMPLATE,
            TRANSACTION_NDC_FILE_NAME_TEMPLATE,
            TRANSACTION_LOINC_FILE_NAME_TEMPLATE,
        ]
    ]


def encrypted_decrypted_file_paths_function(ds, kwargs):
    file_dir = get_tmp_dir(ds, kwargs)
    encrypted_file_path = file_dir \
        + date_utils.insert_date_into_template(TRANSACTION_DEMO_FILE_NAME_TEMPLATE, kwargs)
    return [
        [encrypted_file_path, encrypted_file_path + '.gz']
    ]


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
                    path_template
                ),
                'file_name_pattern_func'  : date_utils.generate_insert_regex_into_template_function(
                    path_template
                ),
                'minimum_file_size'       : minimum_file_size,
                's3_prefix'               : '/'.join(S3_TRANSACTION_RAW_URL.split('/')[3:]),
                's3_bucket'               : 'healthverity',
                'file_description'        : 'HealthJump ' + task_id + ' file'
            }
        ),
        task_id='validate_' + task_id + '_file',
        dag=mdag
    )


if HVDAG.HVDAG.airflow_env != 'test':
    validate_transaction_cpt = generate_file_validation_dag(
        'transaction cpt', TRANSACTION_CPT_FILE_NAME_TEMPLATE,
        10000
    )
    validate_transaction_demographics = generate_file_validation_dag(
        'transaction demographics', TRANSACTION_DEMO_FILE_NAME_ZIP_TEMPLATE,
        10000
    )
    validate_transaction_dx = generate_file_validation_dag(
        'transaction dx', TRANSACTION_DX_FILE_NAME_TEMPLATE,
        10000
    )
    validate_transaction_loinc = generate_file_validation_dag(
        'transaction loinc', TRANSACTION_LOINC_FILE_NAME_TEMPLATE,
        10000
    )
    validate_transaction_ndc = generate_file_validation_dag(
        'transaction ndc', TRANSACTION_NDC_FILE_NAME_TEMPLATE,
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
                    transaction_file_name_template
                ),
                's3_prefix'              : '/'.join(S3_TRANSACTION_RAW_URL.split('/')[3:]),
                's3_bucket'              : 'salusv' if HVDAG.HVDAG.airflow_env == 'test' else 'healthverity'
            }
        ),
        task_id='fetch_{}_file'.format(task_id),
        dag=mdag
    )


fetch_transaction_cpt = generate_fetch_transaction_dag(
    'transaction_cpt', TRANSACTION_CPT_FILE_NAME_TEMPLATE
)
fetch_transaction_demographics = generate_fetch_transaction_dag(
    'transaction_demographics', TRANSACTION_DEMO_FILE_NAME_ZIP_TEMPLATE
)
fetch_transaction_dx = generate_fetch_transaction_dag(
    'transaction_dx', TRANSACTION_DX_FILE_NAME_TEMPLATE
)
fetch_transaction_loinc = generate_fetch_transaction_dag(
    'transaction_loinc', TRANSACTION_LOINC_FILE_NAME_TEMPLATE
)
fetch_transaction_ndc = generate_fetch_transaction_dag(
    'transaction_ndc', TRANSACTION_NDC_FILE_NAME_TEMPLATE
)


def generate_unzip_step():
    def execute(ds, **kwargs):
        decompression.decompress_zip_file(
            get_tmp_dir(ds, kwargs) + date_utils.insert_date_into_template(TRANSACTION_DEMO_FILE_NAME_ZIP_TEMPLATE, kwargs),
            get_tmp_dir(ds, kwargs)
        )
    return PythonOperator(
        task_id='unzip_demo_file',
        provide_context=True,
        python_callable=execute,
        dag=mdag
    )


unzip_demographics = generate_unzip_step()

decrypt_demographics = SubDagOperator(
    subdag=decrypt_files.decrypt_files(
        DAG_NAME,
        'decrypt_demo_file',
        default_args['start_date'],
        mdag.schedule_interval,
        {
            'tmp_dir_func'                        : date_utils.generate_insert_date_into_template_function(TRANSACTION_TMP_PATH_TEMPLATE),
            'encrypted_decrypted_file_paths_func' : encrypted_decrypted_file_paths_function
        }
    ),
    task_id='decrypt_demo_file',
    dag=mdag
)


def generate_split_dag(task_id, file_template, destination_template):
    return SubDagOperator(
        subdag=split_push_files.split_push_files(
            DAG_NAME,
            'split_{}_files'.format(task_id),
            default_args['start_date'],
            mdag.schedule_interval,
            {
                'tmp_dir_func'             : get_tmp_dir,
                'file_paths_to_split_func' : lambda ds, k: [
                    get_tmp_dir(ds, k) + date_utils.insert_date_into_template(file_template, k)
                ],
                's3_prefix_func'           : date_utils.generate_insert_date_into_template_function(
                    destination_template
                ),
                'num_splits'               : 20
            }
        ),
        task_id='split_{}_files'.format(task_id),
        dag=mdag
    )


split_transaction_cpt = generate_split_dag(
    'transaction_cpt', TRANSACTION_CPT_FILE_NAME_TEMPLATE,
    S3_TRANSACTION_PROCESSED_URL_TEMPLATE + 'cpt/'
)
split_transaction_demographics = generate_split_dag(
    'transaction_demographics', TRANSACTION_DEMO_FILE_NAME_TEMPLATE,
    S3_TRANSACTION_PROCESSED_URL_TEMPLATE + 'demographics/'
)
split_transaction_dx = generate_split_dag(
    'transaction_dx', TRANSACTION_DX_FILE_NAME_TEMPLATE,
    S3_TRANSACTION_PROCESSED_URL_TEMPLATE + 'dx/'
)
split_transaction_loinc = generate_split_dag(
    'transaction_loinc', TRANSACTION_LOINC_FILE_NAME_TEMPLATE,
    S3_TRANSACTION_PROCESSED_URL_TEMPLATE + 'loinc/'
)
split_transaction_ndc = generate_split_dag(
    'transaction_ndc', TRANSACTION_NDC_FILE_NAME_TEMPLATE,
    S3_TRANSACTION_PROCESSED_URL_TEMPLATE + 'ndc/'
)


def clean_up_workspace(task_id):
    return SubDagOperator(
        subdag=clean_up_tmp_dir.clean_up_tmp_dir(
            DAG_NAME,
            'clean_up_workspace_{}'.format(task_id),
            default_args['start_date'],
            mdag.schedule_interval,
            {
                'tmp_path_template': TMP_PATH_TEMPLATE
            }
        ),
        task_id='clean_up_workspace_{}'.format(task_id),
        dag=mdag
    )


clean_up_workspace_cpt = clean_up_workspace('cpt')
clean_up_workspace_demographics = clean_up_workspace('demographics')
clean_up_workspace_dx = clean_up_workspace('dx')
clean_up_workspace_loinc = clean_up_workspace('loinc')
clean_up_workspace_ndc = clean_up_workspace('ndc')

if HVDAG.HVDAG.airflow_env != 'test':
    queue_up_for_matching = SubDagOperator(
        subdag=queue_up_for_matching.queue_up_for_matching(
            DAG_NAME,
            'queue_up_for_matching',
            default_args['start_date'],
            mdag.schedule_interval,
            {
                'source_files_func' : lambda ds, k: [
                    get_tmp_dir(ds, k)
                    + date_utils.insert_date_into_template(TRANSACTION_DEMO_FILE_NAME_DEID_TEMPLATE, k)
                ]
            }
        ),
        task_id='queue_up_for_matching',
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
            'expected_matching_files_func'      : lambda ds,k: [
                date_utils.insert_date_into_template(
                    TRANSACTION_DEMO_FILE_NAME_DEID_TEMPLATE, k
                )
            ],
            'file_date_func'                    : date_utils.generate_insert_date_into_template_function(
                '{}/{}/{}'
            ),
            's3_payload_loc_url'                : S3_PAYLOAD_DEST,
            'vendor_uuid'                       : 'd7bac232-bcc3-4428-a818-d33acc53d52b',
            'pyspark_normalization_script_name' : '/home/hadoop/spark/providers/healthjump/emr/sparkNormalizeHealthJump.py',
            'pyspark_normalization_args_func'   : norm_args,
            'pyspark'                           : True
        }
    ),
    task_id='detect_move_normalize',
    dag=mdag
)

# addon
if HVDAG.HVDAG.airflow_env != 'test':
    fetch_transaction_cpt.set_upstream(validate_transaction_cpt)
    fetch_transaction_demographics.set_upstream(validate_transaction_demographics)
    fetch_transaction_dx.set_upstream(validate_transaction_dx)
    fetch_transaction_loinc.set_upstream(validate_transaction_loinc)
    fetch_transaction_ndc.set_upstream(validate_transaction_ndc)

    queue_up_for_matching.set_upstream(unzip_demographics)
    queue_up_for_matching.set_downstream(clean_up_workspace_demographics)

    detect_move_normalize_dag.set_upstream([
        queue_up_for_matching,
        split_transaction_cpt,
        split_transaction_demographics,
        split_transaction_dx,
        split_transaction_loinc,
        split_transaction_ndc
    ])
else:
    detect_move_normalize_dag.set_upstream([
         split_transaction_cpt,
         split_transaction_demographics,
         split_transaction_dx,
         split_transaction_loinc,
         split_transaction_ndc
    ])

unzip_demographics.set_upstream(fetch_transaction_demographics)
decrypt_demographics.set_upstream(unzip_demographics)

split_transaction_cpt.set_upstream(fetch_transaction_cpt)
split_transaction_demographics.set_upstream(decrypt_demographics)
split_transaction_dx.set_upstream(fetch_transaction_dx)
split_transaction_loinc.set_upstream(fetch_transaction_loinc)
split_transaction_ndc.set_upstream(fetch_transaction_ndc)

# only do one at a time
split_transaction_demographics.set_downstream(clean_up_workspace_demographics)
fetch_transaction_cpt.set_upstream(clean_up_workspace_demographics)
split_transaction_cpt.set_downstream(clean_up_workspace_cpt)
fetch_transaction_dx.set_upstream(clean_up_workspace_cpt)
split_transaction_dx.set_downstream(clean_up_workspace_dx)
fetch_transaction_loinc.set_upstream(clean_up_workspace_dx)
split_transaction_loinc.set_downstream(clean_up_workspace_loinc)
fetch_transaction_ndc.set_upstream(clean_up_workspace_loinc)
split_transaction_ndc.set_downstream(clean_up_workspace_ndc)
