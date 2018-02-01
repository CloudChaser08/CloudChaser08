from airflow.operators import PythonOperator, SubDagOperator
from datetime import datetime, timedelta
from subprocess import check_call
import os

# hv-specific modules
import common.HVDAG as HVDAG
import subdags.s3_validate_file as s3_validate_file
import subdags.s3_fetch_file as s3_fetch_file
import subdags.split_push_files as split_push_files
import subdags.queue_up_for_matching as queue_up_for_matching
import subdags.detect_move_normalize as detect_move_normalize

import util.decompression as decompression
import util.date_utils as date_utils
import util.s3_utils as s3_utils

for m in [s3_validate_file, s3_fetch_file, split_push_files, queue_up_for_matching,
        detect_move_normalize, decompression, HVDAG, date_utils, s3_utils]:
    reload(m)

# Applies to all files
TMP_PATH_TEMPLATE = '/tmp/amazingcharts/emr/{}{}{}/'
DAG_NAME = 'amazingcharts_pipeline'

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2017, 11, 1, 18),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2)
}

mdag = HVDAG.HVDAG(
    dag_id=DAG_NAME,
    schedule_interval="0 18 1 2,5,8,11 *",
    default_args=default_args
)

AMAZINGCHARTS_MONTH_OFFSET = 2

# Applies to all transaction files
if HVDAG.HVDAG.airflow_env == 'test':
    S3_TRANSACTION_RAW_URL = 's3://salusv/testing/dewey/airflow/e2e/amazingcharts/emr/raw/'
    S3_TRANSACTION_PROCESSED_URL_TEMPLATE = 's3://salusv/testing/dewey/airflow/e2e/amazingcharts/emr/out/{0}/{1}/'
    S3_PAYLOAD_DEST = 's3://salusv/testing/dewey/airflow/e2e/amazingcharts/emr/payload/'
else:
    S3_TRANSACTION_RAW_URL = 's3://healthverity/incoming/amazingcharts/'
    S3_TRANSACTION_PROCESSED_URL_TEMPLATE = 's3://salusv/incoming/emr/amazingcharts/{0}/{1}/'
    S3_PAYLOAD_DEST = 's3://salusv/matching/payload/emr/amazingcharts/'

# Transaction Zip file
TRANSACTION_FILE_DESCRIPTION = 'AmazingCharts transaction zip file'
TRANSACTION_FILE_NAME_TEMPLATE = 'healthverity_{0}{1}.zip'

get_tmp_dir = date_utils.generate_insert_date_into_template_function(
    TMP_PATH_TEMPLATE
)

if HVDAG.HVDAG.airflow_env != 'test':
    validate_transaction = SubDagOperator(
        subdag=s3_validate_file.s3_validate_file(
            DAG_NAME,
            'validate_transaction_file',
            default_args['start_date'],
            mdag.schedule_interval,
            {
                'expected_file_name_func' : date_utils.generate_insert_date_into_template_function(
                    TRANSACTION_FILE_NAME_TEMPLATE, month_offset=AMAZINGCHARTS_MONTH_OFFSET
                ),
                'file_name_pattern_func'  : date_utils.generate_insert_regex_into_template_function(
                    TRANSACTION_FILE_NAME_TEMPLATE
                ),
                'minimum_file_size'       : 10000,
                's3_prefix'               : '/'.join(S3_TRANSACTION_RAW_URL.split('/')[3:]),
                's3_bucket'               : S3_TRANSACTION_RAW_URL.split('/')[2],
                'file_description'        : TRANSACTION_FILE_DESCRIPTION
            }
        ),
        task_id='validate_transaction_file',
        dag=mdag
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
                TRANSACTION_FILE_NAME_TEMPLATE, month_offset=AMAZINGCHARTS_MONTH_OFFSET
            ),
            's3_prefix'              : '/'.join(S3_TRANSACTION_RAW_URL.split('/')[3:]),
            's3_bucket'              : S3_TRANSACTION_RAW_URL.split('/')[2]
        }
    ),
    task_id='fetch_transaction_file',
    dag=mdag
)


def generate_unzip_step(zip_filename_template):
    def execute(ds, **kwargs):
        zip_filename = date_utils.insert_date_into_template(
            zip_filename_template, kwargs, month_offset=AMAZINGCHARTS_MONTH_OFFSET
        )

        decompression.decompress_zip_file(
            get_tmp_dir(ds, kwargs) + zip_filename, get_tmp_dir(ds, kwargs)
        )
        os.remove(get_tmp_dir(ds, kwargs) + zip_filename)
    return PythonOperator(
        task_id='unzip_' + zip_filename_template.format('y', 'm').split('.')[0],
        provide_context=True,
        python_callable=execute,
        dag=mdag
    )


unzip_transaction = generate_unzip_step(TRANSACTION_FILE_NAME_TEMPLATE)

unzip_d_costar = generate_unzip_step("d_costar.zip")
unzip_d_date = generate_unzip_step("d_date.zip")
unzip_d_icd10 = generate_unzip_step("d_icd10.zip")
unzip_d_lab_directory = generate_unzip_step("d_lab_directory.zip")
unzip_d_provider = generate_unzip_step("d_provider.zip")
unzip_d_vaccine_cpt = generate_unzip_step("d_vaccine_cpt.zip")
unzip_f_encounter = generate_unzip_step("f_encounter.zip")
unzip_f_lab = generate_unzip_step("f_lab.zip")
unzip_f_procedure = generate_unzip_step("f_procedure.zip")
unzip_d_cpt = generate_unzip_step("d_cpt.zip")
unzip_d_drug = generate_unzip_step("d_drug.zip")
unzip_d_icd9 = generate_unzip_step("d_icd9.zip")
unzip_d_patient = generate_unzip_step("d_patient.zip")
unzip_d_time = generate_unzip_step("d_time.zip")
unzip_f_diagnosis = generate_unzip_step("f_diagnosis.zip")
unzip_f_injection = generate_unzip_step("f_injection.zip")
unzip_f_medication = generate_unzip_step("f_medication.zip")
unzip_hv_tokens = generate_unzip_step("HV_TOKENS.zip")
unzip_steps = [
    unzip_d_costar, unzip_d_date, unzip_d_icd10, unzip_d_lab_directory,
    unzip_d_provider, unzip_d_vaccine_cpt, unzip_f_encounter, unzip_f_lab,
    unzip_f_procedure, unzip_d_cpt, unzip_d_drug, unzip_d_icd9,
    unzip_d_patient, unzip_d_time, unzip_f_diagnosis, unzip_f_injection,
    unzip_f_medication, unzip_hv_tokens
]


def generate_split_step(filename):
    file_id = filename.split('.')[0]

    return SubDagOperator(
        subdag=split_push_files.split_push_files(
            DAG_NAME,
            'split_' + file_id + '_file',
            default_args['start_date'],
            mdag.schedule_interval,
            {
                'tmp_dir_func'             : get_tmp_dir,
                'parts_dir_func'           : lambda ds, k: file_id + '_parts',
                'file_paths_to_split_func' : lambda ds, k: [get_tmp_dir(ds, k) + filename],
                'file_name_pattern_func'   : lambda ds, k: filename,
                's3_prefix_func'           : date_utils.generate_insert_date_into_template_function(
                    S3_TRANSACTION_PROCESSED_URL_TEMPLATE + file_id + '/',
                    month_offset=AMAZINGCHARTS_MONTH_OFFSET
                ),
                'num_splits'               : 20
            }
        ),
        task_id='split_' + file_id + '_file',
        dag=mdag
    )


split_d_costar = generate_split_step("d_costar.psv")
split_d_date = generate_split_step("d_date.psv")
split_d_icd10 = generate_split_step("d_icd10.psv")
split_d_lab_directory = generate_split_step("d_lab_directory.psv")
split_d_provider = generate_split_step("d_provider.psv")
split_d_vaccine_cpt = generate_split_step("d_vaccine_cpt.psv")
split_f_encounter = generate_split_step("f_encounter.psv")
split_f_lab = generate_split_step("f_lab.psv")
split_f_procedure = generate_split_step("f_procedure.psv")
split_d_cpt = generate_split_step("d_cpt.psv")
split_d_drug = generate_split_step("d_drug.psv")
split_d_icd9 = generate_split_step("d_icd9.psv")
split_d_patient = generate_split_step("d_patient.psv")
split_d_time = generate_split_step("d_time.psv")
split_f_diagnosis = generate_split_step("f_diagnosis.psv")
split_f_injection = generate_split_step("f_injection.psv")
split_f_medication = generate_split_step("f_medication.psv")
split_steps = [
    split_d_costar, split_d_date, split_d_icd10, split_d_lab_directory,
    split_d_provider, split_d_vaccine_cpt, split_f_encounter, split_f_lab,
    split_f_procedure, split_d_cpt, split_d_drug, split_d_icd9,
    split_d_patient, split_d_time, split_f_diagnosis, split_f_injection,
    split_f_medication
]


def push_deid_file_step():
    def execute(ds, **kwargs):
        s3_utils.copy_file(
            get_tmp_dir(ds, kwargs) + 'HV_TOKENS.txt', date_utils.insert_date_into_template(
                S3_TRANSACTION_PROCESSED_URL_TEMPLATE + 'deid/HV_TOKENS_{0}_{1}.txt', kwargs,
                month_offset=AMAZINGCHARTS_MONTH_OFFSET
            )
        )
    return PythonOperator(
        task_id='push_deid_file',
        provide_context=True,
        python_callable=execute,
        dag=mdag
    )


push_deid_file = push_deid_file_step()


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
                'source_files_func' : lambda ds, k: [
                    date_utils.insert_date_into_template(
                        S3_TRANSACTION_PROCESSED_URL_TEMPLATE + 'deid/HV_TOKENS_{0}_{1}.txt', k,
                        month_offset=AMAZINGCHARTS_MONTH_OFFSET
                    )
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
    base = ['--date', date_utils.insert_date_into_template('{}-{}-{}', k, month_offset=AMAZINGCHARTS_MONTH_OFFSET)]
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
                    'HV_TOKENS_{}_{}.txt', k, month_offset=AMAZINGCHARTS_MONTH_OFFSET
                )
            ],
            'file_date_func'                    : date_utils.generate_insert_date_into_template_function(
                '{}/{}', month_offset=AMAZINGCHARTS_MONTH_OFFSET
            ),
            's3_payload_loc_url'                : S3_PAYLOAD_DEST,
            'vendor_uuid'                       : 'f00ca57c-4935-494e-9e40-b064fd38afda',
            'pyspark_normalization_script_name' : '/home/hadoop/spark/providers/amazingcharts/emr/sparkNormalizeAmazingCharts.py',
            'pyspark_normalization_args_func'   : norm_args,
            'pyspark'                           : True
        }
    ),
    task_id='detect_move_normalize',
    dag=mdag
)

if HVDAG.HVDAG.airflow_env != 'test':
    fetch_transaction.set_upstream(validate_transaction)

    # matching
    queue_up_for_matching.set_upstream(push_deid_file)

    # post-matching
    detect_move_normalize_dag.set_upstream(
        split_steps + [queue_up_for_matching]
    )
else:
    detect_move_normalize_dag.set_upstream(split_steps)

unzip_transaction.set_upstream(fetch_transaction)
unzip_transaction.set_downstream(unzip_steps)

split_d_costar.set_upstream(unzip_d_costar)
split_d_date.set_upstream(unzip_d_date)
split_d_icd10.set_upstream(unzip_d_icd10)
split_d_lab_directory.set_upstream(unzip_d_lab_directory)
split_d_provider.set_upstream(unzip_d_provider)
split_d_vaccine_cpt.set_upstream(unzip_d_vaccine_cpt)
split_f_encounter.set_upstream(unzip_f_encounter)
split_f_lab.set_upstream(unzip_f_lab)
split_f_procedure.set_upstream(unzip_f_procedure)
split_d_cpt.set_upstream(unzip_d_cpt)
split_d_drug.set_upstream(unzip_d_drug)
split_d_icd9.set_upstream(unzip_d_icd9)
split_d_patient.set_upstream(unzip_d_patient)
split_d_time.set_upstream(unzip_d_time)
split_f_diagnosis.set_upstream(unzip_f_diagnosis)
split_f_injection.set_upstream(unzip_f_injection)
split_f_medication.set_upstream(unzip_f_medication)
push_deid_file.set_upstream(unzip_hv_tokens)

# cleanup
clean_up_workspace.set_upstream(split_steps + [push_deid_file])
