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
unzip_hv_tokens = generate_unzip_step("HV_TOKENS.zip")

unzip_dict = {
    'd_costar': generate_unzip_step("d_costar.zip"),
    'd_date': generate_unzip_step("d_date.zip"),
    'd_icd10': generate_unzip_step("d_icd10.zip"),
    'd_lab_directory': generate_unzip_step("d_lab_directory.zip"),
    'd_provider': generate_unzip_step("d_provider.zip"),
    'd_vaccine_cpt': generate_unzip_step("d_vaccine_cpt.zip"),
    'f_encounter': generate_unzip_step("f_encounter.zip"),
    'f_lab': generate_unzip_step("f_lab.zip"),
    'f_procedure': generate_unzip_step("f_procedure.zip"),
    'd_cpt': generate_unzip_step("d_cpt.zip"),
    'd_drug': generate_unzip_step("d_drug.zip"),
    'd_icd9': generate_unzip_step("d_icd9.zip"),
    'd_patient': generate_unzip_step("d_patient.zip"),
    'd_time': generate_unzip_step("d_time.zip"),
    'f_diagnosis': generate_unzip_step("f_diagnosis.zip"),
    'f_injection': generate_unzip_step("f_injection.zip"),
    'f_medication': generate_unzip_step("f_medication.zip")
}

def generate_iconv_step(filename):
    def execute(ds, **kwargs):
        check_call([
            'iconv', '-f', 'utf-16', '-t', 'utf-8',
            '-o', filename + '.utf8', filename
        ])

    return PythonOperator(
        python_callable=execute,
        provide_context=True,
        task_id='convert_{}'.format(filename),
        dag=mdag
    )



iconv_dict = {
    'd_costar': generate_iconv_step("d_costar.psv"),
    'd_date': generate_iconv_step("d_date.psv"),
    'd_icd10': generate_iconv_step("d_icd10.psv"),
    'd_lab_directory': generate_iconv_step("d_lab_directory.psv"),
    'd_provider': generate_iconv_step("d_provider.psv"),
    'd_vaccine_cpt': generate_iconv_step("d_vaccine_cpt.psv"),
    'f_encounter': generate_iconv_step("f_encounter.psv"),
    'f_lab': generate_iconv_step("f_lab.psv"),
    'f_procedure': generate_iconv_step("f_procedure.psv"),
    'd_cpt': generate_iconv_step("d_cpt.psv"),
    'd_drug': generate_iconv_step("d_drug.psv"),
    'd_icd9': generate_iconv_step("d_icd9.psv"),
    'd_patient': generate_iconv_step("d_patient.psv"),
    'd_time': generate_iconv_step("d_time.psv"),
    'f_diagnosis': generate_iconv_step("f_diagnosis.psv"),
    'f_injection': generate_iconv_step("f_injection.psv"),
    'f_medication': generate_iconv_step("f_medication.psv")
}

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


split_dict = {
    'd_costar': generate_split_step("d_costar.psv.utf8"),
    'd_date': generate_split_step("d_date.psv.utf8"),
    'd_icd10': generate_split_step("d_icd10.psv.utf8"),
    'd_lab_directory': generate_split_step("d_lab_directory.psv.utf8"),
    'd_provider': generate_split_step("d_provider.psv.utf8"),
    'd_vaccine_cpt': generate_split_step("d_vaccine_cpt.psv.utf8"),
    'f_encounter': generate_split_step("f_encounter.psv.utf8"),
    'f_lab': generate_split_step("f_lab.psv.utf8"),
    'f_procedure': generate_split_step("f_procedure.psv.utf8"),
    'd_cpt': generate_split_step("d_cpt.psv.utf8"),
    'd_drug': generate_split_step("d_drug.psv.utf8"),
    'd_icd9': generate_split_step("d_icd9.psv.utf8"),
    'd_patient': generate_split_step("d_patient.psv.utf8"),
    'd_time': generate_split_step("d_time.psv.utf8"),
    'f_diagnosis': generate_split_step("f_diagnosis.psv.utf8"),
    'f_injection': generate_split_step("f_injection.psv.utf8"),
    'f_medication': generate_split_step("f_medication.psv.utf8")
}


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
            'pyspark_normalization_script_name' : '/home/hadoop/spark/providers/amazingcharts/emr/sparkNormalizeAmazingChartsEMR.py',
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
        split_dict.values() + [queue_up_for_matching]
    )
else:
    detect_move_normalize_dag.set_upstream(split_dict.values())

unzip_transaction.set_upstream(fetch_transaction)
unzip_transaction.set_downstream(unzip_dict.values() + [unzip_hv_tokens])

for k in split_dict.keys():
    iconv_dict[k].set_upstream(unzip_dict[k])
    split_dict[k].set_upstream(iconv_dict[k])

push_deid_file.set_upstream(unzip_hv_tokens)

# cleanup
clean_up_workspace.set_upstream(split_dict.values() + [push_deid_file])
