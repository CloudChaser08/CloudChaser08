import common.HVDAG as HVDAG
from datetime import datetime, timedelta
import os
import subprocess

from airflow.operators import PythonOperator, SubDagOperator

import subdags.clean_up_tmp_dir as clean_up_tmp_dir
import subdags.run_pyspark_routine as run_pyspark_routine
import subdags.split_push_files as split_push_files

import util.date_utils as date_utils 
import util.decompression as decompression
import util.emr_utils as emr_utils

for m in [clean_up_tmp_dir, run_pyspark_routine, 
    split_push_files, date_utils, decompression, 
    emr_utils, HVDAG]:
    reload(m)

TMP_PATH_TEMPLATE = '/tmp/reference/nppes/{}{}{}/'
DAG_NAME = 'reference_nppes'

EMR_CLUSTER_NAME = 'reference_nppes'
NUM_NODES = 5
NODE_TYPE = 'm4.xlarge'
EBS_VOLUME_SIZE = '100'

if HVDAG.HVDAG.airflow_env == 'test':
    DESTINATION = 's3://salusv/testing/dewey/airflow/e2e/reference/nppes/'
    NPPES_TEXT_LOCATION = 's3://salusv/testing/'
else:
    DESTINATION = 's3://salusv/reference/nppes/'
    NPPES_TEXT_LOCATION = 's3://salusv/warehouse/text/nppes/{}/{}/{}/'

NPPES_URL = 'http://download.cms.gov/nppes/'
NPPES_FILENAME_TEMPLATE = 'NPPES_Data_Dissemination_{}_{}.zip'


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2018, 4, 20),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2)
}

mdag = HVDAG.HVDAG(
    'reference_nppes',
    default_args=default_args,
    schedule_interval='0 0 20 * *',
)

get_tmp_dir = date_utils.generate_insert_date_into_template_function(TMP_PATH_TEMPLATE)

get_tmp_unzipped_dir = date_utils.generate_insert_date_into_template_function(
    TMP_PATH_TEMPLATE + 'npi_unzipped/')


def get_nppes_filename(ds, kwargs):
    date = datetime.strptime(ds, '%Y-%m-%d')
    return NPPES_FILENAME_TEMPLATE.format(date.strftime("%B"), date.year)


def get_file_paths(ds, kwargs):
    return [get_tmp_dir(ds, kwargs) + get_nppes_filename(ds)]


def fetch_monthly_npi_file(ds, kwargs):
    subprocess.check_call([
        'curl', NPPES_URL + get_nppes_filename(ds,kwargs), '-o', get_tmp_dir(ds,kwargs) + get_nppes_filename(ds,kwargs)
    ])


def do_unzip_file(ds, kwargs):
    zip_file_path = get_tmp_dir(ds, kwargs) + get_nppes_filename(ds)
    decompression.decompress_zip_file(zip_file_path, get_tmp_unzipped_dir(ds, kwargs))
    os.remove(zip_file_path)

def do_run_pyspark_routine(ds, **kwargs):
    emr_utils.run_script(
        DAG_NAME, kwargs['pyspark_script_name'], [], None
    )

def do_create_cluster(ds, **kwargs):
    emr_utils.create_emr_cluster(
        DAG_NAME, NUM_NODES, NODE_TYPE, EBS_VOLUME_SIZE, 'reference_search_terms_update', connected_to_metastore=True)

    emr_utils.run_steps(DAG_NAME, [INSTALL_BOTO3_STEP])


def do_delete_cluster(ds, **kwargs):
    emr_utils.delete_emr_cluster(DAG_NAME)


fetch_NPI_file = PythonOperator(
    task_id='fetch_monthly_file',
    provide_context=True,
    python_callable=fetch_monthly_npi_file,
    dag=mdag
)

unzip_file = PythonOperator(
        task_id='unzip_file',
        provide_context=True,
        python_callable=do_unzip_file,
        dag=mdag
    )


split_push_to_s3 = SubDagOperator(
    subdag=split_push_files.split_push_files(
        DAG_NAME,
        'split_transaction_file',
        default_args['start_date'],
        mdag.schedule_interval,
        {
            'tmp_dir_func'             : get_tmp_unzipped_dir,
            'file_paths_to_split_func' : get_file_paths,
            'file_name_pattern_func'   : get_nppes_filename,
            's3_prefix_func'           :
                date_utils.generate_insert_date_into_template_function(
                    NPPES_TEXT_LOCATION
                ),
            'num_splits'               : 20
        }
    ),
    task_id='split_transaction_file',
    dag=mdag
)

run_pyspark_routine = PythonOperator(
    task_id='run_pyspark',
    provide_context=True,
    python_callable=do_run_pyspark_routine,
    dag=mdag
)

delete_cluster = PythonOperator(
    task_id='delete_cluster',
    provide_context=True,
    python_callable=do_delete_cluster,
    dag=mdag
)

# MSCK REPAIR TABLE <table_name>

clean_up_workspace = SubDagOperator(
    subdag=clean_up_tmp_dir.clean_up_tmp_dir(
        'reference_gsdd',
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

unzip_file.set_upstream(fetch_NPI_file)
split_push_to_s3.set_upstream(unzip_file)
run_pyspark_routine.set_upstream(split_push_to_s3)
# cleanup
clean_up_workspace.set_upstream(run_pyspark_routine)

