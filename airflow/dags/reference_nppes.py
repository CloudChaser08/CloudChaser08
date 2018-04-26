import common.HVDAG as HVDAG
from datetime import datetime, timedelta
import requests
import os
import subdags.split_push_files as split_push_files
from airflow.operators import PythonOperator, SubDagOperator
import subdags.clean_up_tmp_dir as clean_up_tmp_dir

import util.date_utils as date_utils
import util.decompression as decompression
import util.emr_utils as emr_utils
import util.sftp_utils as sftp_utils

for m in [sftp_utils, HVDAG]:
    reload(m)

TMP_PATH_TEMPLATE = '/tmp/reference/npess/{}{}{}/'
DAG_NAME = 'reference_nppes'

if HVDAG.HVDAG.airflow_env == 'test':
    DESTINATION = 's3://salusv/testing/dewey/airflow/e2e/reference/npess/'
    NPPES_TEXT_LOCATION = 's3://salusv/testing/'
else:
    DESTINATION = 's3://salusv/reference/npess/'
    NPPES_TEXT_LOCATION = 's3://salusv/warehouse/text/nppes/{}/{}/{}/'

NPPES_URL = 'http://download.cms.gov/nppes/'
NPPES_FILENAME_TEMPLATE = 'NPPES_Data_Dissemination_{}_{}.zip'


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2018, 2, 5, 11),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2)
}

mdag = HVDAG.HVDAG(
    'reference_npess',
    default_args=default_args,
    schedule_interval='0 0 20 * *',
)

get_tmp_dir = date_utils.generate_insert_date_into_template_function(TMP_PATH_TEMPLATE)

get_tmp_unzipped_dir = date_utils.generate_insert_date_into_template_function(
    TMP_PATH_TEMPLATE + 'npi_unzipped/')


def get_nppes_filename(ds):
    date = datetime.strptime(ds, '%Y-%m-%d')
    return NPPES_FILENAME_TEMPLATE.format(date.strftime("%B"), date.year)


def get_file_paths(ds, kwargs):
    return [get_tmp_dir(ds, kwargs) + get_nppes_filename(ds)]


def fetch_monthly_npi_file(ds, kwargs):
    file_url = NPPES_URL + get_nppes_filename(ds)
    r = requests.get(file_url, stream = True)
    with open(get_tmp_dir + NPPES_URL, 'wb') as f:
        f.write(r.content)


def do_unzip_file():
    def out(ds, **kwargs):
        tmp_dir = get_tmp_dir(ds, kwargs)
        file_name = get_nppes_filename(ds)
        decompression.decompress_zip_file(tmp_dir + file_name, get_tmp_unzipped_dir(ds, kwargs))
        os.remove(tmp_dir + file_name)

    return PythonOperator(
        task_id='unzip_file',
        provide_context=True,
        python_callable=out,
        dag=mdag
    )

def do_run_pyspark_routine(ds, **kwargs):
    emr_utils.run_script(
        DAG_NAME, kwargs['pyspark_script_name'], [], None
    )

fetch_NPI_file = PythonOperator(
    task_id='fetch_monthly_file',
    provide_context=True,
    python_callable=fetch_monthly_npi_file,
    dag=mdag
)

unzip_file = do_unzip_file()


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

