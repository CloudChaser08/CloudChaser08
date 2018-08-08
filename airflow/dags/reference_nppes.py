import common.HVDAG as HVDAG
from datetime import datetime, timedelta
import os
import urllib

from airflow.operators import PythonOperator, SubDagOperator, BashOperator

import subdags.clean_up_tmp_dir as clean_up_tmp_dir
import subdags.run_pyspark_routine as run_pyspark_routine
import subdags.s3_push_files as s3_push_files
import subdags.update_analytics_db as update_analytics_db

import util.date_utils as date_utils
import util.decompression as decompression
import util.emr_utils as emr_utils

for m in [clean_up_tmp_dir, run_pyspark_routine,
          s3_push_files, date_utils, decompression,
          emr_utils, HVDAG]:
    reload(m)

TMP_PATH_TEMPLATE = '/tmp/reference/nppes/{}{}{}/'
DAG_NAME = 'reference_nppes'

NPPES_URL = 'http://download.cms.gov/nppes/'
NPPES_FILENAME_TEMPLATE = 'NPPES_Data_Dissemination_{}_{}.zip'
NPPES_CSV_TEMPLATE = "npidata_pfile_date_{}{}{}"

EMR_CLUSTER_NAME_TEMPLATE = 'ref_nppes_{}'
NUM_NODES = 5
NODE_TYPE = 'm4.xlarge'
EBS_VOLUME_SIZE = '100'

if HVDAG.HVDAG.airflow_env == 'test':
    NPPES_TEXT_LOCATION = 's3://salusv/testing/dewey/airflow/e2e/reference/nppes/{}-{}-{}/text/'
    S3_PARQUET_LOCATION = 's3://salusv/testing/dewey/airflow/e2e/reference/nppes/{}-{}-{}/parquet/'
else:
    NPPES_TEXT_LOCATION = 's3://salusv/reference/nppes/{}-{}-{}/text/'
    S3_PARQUET_LOCATION = 's3://salusv/reference/nppes/{}-{}-{}/parquet/'

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2018, 6, 21),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2)
}

mdag = HVDAG.HVDAG(
    'reference_nppes',
    default_args=default_args,
    schedule_interval='0 0 21 * *',
)

get_tmp_dir = date_utils.generate_insert_date_into_template_function(TMP_PATH_TEMPLATE)

get_tmp_unzipped_dir = date_utils.generate_insert_date_into_template_function(
    TMP_PATH_TEMPLATE + 'npi_unzipped/')


def get_nppes_zipped_filename(ds, kwargs):
    month_name = kwargs['execution_date'].strftime("%B")
    return NPPES_FILENAME_TEMPLATE.format(month_name, kwargs['execution_date'].year)


def get_zip_file_path(ds, **kwargs):
    return [get_tmp_dir(ds, kwargs) + get_nppes_zipped_filename(ds)]


def get_csv_file_path(ds, kwargs):
    for f in os.listdir(get_tmp_unzipped_dir(ds, kwargs)):
        if f.startswith('npidata_pfile') and 'FileHeader' not in f:
            file_name = f
    return [get_tmp_unzipped_dir(ds, kwargs) + file_name]


def fetch_monthly_npi_file(ds, **kwargs):
    urllib.urlretrieve(
        NPPES_URL + get_nppes_zipped_filename(ds, kwargs),
        get_tmp_dir(ds, kwargs) + get_nppes_zipped_filename(ds, kwargs)
    )


def do_unzip_file(ds, **kwargs):
    zip_file_path = get_tmp_dir(ds, kwargs) + get_nppes_zipped_filename(ds, kwargs)
    decompression.decompress_zip_file(zip_file_path, get_tmp_unzipped_dir(ds, kwargs))
    os.remove(zip_file_path)


def norm_args(ds, k):
    base = ['--nppes_csv_path', date_utils.insert_date_into_template(NPPES_TEXT_LOCATION, k),
            '--s3_parquet_loc', date_utils.insert_date_into_template(S3_PARQUET_LOCATION, k),
            '--partitions', '20']
    if HVDAG.HVDAG.airflow_env == 'test':
        base += ['--airflow_test']

    return base


create_tmp_dir = BashOperator(
    task_id='create_tmp_directory',
    bash_command='mkdir -p ' + TMP_PATH_TEMPLATE.format('{{ ds_nodash }}', '', '') + ';',
    dag=mdag
)

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

push_file_to_s3 = SubDagOperator(
    subdag=s3_push_files.s3_push_files(
        DAG_NAME,
        'push_text_file_to_s3',
        default_args['start_date'],
        mdag.schedule_interval,
        {
            'file_paths_func'       : get_csv_file_path,
            's3_prefix_func'        :
                lambda ds, kwargs: '/'.join(
                    date_utils.insert_date_into_template(
                        NPPES_TEXT_LOCATION,
                        kwargs
                    ).split('/')[3:]
                ),
            's3_bucket'             : NPPES_TEXT_LOCATION.split('/')[2],
        }
    ),
    task_id='push_text_file_to_s3',
    dag=mdag
)

run_pyspark_routine = SubDagOperator(
    subdag=run_pyspark_routine.run_pyspark_routine(
        DAG_NAME,
        'run_nppes_script',
        default_args['start_date'],
        mdag.schedule_interval,
        {
            'EMR_CLUSTER_NAME_FUNC': date_utils.generate_insert_date_into_template_function(
                EMR_CLUSTER_NAME_TEMPLATE
            ),
            'PYSPARK_SCRIPT_NAME': '/home/hadoop/spark/reference/nppes/sparkNPPES.py',
            'PYSPARK_ARGS_FUNC': norm_args,
            'NUM_NODES': NUM_NODES,
            'NODE_TYPE': NODE_TYPE,
            'EBS_VOLUME_SIZE': EBS_VOLUME_SIZE,
            'PURPOSE': 'reference_data_update',
            'CONNECT_TO_METASTORE': False,
        }
    ),
    task_id='run_nppes_script',
    dag=mdag
)

clean_up_workspace = SubDagOperator(
    subdag=clean_up_tmp_dir.clean_up_tmp_dir(
        'reference_nppes',
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

update_analytics_db = SubDagOperator(
    subdag=update_analytics_db.update_analytics_db(
        DAG_NAME,
        'update_analytics_db',
        default_args['start_date'],
        mdag.schedule_interval,
        {
            'sql_command_func' : lambda ds, k:
            """ ALTER TABLE ref_nppes set location '{}'""".format(date_utils.insert_date_into_template(S3_PARQUET_LOCATION, k))
        }
    ),
    task_id='update_analytics_db',
    dag=mdag
)

if HVDAG.HVDAG.airflow_env == 'test':
    for t in ['update_analytics_db']:
        del mdag.task_dict[t]
        globals()[t] = DummyOperator(
            task_id = t,
            dag = mdag
        )

fetch_NPI_file.set_upstream(create_tmp_dir)
unzip_file.set_upstream(fetch_NPI_file)
push_file_to_s3.set_upstream(unzip_file)
run_pyspark_routine.set_upstream(push_file_to_s3)
update_analytics_db.set_upstream(run_pyspark_routine)
clean_up_workspace.set_upstream(run_pyspark_routine)