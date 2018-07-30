import common.HVDAG as HVDAG
from datetime import datetime, timedelta
import json
from airflow.operators import BashOperator, PythonOperator, SubDagOperator
from airflow.models import Variable
import subdags.clean_up_tmp_dir as clean_up_tmp_dir

import util.date_utils as date_utils
import util.sftp_utils as sftp_utils
import util.s3_utils as s3_utils

for m in [sftp_utils, HVDAG]:
    reload(m)

TMP_PATH_TEMPLATE = '/tmp/reference/gsdd/{}{}{}/'

if HVDAG.HVDAG.airflow_env == 'test':
    DESTINATION = 's3://salusv/testing/dewey/airflow/e2e/reference/gsdd/'
else:
    DESTINATION = 's3://salusv/reference/gsdd/'

FILES_OF_INTEREST = ['GSDD.db', 'GSDDMonograph.db']

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2018, 2, 5, 11),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2)
}

mdag = HVDAG.HVDAG(
    'reference_gsdd',
    default_args=default_args,
    schedule_interval='0 11 5 * *',
)


def sftp_fetch_files(ds, **kwargs):
    file_path = date_utils.insert_date_into_template(TMP_PATH_TEMPLATE, kwargs)
    sftp_config = json.loads(Variable.get('gsdd_cert'))
    path = sftp_config['path']
    del sftp_config['path']
    for filename in FILES_OF_INTEREST:
        sftp_utils.fetch_file(path + filename, file_path + filename, **sftp_cofig)


def copy_files_to_s3(ds, **kwargs):
    file_path = date_utils.insert_date_into_template(TMP_PATH_TEMPLATE, kwargs)
    for filename in FILES_OF_INTEREST:
        s3_utils.copy_file(file_path + filename, DESTINATION + filename)


create_tmp_files = BashOperator(
    task_id='create_tmp_dir',
    bash_command='mkdir -p ' + TMP_PATH_TEMPLATE.format('{{ ds_nodash }}', '', '') + ';'
    'touch ' + TMP_PATH_TEMPLATE.format('{{ ds_nodash }}', '', '') + 'GSDD.db;'
    'touch ' + TMP_PATH_TEMPLATE.format('{{ ds_nodash }}', '', '') + 'GSDDMonograph.db;',
    dag=mdag
)


fetch_files = PythonOperator(
    task_id='sftp_fetch_files',
    provide_context=True,
    python_callable=sftp_fetch_files,
    dag=mdag
)


move_files_to_s3 = PythonOperator(
    task_id='move_files_to_s3',
    provide_context=True,
    python_callable=copy_files_to_s3,
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

if HVDAG.HVDAG.airflow_env != 'test':
    update_s3_with_new_files = BashOperator(
        task_id='update_s3_from_db_files',
        bash_command='docker run --rm 581191604223.dkr.ecr.us-east-1.amazonaws.com/hvgsdd',
        dag=mdag
    )

    update_s3_with_new_files.set_upstream(move_files_to_s3)

fetch_files.set_upstream(create_tmp_files)
move_files_to_s3.set_upstream(fetch_files)
clean_up_workspace.set_upstream(move_files_to_s3)
