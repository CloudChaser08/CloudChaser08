import common.HVDAG as HVDAG
from datetime import datetime, timedelta
from subprocess import check_call
import json
import pysftp

from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable

import util.sftp_utils as sftp_utils
import util.s3_utils as s3_utils

for m in [sftp_utils, HVDAG]:
    reload(m)

TMP_PATH = '/tmp/reference/gsdd/'

if HVDAG.HVDAG.airflow_env == 'test':
    DESTINATION = 's3://salusv/testing/dewey/airflow/e2e/reference/gsdd/'
else:
    DESTINATION = 's3://salusv/reference/gsdd/'

FILES_OF_INTEREST = ['GSDD.db', 'GSDDMonograph.db']

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2018, 3, 1),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2)
}


mdag = HVDAG.HVDAG(
    'reference_gsdd',
    default_args=default_args,
    schedule_interval='0 0 1 * *',
)


def sftp_fetch_files():
    sftp_config = json.loads(Variable.get('gsdd_cert'))
    for filename in FILES_OF_INTEREST:
        cnopts = pysftp.CnOpts()
        cnopts.hostkeys = None
        sftp_utils.fetch_file(sftp_config['path'] + filename,
                              TMP_PATH + filename,
                              sftp_config['host'],
                              sftp_config['user'],
                              sftp_config['password'],
                              cnopts
                              )


def copy_files_to_s3():
    for filename in FILES_OF_INTEREST:
        s3_utils.copy_file(TMP_PATH + filename, DESTINATION + filename)


def clean_tmp_dir():
    check_call([
        'rm', TMP_PATH + '*'
    ])


fetch_files = PythonOperator(
    task_id='sftp_fetch_files',
    python_callable=sftp_fetch_files,
    dag=mdag
)


move_files_to_s3 = PythonOperator(
    task_id='move_files_to_s3',
    python_callable=copy_files_to_s3,
    dag=mdag
)

clean_up_tmp_dir = PythonOperator(
    task_id='clean_up_tmp_dir',
    python_callable=clean_tmp_dir,
    trigger_rule='all_done',
    dag=mdag
)

if HVDAG.HVDAG.airflow_env != 'test':
    update_s3_with_new_files = BashOperator(
        task_id='update_s3_from_db_files',
        bash_command='docker run 581191604223.dkr.ecr.us-east-1.amazonaws.com/hvgsdd',
        retries=3,
        dag=mdag
    )

    update_s3_with_new_files.set_upstream(fetch_files)
    move_files_to_s3.set_upstream(update_s3_with_new_files)

move_files_to_s3.set_upstream(fetch_files)
clean_up_tmp_dir.set_upstream(move_files_to_s3)
