import common.HVDAG as HVDAG
from datetime import datetime, timedelta

from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable

import util.sftp_utils as sftp_utils

for m in [sftp_utils, HVDAG]:
    reload(m)


HOST = 'sftp.gsdd.net'
FILE_DIR = 'Data_Files/GSDD5/'
DESTINATION = 's3://salusv/testing/reference/gsdd/'
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


def sftp_fetch_files(ds, **kwargs):
    for filename in FILES_OF_INTEREST:
        sftp_utils.fetch_file(FILE_DIR + filename, DESTINATION + filename, HOST,
                              Variable.get('gsdd_user'),
                              Variable.get("gsdd_password")
                              )


fetch_files = PythonOperator(
    task_id='sftp_fetch_files',
    python_callable=sftp_fetch_files,
    dag=mdag
)


update_s3_with_new_files = BashOperator(
    task_id='update_s3_from_db_files',
    bash_command='docker run 581191604223.dkr.ecr.us-east-1.amazonaws.com/hvgsdd',
    retries=3,
    dag=mdag)

update_s3_with_new_files.set_upstream(fetch_files)
