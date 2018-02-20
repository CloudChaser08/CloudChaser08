import common.HVDAG as HVDAG
from datetime import datetime, timedelta
import json
import pysftp

from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable

import util.sftp_utils as sftp_utils

for m in [sftp_utils, HVDAG]:
    reload(m)


if HVDAG.HVDAG.airflow_env == 'test':
    DESTINATION = 's3://salusv/testing/dewey/airflow/reference/gsdd/'
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
                              DESTINATION + filename,
                              sftp_config['host'],
                              sftp_config['user'],
                              sftp_config['password'],
                              cnopts
                              )

# sftp_utils.fetch_files(**sftp_config)


fetch_files = PythonOperator(
    task_id='sftp_fetch_files',
    python_callable=sftp_fetch_files,
    dag=mdag
)


if HVDAG.HVDAG.airflow_env != 'test':
    update_s3_with_new_files = BashOperator(
    task_id='update_s3_from_db_files',
    bash_command='docker run 581191604223.dkr.ecr.us-east-1.amazonaws.com/hvgsdd',
    retries=3,
    dag=mdag)
    
    update_s3_with_new_files.set_upstream(fetch_files)

