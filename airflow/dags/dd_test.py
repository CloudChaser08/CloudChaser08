#ye The DAG object; we'll need this to instantiate a DAG
import common.HVDAG as HVDAG

# Operators; we need this to operate!
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

from airflow.models import Variable

import re
import os
import sys
from json import loads
import struct
from datetime import timedelta, datetime

import util.hv_datadog

for m in [util.hv_datadog, HVDAG]:
    reload(m)
from util.hv_datadog import hv_datadog, start_dag_op, end_dag_op

TMP_PATH='/tmp/dd_test_'

if Variable.get('AIRFLOW_ENV', default_var='').find('prod') != -1:
    SCHEMA='default'
    AIRFLOW_ENV='prod'
else:
    SCHEMA='dev'
    AIRFLOW_ENV='dev'

dd = hv_datadog(env=AIRFLOW_ENV, keys=loads(Variable.get('DATADOG_KEYS')))

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'on_success_callback': dd.dd_eventer,
    'on_retry_callback':   dd.dd_eventer,
    'on_failure_callback': dd.dd_eventer
}

dag = HVDAG.HVDAG(
    'dd_test',
    default_args=default_args,
    start_date=datetime(2017, 2, 6),
    schedule_interval='@monthly' if Variable.get('AIRFLOW_ENV', default_var='').find('prod') != -1 else None,
)

start_run = start_dag_op(dag, dd)
end_run   = end_dag_op(dag, dd)


create_tmp_dir = BashOperator(
    task_id='create_tmp_dir',
    params={ "TMP_PATH": TMP_PATH},
    bash_command='mkdir -p {{ params.TMP_PATH }}{{ tomorrow_ds }}',
    retries=3,
    dag=dag)

cleanup_temp = BashOperator(
    task_id='cleanup_temp',
    params={ "TMP_PATH": TMP_PATH},
    bash_command='rm -rf {{ params.TMP_PATH }}{{ tomorrow_ds }}',
    dag=dag
)

start_run.set_downstream(create_tmp_dir)
cleanup_temp.set_upstream(create_tmp_dir)
end_run.set_upstream(cleanup_temp)

