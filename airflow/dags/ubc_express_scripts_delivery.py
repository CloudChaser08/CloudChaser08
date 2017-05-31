from airflow import DAG
from airflow.models import Variable
from airflow.operators import *
from datetime import datetime, timedelta
from subprocess import check_output, check_call, STDOUT
import logging
import os
import re
import sys

import util.emr_utils as emr_utils
for m in [emr_utils]:
    reload(m)

DAG_NAME='ubc_express_scripts_delivery'

EMR_CLUSTER_NAME='delivery_cluster-ubc'
NUM_NODES=5
NODE_TYPE='m4.2xlarge'
EBS_VOLUME_SIZE='100'

def do_create_cluster(ds, **kwargs):
    create_emr_cluster(EMR_CLUSTER_NAME, NUM_NODES, NODE_TYPE, EBS_VOLUME_SIZE, connected_to_metastore)

def do_delete_cluster(ds, **kwargs):
    emr_utils.delete_emr_cluster(EMR_CLUSTER_NAME)

def do_run_pyspark_export_routine(ds, **kwargs):
    emr_utils.export(
        EMR_CLUSTER_NAME,
        kwargs['pyspark_script_name'],
        kwargs['pyspark_args_func'](ds, kwargs)
    )

def get_export_args(ds, kwargs):
    return ['--month', ds[:7]]

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2017, 5, 8),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'priority_weight': 5
}

mdag = DAG(
    dag_id=DAG_NAME,
    schedule_interval=None,
    schedule_interval='0 0 5 * *' if Variable.get('AIRFLOW_ENV', default_var='').find('prod') != -1 else None,
    default_args=default_args
)

create_cluster = PythonOperator(
    task_id='create_cluster',
    provide_context=True,
    python_callable=do_create_cluster,
    dag=mdag
)

run_pyspark_export_routine = PythonOperator(
    task_id='run_pyspark_export_routine',
    provide_context=True,
    python_callable=do_run_pyspark_export_routine,
    op_kwargs={
        'pyspark_script_name' : '/home/airflow/spark/delivery/ubc_0/sparkExtractUBC.py',
        'pyspark_args_func'   : get_export_args
    },
    dag=mdag
)

delete_cluster = PythonOperator(
    task_id='delete_cluster',
    provide_context=True,
    python_callable=do_delete_cluster,
    dag=mdag
)

run_pyspark_export_routine.set_upstream(create_cluster)
delete_cluster.set_upstream(run_pyspark_export_routine)

