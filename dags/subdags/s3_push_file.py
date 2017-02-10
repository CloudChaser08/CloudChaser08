from airflow import DAG
from airflow.models import Variable
from airflow.operators import BashOperator, PythonOperator
from datetime import datetime, timedelta
from airflow.hooks.S3_hook import S3Hook
import logging

def do_push_file(ds, **kwargs):
    file_name      = kwargs['file_name_func'](ds, kwargs)
    s3_prefix      = kwargs['s3_prefix']

    tmp_path = kwargs['tmp_path_template'].format(kwargs['ds_nodash'])
    if kwargs.get('s3_connection'):
        hook = S3Hook(s3_conn_id=kwargs['s3_connection'])
    else:
        hook = S3Hook()
    hook.load_file(tmp_path + file_name, s3_prefix + file_name, kwargs['s3_bucket'])

def s3_push_file(parent_dag_name, child_dag_name, start_date, schedule_interval, dag_config):
    default_args = {
        'owner': 'airflow',
        'depends_on_past': False,
        'retries': 0
    }

    dag = DAG(
        '{}.{}'.format(parent_dag_name, child_dag_name),
        schedule_interval='@daily',
        start_date=start_date,
        default_args=default_args
    )

    fetch_file = PythonOperator(
        task_id='push_file',
        provide_context=True,
        python_callable=do_push_file,
        op_kwargs=dag_config,
        dag=dag
    )

    return dag
