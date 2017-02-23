from airflow import DAG
from airflow.models import Variable
from airflow.operators import BashOperator, PythonOperator
from datetime import datetime, timedelta
import boto3
import logging

def do_push_file(ds, **kwargs):
    file_name      = kwargs['file_name_func'](ds, kwargs)
    s3_prefix      = kwargs['s3_prefix']

    tmp_path = kwargs['tmp_path_template'].format(kwargs['ds_nodash'])
    if kwargs.get('aws_access_key_id'):
        s3 = boto3.resource('s3', aws_access_key_id=kwargs['aws_access_key_id'], aws_secret_access_key=kwargs['aws_secret_access_key'])
    else:
        s3 = boto3.resource('s3')
    obj = s3.Object(kwargs['s3_bucket'], s3_prefix + file_name)
    obj.upload_file(tmp_path + file_name, ExtraArgs={'ServerSideEncryption':'AES256'})

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
