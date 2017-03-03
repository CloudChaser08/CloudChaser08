from airflow import DAG
from airflow.models import Variable
from airflow.operators import BashOperator, PythonOperator
from datetime import datetime, timedelta
import boto3
import logging
import os

def do_push_files(ds, **kwargs):
    file_name      = kwargs['file_name_func'](ds, kwargs) if kwargs.get('file_name_func') else None
    s3_prefix      = kwargs['s3_prefix_func'](ds, kwargs)

    tmp_path = kwargs['tmp_path_template'].format(kwargs['ds_nodash'])
    if kwargs.get('aws_access_key_id'):
        logging.info('Configuring AWS keys')
        s3 = boto3.resource('s3', aws_access_key_id=kwargs['aws_access_key_id'], aws_secret_access_key=kwargs['aws_secret_access_key'])
    else:
        s3 = boto3.resource('s3')

    files = [file_name]
    if kwargs.get('all_files_in_dir'):
        files = os.listdir(tmp_path)
    
    for f in files:
        obj = s3.Object(kwargs['s3_bucket'], s3_prefix + f)
        obj.upload_file(tmp_path + f, ExtraArgs={'ServerSideEncryption':'AES256'})

def s3_push_files(parent_dag_name, child_dag_name, start_date, schedule_interval, dag_config):
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
        task_id='push_files',
        provide_context=True,
        python_callable=do_push_files,
        op_kwargs=dag_config,
        dag=dag
    )

    return dag
