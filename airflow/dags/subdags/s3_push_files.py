from airflow.models import Variable
from airflow.operators import BashOperator, PythonOperator
from datetime import datetime, timedelta
import boto3
import logging
import os

import common.HVDAG as HVDAG

for m in [HVDAG]:
    reload(m)

def do_push_files(ds, **kwargs):
    file_paths     = kwargs['file_paths_func'](ds, kwargs)
    s3_prefix      = kwargs['s3_prefix_func'](ds, kwargs)

    if kwargs.get('aws_access_key_id'):
        s3 = boto3.resource('s3', aws_access_key_id=kwargs['aws_access_key_id'], aws_secret_access_key=kwargs['aws_secret_access_key'])
    else:
        s3 = boto3.resource('s3')
    
    for f in file_paths:
        fn = f.split('/')[-1]
        obj = s3.Object(kwargs['s3_bucket'], s3_prefix + fn)
        obj.upload_file(f, ExtraArgs={'ServerSideEncryption':'AES256'})

def s3_push_files(parent_dag_name, child_dag_name, start_date, schedule_interval, dag_config):
    default_args = {
        'owner': 'airflow',
        'depends_on_past': False,
        'retries': 0
    }

    dag = HVDAG.HVDAG(
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
