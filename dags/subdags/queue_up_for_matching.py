from airflow import DAG
from airflow.models import Variable
from airflow.operators import BashOperator, PythonOperator
from datetime import datetime, timedelta
from airflow.hooks.S3_hook import S3Hook
from subprocess import check_call
import logging

def do_queue_up_for_matching(ds, **kwargs):
    # We expect the files that were made available on the FTP server on $ds to have the date from the day before $ds in the name
    expected_file_name = kwargs['params']['expected_file_name_func'](kwargs)
    s3_prefix          = kwargs['params']['s3_prefix']

    environ = {
        'AWS_ACCESS_KEY_ID' : Variable.get('AWS_ACCESS_KEY_ID_MATCH_PUSHER'),
        'AWS_SECRET_ACCESS_KEY' : Variable.get('AWS_SECRET_ACCESS_KEY_MATCH_PUSHER')
    }

    check_call([
        '/home/airflow/airflow/dags/resources/push_file_to_s3.sh',
        's3://healthverity/' + s3_prefix + expected_file_name, '0',
        'prod-matching-ending', 'priority3'
    ], env=environ)

def queue_up_for_matching(parent_dag_name, child_dag_name, start_date, schedule_interval, dag_config):
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

    queue_up_for_matching = PythonOperator(
        task_id='queue_up_for_matching',
        provide_context=True,
        python_callable=do_queue_up_for_matching,
        params=dag_config,
        dag=dag
    )
    
    return dag
