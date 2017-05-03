import os
from airflow import DAG
from airflow.models import Variable
from airflow.operators import PythonOperator
from subprocess import check_call


def do_queue_up_for_matching(ds, **kwargs):
    source_files = kwargs['source_files_func'](ds, kwargs)
    environ = {
        'AWS_ACCESS_KEY_ID' : Variable.get('AWS_ACCESS_KEY_ID_MATCH_PUSHER'),
        'AWS_SECRET_ACCESS_KEY' : Variable.get('AWS_SECRET_ACCESS_KEY_MATCH_PUSHER')
    }

    for f in source_files:
        check_call([
            os.getenv('AIRFLOW_HOME')
            + '/dags/resources/push_file_to_s3_batchless.sh',
            f, '0', 'prod-matching-engine', 'priority3'
        ], env=environ)


def queue_up_for_matching(
        parent_dag_name, child_dag_name, start_date,
        schedule_interval, dag_config
):
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
        op_kwargs=dag_config,
        dag=dag
    )

    return dag
