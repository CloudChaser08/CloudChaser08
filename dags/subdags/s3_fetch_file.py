from airflow import DAG
from airflow.operators import BashOperator, PythonOperator
import sys

if sys.modules.get('modules.s3_utils'):
    del sys.modules['modules.s3_utils']
import modules.s3_utils as s3_utils

def do_fetch_file(ds, **kwargs):
    # We expect the files that were made available on the FTP server on $ds to have the date from the day before $ds in the name
    expected_file_name = kwargs['expected_file_name_func'](ds, kwargs)
    new_file_name      = kwargs['new_file_name_func'](ds, kwargs)
    s3_prefix          = kwargs['s3_prefix']

    tmp_path = kwargs['tmp_path_template'].format(kwargs['ds_nodash'])

    s3_utils.fetch_file_from_s3(
        's3://healthverity/' + s3_prefix + expected_file_name,
        tmp_path + new_file_name
    )


def s3_fetch_file(parent_dag_name, child_dag_name, start_date, schedule_interval, dag_config):
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

    create_tmp_dir = BashOperator(
        task_id='create_tmp_dir',
        bash_command='mkdir -p {};'.format(dag_config['tmp_path_template'].format('{{ ds_nodash }}')),
        dag=dag
    )
    
    dag_config['new_file_name_func'] = dag_config.get('new_file_name_func', dag_config['expected_file_name_func'])
    fetch_file = PythonOperator(
        task_id='fetch_file',
        provide_context=True,
        python_callable=do_fetch_file,
        op_kwargs=dag_config,
        dag=dag
    )

    fetch_file.set_upstream(create_tmp_dir)

    return dag
