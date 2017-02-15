from airflow import DAG
from airflow.models import Variable
from airflow.operators import BashOperator, PythonOperator
from datetime import datetime, timedelta
from subprocess import check_call
import os
import sys

if sys.modules.get('util.s3_utils'):
    del sys.modules['util.s3_utils']
import util.s3_utils as s3_utils

DECRYPTOR_JAR='HVDecryptor.jar'
DECRYPTION_KEY='hv_record_private.base64.reformat'


def do_fetch_decryption_files(ds, **kwargs):
    # jar
    s3_utils.fetch_file_from_s3(
        Variable.get('DECRYPTOR_JAR_REMOTE_LOCATION'),
        kwargs['tmp_path_template'].format('{{ ds_nodash }}')
    )

    # key
    s3_utils.fetch_file_from_s3(
        Variable.get('DECRYPTION_KEY_REMOTE_LOCATION'),
        kwargs['tmp_path_template'].format('{{ ds_nodash }}')
    )


def do_run_decryption(ds, **kwargs):
    tmp_dir = kwargs['tmp_path_template'].format(kwargs['ds_nodash'])
    encrypted_file_name = tmp_dir + kwargs['encrypted_file_name_func'](ds, kwargs)
    decrypted_file_name = tmp_dir + kwargs['decrypted_file_name_func'](ds, kwargs)
    decryptor_jar = tmp_dir + DECRYPTOR_JAR
    decryption_key = tmp_dir + DECRYPTION_KEY

    check_call([
        'java', '-jar', decryptor_jar, '-i', encrypted_file_name, '-o',
        decrypted_file_name, '-k', decryption_key
    ])


def do_decompress_file(ds, **kwargs):
    tmp_dir = kwargs['tmp_path_template'].format(kwargs['ds_nodash'])
    decrypted_file = tmp_dir + kwargs['decrypted_file_name_func'](ds, kwargs)

    check_call(['gzip', '-d', '-k', decrypted_file])


def do_clean_up(ds, **kwargs):
    tmp_dir = kwargs['tmp_path_template'].format(kwargs['ds_nodash'])
    encrypted_file_name = tmp_dir + kwargs['encrypted_file_name_func'](ds, kwargs)
    decryptor_jar = tmp_dir + DECRYPTOR_JAR
    decryption_key = tmp_dir + DECRYPTION_KEY

    for f in [encrypted_file_name, decryptor_jar, decryption_key]:
        check_call(['rm', f])


def decrypt_file(parent_dag_name, child_dag_name, start_date, schedule_interval, dag_config):
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

    fetch_decryption_files = PythonOperator(
        task_id='fetch_decryption_files',
        python_callable=do_fetch_decryption_files,
        provide_context=True,
        op_kwargs=dag_config,
        dag=dag
    )

    run_decryption = PythonOperator(
        task_id='run_decryption',
        python_callable=do_run_decryption,
        provide_context=True,
        op_kwargs=dag_config,
        dag=dag
    )

    decompress_file = PythonOperator(
        task_id='decompress_file',
        python_callable=do_decompress_file,
        provide_context=True,
        op_kwargs=dag_config,
        dag=dag
    )

    clean_up = PythonOperator(
        task_id='clean_up',
        python_callable=do_clean_up,
        provide_context=True,
        op_kwargs=dag_config,
        dag=dag
    )

    run_decryption.set_upstream(fetch_decryption_files)
    decompress_file.set_upstream(run_decryption)
    clean_up.set_upstream(decompress_file)

    return dag
