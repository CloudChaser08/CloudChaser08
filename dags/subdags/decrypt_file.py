from airflow import DAG
from airflow.models import Variable
from airflow.operators import BashOperator, PythonOperator
from datetime import datetime, timedelta
from subprocess import check_call
import os

DECRYPTOR_JAR='HVDecryptor.jar'
DECRYPTION_KEY='hv_record_private.base64.reformat'

def do_run_decryption(ds, **kwargs):
    tmp_dir = kwargs['tmp_path_template'].format(kwargs['ds_nodash'])
    encrypted_file_name = tmp_dir + kwargs['encrypted_file_name_func'](kwargs)
    decrypted_file_name = tmp_dir + kwargs['decrypted_file_name_func'](kwargs)
    decryptor_jar = tmp_dir + DECRYPTOR_JAR
    decryption_key = tmp_dir + DECRYPTION_KEY

    check_call([
        'java', '-jar', decryptor_jar, '-i', encrypted_file_name, '-o',
        decrypted_file_name, '-k', decryption_key
    ])

def do_clean_up(ds, **kwargs):
    tmp_dir = kwargs['tmp_path_template'].format(kwargs['ds_nodash'])
    encrypted_file_name = tmp_dir + kwargs['encrypted_file_name_func'](kwargs)
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

    env = dict(os.environ)
    env['AWS_ACCESS_KEY_ID'] = Variable.get('AWS_ACCESS_KEY_ID')
    env['AWS_SECRET_ACCESS_KEY'] = Variable.get('AWS_SECRET_ACCESS_KEY')
    fetch_decryptor_jar = BashOperator(
        task_id='fetch_decryptor_jar',
        bash_command='aws s3 cp {{ params.jar_src }} ' + dag_config['tmp_path_template'].format('{{ ds_nodash }}'),
        params={
            'jar_src': Variable.get('DECRYPTOR_JAR_REMOTE_LOCATION')
        },
        env=env,
        dag=dag
    )

    fetch_decryption_key = BashOperator(
        task_id='fetch_decryption_key',
        bash_command='aws s3 cp {{ params.key_src }} ' + dag_config['tmp_path_template'].format('{{ ds_nodash }}'),
        params={
            'key_src': Variable.get('DECRYPTION_KEY_REMOTE_LOCATION')
        },
        env=env,
        dag=dag
    )

    run_decryption = PythonOperator(
        task_id='run_decryption',
        python_callable=do_run_decryption,
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

    run_decryption.set_upstream([fetch_decryptor_jar, fetch_decryption_key])
    clean_up.set_upstream(run_decryption)

    return dag
