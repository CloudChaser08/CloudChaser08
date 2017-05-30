from airflow.models import Variable
from airflow.operators import PythonOperator
from subprocess import check_call

import common.HVDAG as HVDAG
import util.s3_utils as s3_utils
import util.decompression as decompression

for m in [s3_utils, decompression, HVDAG]:
    reload(m)

DECRYPTOR_JAR='HVDecryptor.jar'
DECRYPTION_KEY='hv_record_private.base64.reformat'


def do_fetch_decryption_files(ds, **kwargs):
    tmp_dir = kwargs['tmp_dir_func'](ds, kwargs)
    # jar
    s3_utils.fetch_file_from_s3(
        Variable.get('DECRYPTOR_JAR_REMOTE_LOCATION'),
        tmp_dir + DECRYPTOR_JAR
    )

    # key
    s3_utils.fetch_file_from_s3(
        Variable.get('DECRYPTION_KEY_REMOTE_LOCATION'),
        tmp_dir + DECRYPTION_KEY
    )


def do_run_decryption(ds, **kwargs):
    encrypted_decrypted_file_paths = kwargs['encrypted_decrypted_file_paths_func'](ds,kwargs)
    tmp_dir = kwargs['tmp_dir_func'](ds, kwargs)
    decryptor_jar = tmp_dir + DECRYPTOR_JAR
    decryption_key = tmp_dir + DECRYPTION_KEY

    for f in encrypted_decrypted_file_paths:
        check_call([
            'java', '-jar', decryptor_jar, '-i', f[0], '-o',
            f[1], '-k', decryption_key
        ])


def do_decompress_files(ds, **kwargs):
    encrypted_decrypted_file_paths = kwargs['encrypted_decrypted_file_paths_func'](ds,kwargs)

    for f in encrypted_decrypted_file_paths:
        decompression.decompress_gzip_file(f[1])

def do_clean_up(ds, **kwargs):
    tmp_dir = kwargs['tmp_dir_func'](ds, kwargs)
    decryptor_jar = tmp_dir + DECRYPTOR_JAR
    decryption_key = tmp_dir + DECRYPTION_KEY

    check_call(['rm', decryptor_jar])
    check_call(['rm', decryption_key])


def decrypt_files(parent_dag_name, child_dag_name, start_date, schedule_interval, dag_config):
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

    if not dag_config.get('not_compressed'):
        decompress_files = PythonOperator(
            task_id='decompress_file',
            python_callable=do_decompress_files,
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
    if not dag_config.get('not_compressed'):
        decompress_files.set_upstream(run_decryption)
        clean_up.set_upstream(decompress_files)
    else:
        clean_up.set_upstream(run_decryption)

    return dag
