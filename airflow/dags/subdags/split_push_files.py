from airflow import DAG
from airflow.operators import BashOperator, PythonOperator
from subprocess import check_call
from util.s3_utils import copy_file_async
import os
import logging

def do_create_parts_dir(ds, **kwargs):
    tmp_dir = kwargs['tmp_dir_func'](ds, kwargs) + 'parts/'
    check_call(['mkdir', '-p', tmp_dir])

def do_split_files(ds, **kwargs):
    tmp_dir = kwargs['tmp_dir_func'](ds, kwargs)
    file_paths_to_split = kwargs['file_paths_to_split_func'](ds, kwargs)

    logging.info(file_paths_to_split)
    for fp in file_paths_to_split:
        f = fp.split('/')[-1]
        check_call([
            'split', '-n', 'l/' + str(kwargs['num_splits']), fp,
            tmp_dir + 'parts/' + f + '.'
        ])

def do_bzip_part_files(ds, **kwargs):
    tmp_dir = kwargs['tmp_dir_func'](ds, kwargs)
    files = os.listdir(tmp_dir + 'parts/')
    for f in files:
        check_call(['lbzip2', '-z', tmp_dir + 'parts/' + f])

def do_push_part_files(ds, **kwargs):
    tmp_dir   = kwargs['tmp_dir_func'](ds, kwargs) + 'parts/'
    s3_prefix = kwargs['s3_prefix_func'](ds, kwargs)
    files = os.listdir(tmp_dir)
    for i in xrange(0, len(files), 5):
        processes = []
        for j in xrange(i, min(i + 5, len(files))):
            processes.append(copy_file_async(tmp_dir + files[j], s3_prefix))
        for p in processes:
            p.wait()

def do_clean_up(ds, **kwargs):
    tmp_dir = kwargs['tmp_dir_func'](ds, kwargs)
    file_paths_to_split = kwargs['file_paths_to_split_func'](ds, kwargs)

    for fp in file_paths_to_split:
        check_call(['rm', fp])
    check_call(['rm', '-r', tmp_dir + 'parts'])
            
def split_push_files(parent_dag_name, child_dag_name, start_date, schedule_interval, dag_config):
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

    create_parts_dir = PythonOperator(
        task_id='create_parts_dir',
        provide_context=True,
        python_callable=do_create_parts_dir,
        op_kwargs=dag_config,
        dag=dag
    )

    split_files = PythonOperator(
        task_id='split_files',
        provide_context=True,
        python_callable=do_split_files,
        op_kwargs=dag_config,
        dag=dag
    )

    bzip_part_files = PythonOperator(
        task_id='bzip_part_files',
        provide_context=True,
        python_callable=do_bzip_part_files,
        op_kwargs=dag_config,
        dag=dag
    )

    push_part_files = PythonOperator(
        task_id='push_part_files',
        provide_context=True,
        python_callable=do_push_part_files,
        op_kwargs=dag_config,
        dag=dag
    )


    clean_up = PythonOperator(
        task_id='clean_up',
        provide_context=True,
        python_callable=do_clean_up,
        op_kwargs=dag_config,
        dag=dag
    )

    split_files.set_upstream(create_parts_dir)
    bzip_part_files.set_upstream(split_files)
    push_part_files.set_upstream(bzip_part_files)
    clean_up.set_upstream(push_part_files)

    return dag
