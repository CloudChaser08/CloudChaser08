import os
from airflow import DAG
from airflow.operators import BashOperator, PythonOperator
from subprocess import check_call

import util.s3_utils as s3_utils
reload(s3_utils)


def do_split_file(ds, **kwargs):
    tmp_dir = kwargs['params']['tmp_path_template'].format(kwargs['ds_nodash'])
    expected_file_name = kwargs['params']['source_file_name_func'](ds, kwargs)
    source_file = tmp_dir + expected_file_name

    check_call([
        'split', '-n', 'l/' + str(kwargs['params']['num_splits']), source_file,
        tmp_dir + 'parts/' + expected_file_name + '.'
    ])


def do_clean_up(ds, **kwargs):
    tmp_dir = kwargs['params']['tmp_path_template'].format(kwargs['ds_nodash'])
    source_file = tmp_dir + kwargs['params']['source_file_name_func'](ds, kwargs)
    check_call(['rm', source_file])
    check_call(['rm', '-r', tmp_dir + 'parts'])


def split_push_file(parent_dag_name, child_dag_name, start_date, schedule_interval, dag_config):
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

    tmp_path_jinja = dag_config['tmp_path_template'].format('{{ ds_nodash }}')
    create_parts_dir_cmd = 'mkdir -p ' + tmp_path_jinja + 'parts'
    create_parts_dir = BashOperator(
        task_id='create_parts_dir',
        bash_command=create_parts_dir_cmd,
        dag=dag
    )

    split_file = PythonOperator(
        task_id='split_file',
        provide_context=True,
        python_callable=do_split_file,
        params=dag_config,
        dag=dag
    )

    bzip_part_files = BashOperator(
        task_id='bzip_part_files',
        bash_command='lbzip2 -z ' + tmp_path_jinja + 'parts/*',
        dag=dag
    )


    # push files up to s3 in 4 separate tasks, each given 5 files each
    # the assumption here is that there will be 20 files
    file_push_tasks = []

    def push_task_group(i):
        def execute(ds, **kwargs):
            split_files = os.listdir(
                    dag_config['tmp_path_template'].format(kwargs['ds_nodash'])
                    + 'parts/'
            )

            # iterate over chunk number i of split_files
            for split_file in [split_files[j:min(j+5, len(split_files))]
                               for j in xrange(0, len(split_files), 5)][i]:
                s3_utils.copy_file(
                    dag_config['tmp_path_template'].format(kwargs['ds_nodash'])
                    + 'parts/' + split_file,
                    dag_config['s3_dest_path_func'](ds, kwargs)
                )
        return PythonOperator(
            task_id='push_part_files_' + str(i),
            provide_context=True,
            python_callable=execute,
            params=dag_config,
            dag=dag
        )

    for i in xrange(4):
        file_push_tasks.append(push_task_group(i))

    clean_up = PythonOperator(
        task_id='clean_up',
        provide_context=True,
        python_callable=do_clean_up,
        params=dag_config,
        dag=dag
    )

    split_file.set_upstream(create_parts_dir)
    bzip_part_files.set_upstream(split_file)
    bzip_part_files.set_downstream(file_push_tasks)
    clean_up.set_upstream(file_push_tasks)

    return dag
