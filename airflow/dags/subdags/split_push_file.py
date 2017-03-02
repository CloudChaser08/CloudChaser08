from airflow import DAG
from airflow.operators import BashOperator, PythonOperator
from subprocess import check_call


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

    file_push_tasks = []
    for i in xrange(5):
        file_push_tasks.append(BashOperator(
            task_id='push_part_files_' + str(i),
            bash_command=(
               "for f in $(ls " + tmp_path_jinja + "parts/ "
               "| awk 'NR % {{ 5 }} == {{ params.task_idx }}' | awk -F' ' '{print $NF}');"
               "do aws s3 cp " + tmp_path_jinja +"parts/$f {{ params.s3_destination_prefix }}{{ ds|replace('-', '/') }}/; done"
            ),
            params={
                'task_idx': i, 
                's3_destination_prefix': dag_config['s3_destination_prefix']
            },
            dag=dag
        ))

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
