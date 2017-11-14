from airflow.operators import BashOperator, PythonOperator
import re

import common.HVDAG as HVDAG
import util.s3_utils as s3_utils
import util.date_utils as date_utils

for m in [s3_utils, HVDAG, date_utils]:
    reload(m)

def do_fetch_file(ds, **kwargs):
    # We expect the files that were made available on the FTP server on $ds to have the date from the day before $ds in the name
    expected_file_name = kwargs['expected_file_name_func'](ds, kwargs)
    new_file_name      = kwargs['new_file_name_func'](ds, kwargs)
    s3_prefix          = kwargs['s3_prefix']

    if kwargs['tmp_path_template'].count('{}') >= 3:
        tmp_path = date_utils.insert_date_into_template(kwargs['tmp_path_template'],kwargs)
    else:
        tmp_path = kwargs['tmp_path_template'].format(kwargs['ds_nodash'])

    if 'regex_name_match' in kwargs and kwargs['regex_name_match']:
        s3_keys = s3_utils.list_s3_bucket_files(
            's3://' + kwargs['s3_bucket'] + '/' + s3_prefix,
            kwargs.get('s3_connection', s3_utils.DEFAULT_CONNECTION_ID)
        )

        expected_file_name = \
            filter(lambda k: re.search(expected_file_name, k.split('/')[-1]), s3_keys)[0]

    # When a new file name is not specified, use the expected file name
    # This is useful when the expected file name is determined using a regular expression
    # since the exact file name is unknown
    if new_file_name is None:
        new_file_name = expected_file_name

    s3_utils.fetch_file_from_s3(
        's3://' + kwargs['s3_bucket'] + '/' + s3_prefix + expected_file_name,
        tmp_path + new_file_name,
        kwargs.get('s3_connection', s3_utils.DEFAULT_CONNECTION_ID)
    )


def s3_fetch_file(parent_dag_name, child_dag_name, start_date, schedule_interval, dag_config):
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

    create_tmp_dir = BashOperator(
        task_id='create_tmp_dir',
        bash_command='mkdir -p {};'.format(dag_config['tmp_path_template'].format('{{ ds_nodash }}','','')),
        dag=dag
    )

    dag_config['new_file_name_func'] = dag_config.get('new_file_name_func', lambda ds, k: None)

    fetch_file = PythonOperator(
        task_id='fetch_file',
        provide_context=True,
        python_callable=do_fetch_file,
        op_kwargs=dag_config,
        dag=dag
    )

    fetch_file.set_upstream(create_tmp_dir)

    return dag
