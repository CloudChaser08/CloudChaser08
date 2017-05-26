from airflow.models import Variable
from airflow.operators import BashOperator, PythonOperator, BranchPythonOperator, SlackAPIOperator, DummyOperator
from datetime import datetime, timedelta
from subprocess import check_call
from json import loads as json_loads
import logging
import os
import pysftp
import re

import common.HVDAG as HVDAG

for m in [HVDAG]:
    reload(m)

SLACK_CHANNEL='#airflow_alerts'

def do_is_valid_new_file(ds, **kwargs):
    # We expect the files that were made available on the FTP server on $ds to have the date from the day before $ds in the name
    expected_file_name  = kwargs['file_name_template'].format(kwargs['yesterday_ds_nodash'])

    sftp_config = json_loads(Variable.get('Emdeon SFTP Configuration'))[kwargs['datatype']]
    with pysftp.Connection(sftp_config['source_host'], username=sftp_config['user'], password=sftp_config['password']) as conn:
        all_files = conn.listdir_attr(sftp_config['source_path'])

    # We expect daily_num_files files to be delivered every day for this data type
    recent_files = sorted(all_files, key=lambda f: f.st_mtime)

    expected_file_regex = kwargs['file_name_template'].format('\d{8}')

    if len(filter(lambda f: len(re.findall(expected_file_regex, f.filename)) == 1, recent_files)) == 0:
        return kwargs['is_bad_name']
    
    if len(filter(lambda f: f.filename == expected_file_name, recent_files)) == 0:
        return kwargs['is_not_new']

    new_file = filter(lambda f: f.filename == expected_file_name, recent_files)[0]
    if new_file.st_size < kwargs['minimum_file_size']:
        return kwargs['is_not_valid']

    return kwargs['is_new_valid']
        
def do_fetch_file(ds, **kwargs):
    # We expect the files that were made available on the FTP server on $ds to have the date from the day before $ds in the name
    expected_file_name  = kwargs['file_name_template'].format(kwargs['yesterday_ds_nodash'])

    tmp_path = kwargs['tmp_path_template'].format(kwargs['ds_nodash'])

    sftp_config = json_loads(Variable.get('Emdeon SFTP Configuration'))[kwargs['datatype']]
    with pysftp.Connection(sftp_config['source_host'], username=sftp_config['user'], password=sftp_config['password']) as conn:
        conn.get("{}/{}".format(sftp_config['source_path'], expected_file_name), "{}/{}".format(tmp_path, expected_file_name))

def do_push_raw_to_s3(ds, **kwargs):
    # We expect the files that were made available on the FTP server on $ds to have the date from the day before $ds in the name
    expected_file_name  = kwargs['file_name_template'].format(kwargs['yesterday_ds_nodash'])

    tmp_path = kwargs['tmp_path_template'].format(kwargs['ds_nodash'])

    check_call(['aws', 's3', 'cp', '--sse', 'AES256', "{}{}".format(tmp_path, expected_file_name), "{}".format(kwargs['s3_raw_path'])])

def emdeon_validate_fetch_file(parent_dag_name, child_dag_name, start_date, schedule_interval, dag_config):
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

    is_valid_new_file = BranchPythonOperator(
        task_id='is_new_file',
        provide_context=True,
        python_callable=do_is_valid_new_file,
        op_kwargs={
            'file_name_template' : dag_config['file_name_template'],
            'minimum_file_size'  : dag_config['minimum_file_size'],
            'datatype'     : dag_config['datatype'],
            'is_new_valid' : 'create_tmp_dir',
            'is_not_valid' : 'alert_file_size_problem',
            'is_not_new'   : 'alert_no_new_file',
            'is_bad_name'  : 'alert_is_bad_name'
        },
        dag=dag
    )
    
    alert_is_bad_name = SlackAPIOperator(
        task_id='alert_is_bad_name',
        token=Variable.get('SlackToken'),
        method='chat.postMessage',
        retries=0,
        api_params={
            'channel'  : SLACK_CHANNEL,
            'text'     : 'No new {} matching expected patten found'.format(dag_config['file_description']),
            'username' : 'AirFlow',
            'icon_url' : 'https://airflow.incubator.apache.org/_images/pin_large.png'
        },
        dag=dag
    )
    
    alert_no_new_file = SlackAPIOperator(
        task_id='alert_no_new_file',
        token=Variable.get('SlackToken'),
        method='chat.postMessage',
        retries=0,
        api_params={
            'channel'  : SLACK_CHANNEL,
            'text'     : 'No new {} found'.format(dag_config['file_description']),
            'username' : 'AirFlow',
            'icon_url' : 'https://airflow.incubator.apache.org/_images/pin_large.png'
        },
        dag=dag
    )
    
    alert_file_size_problem = SlackAPIOperator(
        task_id='alert_file_size_problem',
        token=Variable.get('SlackToken'),
        method='chat.postMessage',
        retries=0,
        api_params={
            'channel'  : SLACK_CHANNEL,
            'text'     : '{} is of an unexpected size'.format(dag_config['file_description']),
            'username' : 'AirFlow',
            'icon_url' : 'https://airflow.incubator.apache.org/_images/pin_large.png'
        },
        dag=dag
    )

    force_failure = BashOperator(
        task_id='force_failure',
        bash_command='exit 1;',
        trigger_rule='one_success',
        dag=dag
    )
    
    create_tmp_dir = BashOperator(
        task_id='create_tmp_dir',
        bash_command='mkdir -p {};'.format(dag_config['tmp_path_template'].format('{{ ds_nodash }}')),
        dag=dag
    )
    
    fetch_file = PythonOperator(
        task_id='fetch_file',
        provide_context=True,
        python_callable=do_fetch_file,
        op_kwargs = {
            'file_name_template' : dag_config['file_name_template'],
            'tmp_path_template'  : dag_config['tmp_path_template'],
            'datatype' : dag_config['datatype']
        },
        dag=dag
    )
    
    push_raw_to_s3 = PythonOperator(
        task_id='push_raw_to_s3',
        provide_context=True,
        python_callable=do_push_raw_to_s3,
        op_kwargs = {
            'file_name_template' : dag_config['file_name_template'],
            'tmp_path_template'  : dag_config['tmp_path_template'],
            's3_raw_path' : dag_config['s3_raw_path']
        },
        dag=dag
    )

    alert_no_new_file.set_upstream(is_valid_new_file)
    alert_is_bad_name.set_upstream(is_valid_new_file)
    alert_file_size_problem.set_upstream(is_valid_new_file)
    alert_no_new_file.set_downstream(force_failure)
    alert_is_bad_name.set_downstream(force_failure)
    alert_file_size_problem.set_downstream(force_failure)
    create_tmp_dir.set_upstream(is_valid_new_file)
    fetch_file.set_upstream(create_tmp_dir)
    push_raw_to_s3.set_upstream(fetch_file)

    return dag
