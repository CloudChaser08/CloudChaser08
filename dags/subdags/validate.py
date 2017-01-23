from airflow import DAG
from airflow.models import Variable
from airflow.operators import BashOperator, BranchPythonOperator, \
    SlackAPIOperator
from datetime import datetime
import sys
import os

sys.path.append(os.path.abspath(os.path.join('..', 'util')))

if sys.modules.get('util.aws_utils'):
    del sys.modules['util.aws_utils']
import util.aws_utils as aws_utils

SLACK_CHANNEL = '#dev'


def do_is_valid_new_s3_file(ds, **kwargs):
    expected_file_name = kwargs['file_name_template'].format(
        kwargs['kwargs_fn'](kwargs)
    )
    minimum_file_size = kwargs['minimum_file_size']

    if not aws_utils.s3_key_exists(expected_file_name):
        return kwargs['is_not_new']

    if aws_utils.get_file_size(expected_file_name) < minimum_file_size:
        return kwargs['is_not_valid']

    return kwargs['success']


def validate_file(
        parent_dag_name, file_description, file_name_template,
        kwargs_fn, minimum_file_size
):
    default_args = {
        'owner': 'airflow',
        'depends_on_past': False,
        'retries': 0
    }

    dag = DAG(
        '{}.validate_{}'.format(parent_dag_name, file_description),
        schedule_interval="@daily",
        start_date=datetime(9999, 12, 31, 0),
        default_args=default_args
    )

    is_valid_new_s3_file = BranchPythonOperator(
        task_id='validate_new_file',
        provide_context=True,
        python_callable=do_is_valid_new_s3_file,
        op_kwargs={
            'file_name_template': file_name_template,
            'kwargs_fn': kwargs_fn,
            'minimum_file_size': minimum_file_size,
            'is_not_valid': 'alert_file_size_problem',
            'is_not_new': 'alert_no_new_file',
            'success': 'success'
        },
        dag=dag
    )

    alert_no_new_file = SlackAPIOperator(
        task_id='alert_no_new_file',
        token=Variable.get('Slack Token'),
        method='chat.postMessage',
        retries=0,
        api_params={
            'channel': SLACK_CHANNEL,
            'text': '{}: No new {} found'.format(
                parent_dag_name, file_description
            ),
            'username': 'AirFlow',
            'icon_url': 'https://airflow.incubator.apache.org/_images/pin_large.png'
        },
        dag=dag
    )

    alert_file_size_problem = SlackAPIOperator(
        task_id='alert_file_size_problem',
        token=Variable.get('Slack Token'),
        method='chat.postMessage',
        retries=0,
        api_params={
            'channel': SLACK_CHANNEL,
            'text': '{}: {} is of an unexpected size'.format(
                parent_dag_name, file_description
            ),
            'username': 'AirFlow',
            'icon_url': 'https://airflow.incubator.apache.org/_images/pin_large.png'
        },
        dag=dag
    )

    force_failure = BashOperator(
        task_id='force_failure',
        bash_command='exit 1;',
        trigger_rule='one_success',
        dag=dag
    )

    success = BashOperator(
        task_id='success',
        bash_command='echo "Validation complete."',
        dag=dag
    )

    alert_no_new_file.set_upstream(is_valid_new_s3_file)
    alert_file_size_problem.set_upstream(is_valid_new_s3_file)
    alert_no_new_file.set_downstream(force_failure)
    alert_file_size_problem.set_downstream(force_failure)

    return dag
