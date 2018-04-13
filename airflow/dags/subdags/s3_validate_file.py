from airflow.models import Variable
from airflow.settings import Session
from airflow.exceptions import AirflowException
from airflow.operators import BashOperator, BranchPythonOperator, PythonOperator
from datetime import timedelta
import re
import logging
from slackclient import SlackClient

import util.s3_utils as s3_utils
import config as config
import common.HVDAG as HVDAG

for m in [s3_utils, config, HVDAG]:
    reload(m)

def do_is_valid_new_file(ds, **kwargs):
    # We expect the files that were made available on HealthVerity's S3
    s3_prefix          = kwargs['s3_prefix_func'](ds, kwargs) if 's3_prefix_func' in kwargs \
                         else kwargs['s3_prefix']
    file_name_pattern  = kwargs['file_name_pattern_func'](ds, kwargs)
    expected_file_name = kwargs['expected_file_name_func'](ds, kwargs)
    minimum_file_size  = kwargs['minimum_file_size']
    s3_connection_id   = kwargs.get('s3_connection', s3_utils.DEFAULT_CONNECTION_ID)

    s3_prefix = s3_prefix[:-1] if s3_prefix[-1] == '/' else s3_prefix

    s3_keys = s3_utils.list_s3_bucket_files(
        's3://' + kwargs['s3_bucket'] + '/' + s3_prefix + '/', s3_connection_id
    )

    logging.info("Looking for file: {}".format(expected_file_name))

    if len(filter(lambda k: len(re.findall(file_name_pattern, k.split('/')[-1])) == 1, s3_keys)) == 0:
        if 'quiet_retries' in kwargs and kwargs['quiet_retries'] > kwargs['ti'].try_number:
            raise ValueError('No files of the expected pattern')
        return kwargs['is_bad_name']

    # Check if there are files matching the name exactly, or, if regex match
    # was specified, there are files matching the regex pattern
    if not (len(filter(lambda k: k.split('/')[-1] == expected_file_name, s3_keys)) > 0 \
            or ('regex_name_match' in kwargs and kwargs['regex_name_match'] and \
            len(filter(lambda k: re.search(expected_file_name, k.split('/')[-1]), s3_keys)) > 0)):

        if 'quiet_retries' in kwargs and kwargs['quiet_retries'] > kwargs['ti'].try_number:
            raise ValueError('No new file found')
        return kwargs['is_not_new']

    # Grab the first key that's either an exact match or a partial match
    s3_key = (filter(lambda k: k.split('/')[-1] == expected_file_name, s3_keys) +
            filter(lambda k: re.search(expected_file_name, k.split('/')[-1]), s3_keys))[0]

    if s3_utils.get_file_size(
            's3://' + kwargs['s3_bucket'] + '/' + s3_prefix + '/' + s3_key, s3_connection_id
    ) < minimum_file_size:
        if 'quiet_retries' in kwargs and kwargs['quiet_retries'] > kwargs['ti'].try_number:
            raise ValueError('File is of an unexpected size')
        return kwargs['is_not_valid']

    return kwargs['is_new_valid']

def send_slack_alert(ds, **kwargs):

    # the current task instance of this subdag
    subdag_task_instance = [
        t for t in kwargs['dag'].parent_dag.get_task_instances(Session())
        if t.task_id == kwargs['ti'].dag_id.split('.')[1]
    ][0]

    # amount of total retries
    retries = kwargs['dag'].parent_dag.default_args['retries']

    # only alert on the last retry
    if subdag_task_instance.try_number > retries:
        sc = SlackClient(Variable.get('SlackToken'))
        description = kwargs['file_description']
        template = kwargs['templates_dict']['message']
        expected_file_name = kwargs['expected_file_name_func'](ds, kwargs)
        text = template.format(description, expected_file_name)
        api_params={
            'channel'  : config.PROVIDER_ALERTS_CHANNEL,
            'text'     : text,
            'username' : 'AirFlow',
            'icon_url' : 'https://airflow.incubator.apache.org/_images/pin_large.png'
        }
        r = sc.api_call('chat.postMessage', **api_params)
        if not r['ok']:
            raise AirflowException("Slack API call failed: ({})".format(r['error']))

def s3_validate_file(parent_dag_name, child_dag_name, start_date, schedule_interval, dag_config):
    default_args = {
        'owner': 'airflow',
        'depends_on_past': False,
        'retries': 0
    }

    dag = HVDAG.HVDAG(
        '{}.{}'.format(parent_dag_name, child_dag_name),
        schedule_interval='@daily',
        start_date=start_date,
        default_args=default_args,
        clear_all_tasks_on_retry=True
    )

    is_valid_params = dict(dag_config)
    is_valid_params['is_new_valid'] = 'create_tmp_dir'
    is_valid_params['is_not_valid'] = 'alert_file_size_problem'
    is_valid_params['is_not_new'] = 'alert_no_new_file'
    is_valid_params['is_bad_name'] = 'alert_is_bad_name'
    is_valid_params['quiet_retries'] = dag_config.get('quiet_retries', 0)

    is_valid_new_file = BranchPythonOperator(
        task_id='is_new_file',
        provide_context=True,
        python_callable=do_is_valid_new_file,
        op_kwargs=is_valid_params,
        retries=dag_config.get('quiet_retries', 0),
        retry_delay=timedelta(minutes=10),
        dag=dag
    )

    alert_is_bad_name = PythonOperator(
        task_id='alert_is_bad_name',
        provide_context=True,
        python_callable=send_slack_alert,
        op_kwargs=dag_config,
        templates_dict={
            'message': 'No new {} ({}) matching expected pattern found'
        },
        retries=0,
        dag=dag
    )

    alert_no_new_file = PythonOperator(
        task_id='alert_no_new_file',
        provide_context=True,
        python_callable=send_slack_alert,
        op_kwargs=dag_config,
        templates_dict={
            'message': 'No new {} ({}) found'
        },
        retries=0,
        dag=dag
    )

    alert_file_size_problem = PythonOperator(
        task_id='alert_file_size_problem',
        provide_context=True,
        python_callable=send_slack_alert,
        op_kwargs=dag_config,
        templates_dict={
            'message': '{} ({}) is of an unexpected size'
        },
        retries=0,
        dag=dag
    )

    force_failure = BashOperator(
        task_id='force_failure',
        bash_command='exit 1;',
        trigger_rule='one_success',
        dag=dag
    )

    alert_no_new_file.set_upstream(is_valid_new_file)
    alert_is_bad_name.set_upstream(is_valid_new_file)
    alert_file_size_problem.set_upstream(is_valid_new_file)
    alert_no_new_file.set_downstream(force_failure)
    alert_is_bad_name.set_downstream(force_failure)
    alert_file_size_problem.set_downstream(force_failure)

    return dag
