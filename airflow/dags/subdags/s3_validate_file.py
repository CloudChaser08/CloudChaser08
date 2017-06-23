from airflow.models import Variable
from airflow.operators import BashOperator, \
    BranchPythonOperator, SlackAPIOperator
import re

import util.s3_utils as s3_utils
import config as config
import common.HVDAG as HVDAG

for m in [s3_utils, config, HVDAG]:
    reload(m)

def do_is_valid_new_file(ds, **kwargs):
    # We expect the files that were made available on HealthVerity's S3
    s3_prefix          = kwargs['s3_prefix'][:-1] if kwargs['s3_prefix'].endswith('/') \
                         else kwargs['s3_prefix']
    file_name_pattern  = kwargs['file_name_pattern_func'](ds, kwargs)
    expected_file_name = kwargs['expected_file_name_func'](ds, kwargs)
    minimum_file_size  = kwargs['minimum_file_size']
    s3_connection_id   = kwargs.get('s3_connection', s3_utils.DEFAULT_CONNECTION_ID)

    s3_keys = s3_utils.list_s3_bucket_files(
        's3://' + kwargs['s3_bucket'] + '/' + s3_prefix + '/', s3_connection_id
    )

    if len(filter(lambda k: len(re.findall(file_name_pattern, k.split('/')[-1])) == 1, s3_keys)) == 0:
        return kwargs['is_bad_name']

    # Check if there are files matching the name exactly, or, if regex match
    # was specified, there are files matching the regex pattern
    if not (len(filter(lambda k: k.split('/')[-1] == expected_file_name, s3_keys)) > 0 \
            or (regex_name_match in kwargs and kwargs['regex_name_match'] and \
            len(filter(lambda k: re.search(expected_file_name, k.split('/')[-1]), s3_keys)) > 0)):

        return kwargs['is_not_new']

    # Grab the first key that's either an exact match or a partial match
    s3_key = (filter(lambda k: k.split('/')[-1] == expected_file_name, s3_keys) + 
            filter(lambda k: re.search(expected_file_name, k.split('/')[-1]), s3_keys))[0]

    if s3_utils.get_file_size(
            's3://' + kwargs['s3_bucket'] + '/' + s3_prefix + '/' + s3_key, s3_connection_id
    ) < minimum_file_size:
        return kwargs['is_not_valid']

    return kwargs['is_new_valid']


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
        default_args=default_args
    )

    is_valid_params = dict(dag_config)
    is_valid_params['is_new_valid'] = 'create_tmp_dir'
    is_valid_params['is_not_valid'] = 'alert_file_size_problem'
    is_valid_params['is_not_new'] = 'alert_no_new_file'
    is_valid_params['is_bad_name'] = 'alert_is_bad_name'

    is_valid_new_file = BranchPythonOperator(
        task_id='is_new_file',
        provide_context=True,
        python_callable=do_is_valid_new_file,
        op_kwargs=is_valid_params,
        dag=dag
    )
    
    alert_is_bad_name = SlackAPIOperator(
        task_id='alert_is_bad_name',
        token=Variable.get('SlackToken'),
        method='chat.postMessage',
        retries=0,
        api_params={
            'channel'  : config.SLACK_CHANNEL,
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
            'channel'  : config.SLACK_CHANNEL,
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
            'channel'  : config.SLACK_CHANNEL,
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
    
    alert_no_new_file.set_upstream(is_valid_new_file)
    alert_is_bad_name.set_upstream(is_valid_new_file)
    alert_file_size_problem.set_upstream(is_valid_new_file)
    alert_no_new_file.set_downstream(force_failure)
    alert_is_bad_name.set_downstream(force_failure)
    alert_file_size_problem.set_downstream(force_failure)

    return dag
