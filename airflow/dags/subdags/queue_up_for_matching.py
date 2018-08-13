import os
import logging
import re
from airflow.models import Variable
from airflow.operators import PythonOperator
from subprocess import check_call

import common.HVDAG as HVDAG
import util.s3_utils as s3_utils

for m in [HVDAG, s3_utils]:
    reload(m)

def do_queue_up_for_matching(ds, **kwargs):
    source_files = kwargs['source_files_func'](ds, kwargs)

    environ = {
        'AWS_ACCESS_KEY_ID' : Variable.get('AWS_ACCESS_KEY_ID_MATCH_PUSHER'),
        'AWS_SECRET_ACCESS_KEY' : Variable.get('AWS_SECRET_ACCESS_KEY_MATCH_PUSHER')
    }

    if kwargs.get('regex_name_match'):
        source_file_regexes = source_files
        source_files = []
        for regex in source_file_regexes:
            path = '/'.join(regex.split('/')[:-1]) + '/'
            name_reg = regex.split('/')[-1]

            keys = [
                k for k in s3_utils.list_s3_bucket(path) if re.search(name_reg, k)
            ]
            if not keys:
                logging.warn("No files found for regex: {}".format(name_reg))
            else:
                source_files.extend(keys)

    if not source_files:
        logging.warn("Source files func returned no files. No files queued.")
        return

    queue_up_cmd = [
            os.getenv('AIRFLOW_HOME')
            + '/dags/resources/push_file_to_s3_batchless.sh',
            None, '0', 'prod-matching-engine', 'priority3'
        ]

    if 'write_lock' in kwargs and kwargs['write_lock']:
        queue_up_cmd.append("true")
    else:
        queue_up_cmd.append("false")

    if 'passthrough_only' in kwargs and kwargs['passthrough_only']:
        queue_up_cmd.append("true")

    if 'matching-engine' in kwargs and kwargs['matching-engine']:
        queue_up_cmd[3] = kwargs['matching-engine']

    if 'priority' in kwargs:
        queue_up_cmd[4] = kwargs['priority']

    for f in source_files:
        logging.info("Queueing up {}".format(f))
        queue_up_cmd[1] = f
        check_call(queue_up_cmd, env=environ)


def queue_up_for_matching(
        parent_dag_name, child_dag_name, start_date,
        schedule_interval, dag_config
):
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

    queue_up_for_matching = PythonOperator(
        task_id='queue_up_for_matching',
        provide_context=True,
        python_callable=do_queue_up_for_matching,
        op_kwargs=dag_config,
        dag=dag
    )

    return dag
