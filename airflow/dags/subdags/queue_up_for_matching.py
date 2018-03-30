import os
import logging
from airflow.models import Variable
from airflow.operators import PythonOperator
from subprocess import check_call

import common.HVDAG as HVDAG

for m in [HVDAG]:
    reload(m)

def do_queue_up_for_matching(ds, **kwargs):
    source_files = kwargs['source_files_func'](ds, kwargs)
    environ = {
        'AWS_ACCESS_KEY_ID' : Variable.get('AWS_ACCESS_KEY_ID_MATCH_PUSHER'),
        'AWS_SECRET_ACCESS_KEY' : Variable.get('AWS_SECRET_ACCESS_KEY_MATCH_PUSHER')
    }

    queue_up_cmd = [
            os.getenv('AIRFLOW_HOME')
            + '/dags/resources/push_file_to_s3_batchless.sh',
            None, '0', 'prod-matching-engine', 'priority3'
        ]

    if 'passthrough_only' in kwargs and kwargs['passthrough_only']:
        queue_up_cmd.append("true")

    if 'priority' in kwargs:
        queue_up_cmd[4] = kwargs['priority']

    if not source_files:
        logging.warn("Source files func returned no files. No files queued.")

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
