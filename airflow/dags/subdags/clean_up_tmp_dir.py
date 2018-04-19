from airflow.operators import BashOperator
from datetime import datetime, timedelta
import logging

import common.HVDAG as HVDAG

for m in [HVDAG]:
    reload(m)

def clean_up_tmp_dir(parent_dag_name, child_dag_name, start_date, schedule_interval, dag_config):
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

    if dag_config['tmp_path_template'].count('{}') == 3:
        bash_string = 'rm -r {};'.format(dag_config['tmp_path_template'].format('{{ ts_nodash }}', '', ''))
    else:
        bash_string = 'rm -r {};'.format(dag_config['tmp_path_template'].format('{{ ts_nodash }}'))

    remove_tmp_dir = BashOperator(
        task_id='remove_tmp_dir',
        bash_command = bash_string,
        dag=dag
    )

    return dag
