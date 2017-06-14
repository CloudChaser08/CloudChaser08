from airflow.models import Variable
from airflow.operators import PythonOperator
from pyhive import hive

import common.HVDAG as HVDAG

for m in [ HVDAG]:
    reload(m)

def do_update_db(ds, **kwargs):
    sql_command=kwargs['sql_command_func'](ds, kwargs)
    cursor = hive.connect(Variable.get('analytics_db_host')).cursor()
    cursor.execute(sql_command)
    cursor.close()

def update_analytics_db(parent_dag_name, child_dag_name, start_date, schedule_interval, dag_config):
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

    update_db = PythonOperator(
        task_id='update_db',
        python_callable=do_update_db,
        provide_context=True,
        op_kwargs=dag_config,
        dag=dag
    )

    return dag
