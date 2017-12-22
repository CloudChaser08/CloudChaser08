from airflow.operators import PythonOperator
from datetime import datetime, timedelta

import util.emr_utils as emr_utils
import common.HVDAG as HVDAG
for m in [emr_utils, HVDAG]:
    reload(m)

DAG_NAME='celgene_delivery'

EMR_CLUSTER_NAME='delivery_cluster-celgene'
NUM_NODES=5
NODE_TYPE='m4.2xlarge'
EBS_VOLUME_SIZE='100'

def do_create_cluster(ds, **kwargs):
    emr_utils.create_emr_cluster(EMR_CLUSTER_NAME, NUM_NODES, NODE_TYPE,
            EBS_VOLUME_SIZE, 'delivery', connected_to_metastore=True)

def do_delete_cluster(ds, **kwargs):
    emr_utils.delete_emr_cluster(EMR_CLUSTER_NAME)

def do_run_pyspark_export_routine(ds, **kwargs):
    emr_utils.export(
        EMR_CLUSTER_NAME,
        kwargs['pyspark_script_name'],
        kwargs['pyspark_args_func'](ds, kwargs)
    )

def get_export_args(ds, kwargs):
    return ['--date', kwargs['ds']]

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2017, 5, 8),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'priority_weight': 5
}

mdag = HVDAG.HVDAG(
    dag_id=DAG_NAME,
    schedule_interval='0 0 5 * *',
    default_args=default_args
)

create_cluster = PythonOperator(
    task_id='create_cluster',
    provide_context=True,
    python_callable=do_create_cluster,
    dag=mdag
)

run_pyspark_export_routine = PythonOperator(
    task_id='run_pyspark_export_routine',
    provide_context=True,
    python_callable=do_run_pyspark_export_routine,
    op_kwargs={
        'pyspark_script_name' : '/home/airflow/spark/delivery/celgene_hv000242/sparkExtractCelgene.py',
        'pyspark_args_func'   : get_export_args
    },
    dag=mdag
)

delete_cluster = PythonOperator(
    task_id='delete_cluster',
    provide_context=True,
    python_callable=do_delete_cluster,
    dag=mdag
)

run_pyspark_export_routine.set_upstream(create_cluster)
delete_cluster.set_upstream(run_pyspark_export_routine)
