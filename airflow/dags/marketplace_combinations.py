from airflow.models import Variable
from airflow.operators import *
from datetime import datetime, timedelta

import util.emr_utils as emr_utils
import common.HVDAG as HVDAG
for m in [emr_utils, HVDAG]:
    reload(m)

DAG_NAME='marketplace_combination_hlls_generation'

EMR_CLUSTER_NAME='marketplace-combinations'
NUM_NODES=5
NODE_TYPE='m4.16xlarge'
EBS_VOLUME_SIZE='200'

def do_create_cluster(ds, **kwargs):
    emr_utils.create_emr_cluster(EMR_CLUSTER_NAME, NUM_NODES, NODE_TYPE,
            EBS_VOLUME_SIZE, 'HLL')

def do_delete_cluster(ds, **kwargs):
    emr_utils.delete_emr_cluster(EMR_CLUSTER_NAME)

def do_generate_combination_hlls(ds, **kwargs):
    steps = [('Type=CUSTOM_JAR,Name="Copy Mellon",Jar="command-runner.jar",'
            'ActionOnFailure=CONTINUE,Args=[aws,s3,cp,s3://healthverityreleases/mellon/mellon-assembly-latest.jar,'
            '/tmp/mellon-assembly-latest.jar]'),
        ('Type=Spark,Name="Generate Combinations",ActionOnFailure=CONTINUE, '
            'Args=[--class, com.healthverity.combine.Main, --conf, spark.executor.memory=12G,'
            '--conf, spark.driver.memory=10G, --conf, spark.executor.cores=4, --conf,'
            'spark.executor.instances=80, --conf, spark.yarn.executor.memoryOverhead=1024,'
            '--conf, spark.scheduler.minRegisteredResourcesRatio=1, --conf,'
            'spark.scheduler.maxRegisteredResourcesWaitingTime=60s,'
            '/tmp/mellon-assembly-latest.jar, --patientDir,'
            "'s3a://healthverityreleases/PatientIntersector/hll_seq_data_store/patient/',"
            '--partitions, 2000, --attributes,'
            "'age,biomarker,diagnosis,drug,gender,lab,procedure,region,state']")]

    emr_utils.run_steps(EMR_CLUSTER_NAME, steps)

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2017, 5, 8),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'priority_weight': 5
}

mdag = HVDAG.HVDAG(
    dag_id=DAG_NAME,
    schedule_interval=None,
    default_args=default_args
)

create_cluster = PythonOperator(
    task_id='create_cluster',
    provide_context=True,
    python_callable=do_create_cluster,
    dag=mdag
)

generate_marketplace_combination_hlls = PythonOperator(
    task_id='generate_marketplace_combination_hlls',
    provide_context=True,
    python_callable=do_generate_combination_hlls,
    dag=mdag
)

delete_cluster = PythonOperator(
    task_id='delete_cluster',
    provide_context=True,
    python_callable=do_delete_cluster,
    dag=mdag
)

generate_marketplace_combination_hlls.set_upstream(create_cluster)
delete_cluster.set_upstream(generate_marketplace_combination_hlls)


