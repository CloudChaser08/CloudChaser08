from airflow.models import Variable
from airflow.operators import *
from datetime import datetime, timedelta

import util.emr_utils as emr_utils
import util.sqs_utils as sqs_utils
import common.HVDAG as HVDAG
for m in [emr_utils, HVDAG, sqs_utils]:
    reload(m)

DAG_NAME='humana_hv000468_extraction'
EMR_CLUSTER_NAME='humana-data-extraction'
START_DATE=datetime(2018, 7, 10)
HUMANA_INBOX='https://sqs.us-east-1.amazonaws.com/581191604223/humana-inbox-prod'

NUM_NODES=5
NODE_TYPE='m4.16xlarge'
EBS_VOLUME_SIZE='100'

BOTO3_INSTALL_STEP = ('Type=CUSTOM_JAR,Name="Install Boto3",Jar="command-runner.jar",'
        'ActionOnFailure=CONTINUE,Args=[sudo,pip,install,boto3]')
EXTRACTION_STEP = ('Type=Spark,Name="Extract for Humana",'
        'ActionOnFailure=TERMINATE_JOB_FLOW,Args=[--jars,'
        '"/home/hadoop/spark/common/json-serde-1.3.7-jar-with-dependencies.jar,'
        '/home/hadoop/spark/common/HiveJDBC41.jar",'
        '--py-files, /home/hadoop/spark/target/dewey.zip, --conf,'
        'spark.driver.memory=10G, --conf, spark.executor.memory=13G,'
        '--conf, spark.executor.cores=4, --conf, spark.files.useFetchCache=false,'
        '--conf, spark.hadoop.s3a.connection.maximum=500, --conf,'
        'spark.default.parallelism=5000, /home/hadoop/spark/delivery/humana_000468/sparkExtractHumana.py]')

create_humana_dag(DAG_NAME, EMR_CLUSTER_NAME, HUMANA_INBOX, START_DATE)

def create_humana_dag(dag_name, emr_cluster_name, inbox_url, start_date):

    default_args = {
        'owner': 'airflow',
        'start_date': start_date,
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=2),
        'priority_weight': 5
    }

    mdag = HVDAG.HVDAG(
        dag_id=dag_name,
        schedule_interval='4,19,34,49 * * * *',
        default_args=default_args
    )

    def do_check_pending_requests(ds, **kwargs):
        if not emr_utils.cluster_running(emr_cluster_name):
            if not sqs_utils.is_empty(inbox_url):
                return 'create_cluster'

        return 'do_nothing'

    check_pending_requests = BranchPythonOperator(
        task_id='check_pending_requests',
        provide_context=True,
        python_callable=do_check_pending_requests,
        retries=0,
        dag=mdag
    )

    do_nothing = DummyOperator(task_id='do_nothing', dag=mdag)

    def do_create_cluster(ds, **kwargs):
        emr_utils.create_emr_cluster(emr_cluster_name, NUM_NODES, NODE_TYPE,
                EBS_VOLUME_SIZE, 'delivery', connected_to_metastore=True)

    create_cluster = PythonOperator(
        task_id='create_cluster',
        provide_context=True,
        python_callable=do_create_cluster,
        dag=mdag
    )

    def do_run_extraction(ds, **kwargs):
        emr_utils._build_dewey(emr_utils._get_emr_cluster_id(emr_cluster_name))
        steps = [BOTO3_INSTALL_STEP, EXTRACTION_STEP]
        emr_utils.run_steps(emr_cluster_name, steps)

    run_extraction = PythonOperator(
        task_id='run_extraction',
        provide_context=True,
        python_callable=do_run_extraction,
        dag=mdag
    )
    def do_delete_cluster(ds, **kwargs):
        emr_utils.delete_emr_cluster(emr_cluster_name)

    delete_cluster = PythonOperator(
        task_id='delete_cluster',
        provide_context=True,
        python_callable=do_delete_cluster,
        dag=mdag
    )

    create_cluster.set_upstream(check_pending_requests)
    do_nothing.set_upstream(check_pending_requests)
    run_extraction.set_upstream(create_cluster)
    delete_cluster.set_upstream(run_extraction)
