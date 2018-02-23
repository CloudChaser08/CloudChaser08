from airflow.models import Variable
from airflow.operators import *
from datetime import datetime, timedelta
from subprocess import check_call
import psycopg2
import json
import re

import util.emr_utils as emr_utils
import util.slack as slack
import common.HVDAG as HVDAG
for m in [emr_utils, HVDAG, slack]:
    reload(m)

DAG_NAME='data_profiling'

EMR_CLUSTER_NAME='data-profiling'
NUM_NODES=5
NODE_TYPE='m4.16xlarge'
EBS_VOLUME_SIZE='100'

PROFILING_STEP_TEMPLATE = ('Type=Spark,Name="{0} Data Profile",ActionOnFailure=TERMINATE_JOB_FLOW, '
        'Args=[--conf, spark.executor.memory=13G, --conf, spark.driver.memory=10G,'
        '--conf, spark.executor.cores=4, --conf, spark.executor.instances=80,'
        '--conf, spark.yarn.executor.memoryOverhead=1024, --conf,'
        'spark.scheduler.minRegisteredResourcesRatio=1, --conf,'
        'spark.files.useFetchCache=false, --conf,'
        'spark.hadoop.fs.s3a.connection.maximum=500, --conf,'
        'spark.scheduler.maxRegisteredResourcesWaitingTime=60s,'
        '/tmp/spark-df-profiling/bin/profile_table.py, --report_name, {1},'
        '--table_name, {0}, --s3_path, {2}]')
BOTO3_INSTALL_STEP = ('Type=CUSTOM_JAR,Name="Install Boto3",Jar="command-runner.jar",'
        'ActionOnFailure=CONTINUE,Args=[sudo,pip,install,boto3]')
PROFILER_COPY_STEP = ('Type=CUSTOM_JAR,Name="Copy Profiler",Jar="command-runner.jar",'
        'ActionOnFailure=CONTINUE,Args=[aws,s3,cp,'
        's3://healthverityreleases/profiling/spark-df-profiling.tar.gz,'
        '/tmp/spark-df-profiling.tar.gz]')
DECOMPRESS_PROFILER_STEP = ('Type=CUSTOM_JAR,Name="Decompress Profiler",Jar="command-runner.jar",'
        'ActionOnFailure=CONTINUE,Args=[tar,-C,/tmp/,-xzf,/tmp/spark-df-profiling.tar.gz]')
PROFILER_INSTALL_STEP = ('Type=CUSTOM_JAR,Name="Install Profiler",Jar="command-runner.jar",'
        'ActionOnFailure=CONTINUE,Args=[sudo,pip,install,/tmp/spark-df-profiling/]')
PROFILING_CONFIG_DB = 'hll_config'
SELECT_PENDING_REQUESTS = 'SELECT * FROM profiling_request WHERE completed IS NULL'
UPDATE_GENERATION_LOG = "UPDATE profiling_request SET completed=now(), s3_url=%s WHERE request_id=%s"
S3_PATH_TEMPLATE = 's3://hvstatus.healthverity.com/profile_reports/{}/'

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2018, 2, 23),
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

def get_ref_db_connection():
    db_config = json.loads(Variable.get('reference_db_user_airflow'))
    return psycopg2.connect(dbname=PROFILING_CONFIG_DB, user=db_config['user'],
            password=db_config['password'], host=db_config['db_host'], 
            port=db_config['db_port'],
            cursor_factory=psycopg2.extras.NamedTupleCursor)
    
def get_pending_requests():
    with get_ref_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(SELECT_PENDING_REQUESTS)
            return cur.fetchall()

def do_create_cluster(ds, **kwargs):
    emr_utils.create_emr_cluster(EMR_CLUSTER_NAME, NUM_NODES, NODE_TYPE,
            EBS_VOLUME_SIZE, 'data-profiling', connected_to_metastore=True)

create_cluster = PythonOperator(
    task_id='create_cluster',
    provide_context=True,
    python_callable=do_create_cluster,
    dag=mdag
)

def get_report_info(req):
    clean_table_name = re.sub('[^a-zA-Z0-9]', '_', req.analytics_table)
    report_name = '{}_{}_report.html'.format(req.request_id, clean_table_name)
    return (S3_PATH_TEMPLATE.format(req.subdirectory), report_name)

def do_generate_profiles(ds, **kwargs):
    profiling_requests = get_pending_requests()

    requests_to_complete = []
    steps = [BOTO3_INSTALL_STEP, PROFILER_COPY_STEP,
            DECOMPRESS_PROFILER_STEP, PROFILER_INSTALL_STEP]
    for req in profiling_requests:
        (s3_path, report_name) = get_report_info(req)
        steps.append(PROFILING_STEP_TEMPLATE.format(req.analytics_table, report_name, s3_path))
        requests_to_complete.append({
            'id' : req.request_id,
            'table' : req.analytics_table,
            's3_url' : s3_path + report_name
        })

    kwargs['ti'].xcom_push(key = 'requests_to_complete', value = json.dumps(requests_to_complete))

    emr_utils.run_steps(EMR_CLUSTER_NAME, steps)

generate_profiles = PythonOperator(
    task_id='generate_profiles',
    provide_context=True,
    python_callable=do_generate_profiles,
    dag=mdag
)

def do_delete_cluster(ds, **kwargs):
    emr_utils.delete_emr_cluster(EMR_CLUSTER_NAME)

delete_cluster = PythonOperator(
    task_id='delete_cluster',
    provide_context=True,
    python_callable=do_delete_cluster,
    dag=mdag
)

def do_update_log(ds, **kwargs):
    requests_completed = json.loads(kwargs['ti'].xcom_pull(dag_id = DAG_NAME,
            task_ids = 'generate_profiles', key = 'requests_to_complete'))
    msg = 'Finished generating data profiles:'
    with get_ref_db_connection() as conn:
        with conn.cursor() as cur:
            for r in requests_completed:
                report_url = r['s3_url'].replace('s3://', 'https://s3.amazonaws.com/')
                cur.execute(UPDATE_GENERATION_LOG,
                    [report_url, r['id']])
                msg += '\nTable `{}`: {}'.format(r['table'], report_url)
                
    slack.send_message('#warehouse', text=msg)

update_log = PythonOperator(
    task_id='update_log',
    provide_context=True,
    python_callable=do_update_log,
    dag=mdag
)

generate_profiles.set_upstream(create_cluster)
delete_cluster.set_upstream(generate_profiles)
update_log.set_upstream(delete_cluster)
