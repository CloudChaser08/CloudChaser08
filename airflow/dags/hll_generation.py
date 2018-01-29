from airflow.models import Variable
from airflow.operators import *
from datetime import datetime, timedelta
from subprocess import check_call
import psycopg2
import json

import util.emr_utils as emr_utils
import util.slack as slack
import common.HVDAG as HVDAG
for m in [emr_utils, HVDAG, slack]:
    reload(m)

DAG_NAME='hll_generation'

EMR_CLUSTER_NAME='hll-generation'
NUM_NODES=5
NODE_TYPE='m4.16xlarge'
EBS_VOLUME_SIZE='500'

HDFS_STAGING = 'hdfs:///hlls_out/'
HLL_STEP_TEMPLATE = ('Type=Spark,Name="{} HLLs",ActionOnFailure=TERMINATE_JOB_FLOW, '
        'Args=[--class, com.healthverity.aggregate.Main, --conf,'
        'spark.executor.memory=13G, --conf, spark.driver.memory=10G,'
        '--conf, spark.executor.cores=4, --conf, spark.executor.instances=80,'
        '--conf, spark.yarn.executor.memoryOverhead=1024, --conf,'
        'spark.scheduler.minRegisteredResourcesRatio=1, --conf,'
        'spark.scheduler.maxRegisteredResourcesWaitingTime=60s,'
        '/tmp/mellon-assembly-latest.jar, --outpath,' + HDFS_STAGING + ','
        '--partitions, 5000, {}]')
MELLON_COPY_STEP = ('Type=CUSTOM_JAR,Name="Copy Mellon",Jar="command-runner.jar",'
        'ActionOnFailure=CONTINUE,Args=[aws,s3,cp,'
        's3://healthverityreleases/mellon/mellon-assembly-latest.jar,'
        '/tmp/mellon-assembly-latest.jar]')
HLL_COPY_STEP = ('Type=CUSTOM_JAR,Name="Copy HLLs",Jar="command-runner.jar",'
        'ActionOnFailure=CONTINUE,Args=[s3-dist-cp,--src,' + HDFS_STAGING + ','
        '--dest,s3a://healthverityreleases/PatientIntersector/hll_seq_data_store/]')
HLL_CONFIG_DB = 'hll_config'
SELECT_CONFIG_AND_LAST_LOG_ENTRY = """
    SELECT *
    FROM generation_config
    LEFT JOIN
        (
        SELECT l.*
        FROM generation_log l
        INNER JOIN
            (
            SELECT feed_id, MAX(generated) last_generated
            FROM generation_log
            GROUP BY feed_id
            ) a
            ON l.generated=a.last_generated AND l.feed_id = a.feed_id
        ) gl USING (feed_id)
    """
UPDATE_GENERATION_LOG = "INSERT INTO generation_log VALUES (%s)"

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2018, 1, 17),
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
    return psycopg2.connect(dbname=HLL_CONFIG_DB, user=db_config['user'],
            password=db_config['password'], host=db_config['db_host'], 
            port=db_config['db_port'],
            cursor_factory=psycopg2.extras.NamedTupleCursor)
    
def get_config():
    with get_ref_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(SELECT_CONFIG_AND_LAST_LOG_ENTRY)
            return cur.fetchall()

def do_create_cluster(ds, **kwargs):
    emr_utils.create_emr_cluster(EMR_CLUSTER_NAME, NUM_NODES, NODE_TYPE,
            EBS_VOLUME_SIZE, 'HLL')

create_cluster = PythonOperator(
    task_id='create_cluster',
    provide_context=True,
    python_callable=do_create_cluster,
    dag=mdag
)

def do_generate_hlls(ds, **kwargs):
    hlls_config = get_config()

    feeds_to_generate = []
    steps = [MELLON_COPY_STEP]
    for entry in hlls_config:
        if not entry.generated or entry.is_stale or (not entry.once_only and \
                entry.generated.replace(tzinfo=None) < datetime(2018, 1, 3, 12)):

            args = ''
            args += '--datafeed, {},'.format(entry.feed_id)
            args += '--modelName, {},'.format(entry.model)
            args += "--inpath, '{}',".format(entry.s3a_url)
            args += '--format, {},'.format(entry.file_format or 'parquet')
            args += '--start, 2015-10-01,' if not entry.no_min_cap else ''
            args += '--end, 2017-10-01,' if not entry.no_max_cap else ''
            args += "--models, '{}',".format(entry.emr_models) if entry.emr_models else ''
            args += ', '.join((entry.flags or '').split())

            steps.append(HLL_STEP_TEMPLATE.format(entry.feed_id, args))
            feeds_to_generate.append(entry.feed_id)
    steps.append(HLL_COPY_STEP)

    kwargs['ti'].xcom_push(key = 'feeds_to_generate', value = json.dumps(feeds_to_generate))

    emr_utils.run_steps(EMR_CLUSTER_NAME, steps)

generate_hlls = PythonOperator(
    task_id='generate_hlls',
    provide_context=True,
    python_callable=do_generate_hlls,
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
    feeds_generated = json.loads(kwargs['ti'].xcom_pull(dag_id = DAG_NAME,
            task_ids = 'generate_hlls', key = 'feeds_to_generate'))
    for f in feeds_generated:
        with get_ref_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(UPDATE_GENERATION_LOG, [f])

    msg =  'Finished generating HLLs for feed'
    if len(feeds_generated) > 1:
        msg += 's '
        msg += ', '.join(feeds_generated[:-1])
        msg += ', and {}'.format(feeds_generated[-1])
    else:
        msg += ' ' + feeds_generated[0]

    slack.send_message('#data-automation', text=msg)

update_log = PythonOperator(
    task_id='update_log',
    provide_context=True,
    python_callable=do_update_log,
    dag=mdag
)

generate_hlls.set_upstream(create_cluster)
delete_cluster.set_upstream(generate_hlls)
update_log.set_upstream(delete_cluster)
