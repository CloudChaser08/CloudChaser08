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

# modify for quarterly refresh
MIN_DATE = '2016-01-01'
MAX_DATE = '2018-01-01'

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
HLL_RM_STEP = ('Type=CUSTOM_JAR,Name="Delete HLLs Dir",Jar="command-runner.jar",'
        'ActionOnFailure=CONTINUE,Args=[hdfs,dfs,-rm,-r,' + HDFS_STAGING + ']')
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
    schedule_interval='7,22,37,52 * * * *',
    default_args=default_args
)

def get_ref_db_connection():
    db_config = json.loads(Variable.get('reference_db_user_airflow'))
    return psycopg2.connect(dbname=HLL_CONFIG_DB, user=db_config['user'],
            password=db_config['password'], host=db_config['db_host'],
            port=db_config['db_port'],
            cursor_factory=psycopg2.extras.NamedTupleCursor)

def get_feeds_to_generate_configs():
    all_configs = None
    with get_ref_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(SELECT_CONFIG_AND_LAST_LOG_ENTRY)
            all_configs = cur.fetchall()

    to_generate_configs = []
    for entry in all_configs:
        # TODO: Let Airflow figure out the current quarter programatically
        # For now, hardcode to 2017Q3, with an HLL generation date of 04/24/2018
        if not entry.generated or entry.is_stale or (not entry.once_only and \
                entry.generated.replace(tzinfo=None) < datetime(2018, 4, 24, 12)):
            to_generate_configs.append(entry)

    return to_generate_configs

def do_check_pending_requests(ds, **kwargs):
    if not emr_utils.cluster_running(EMR_CLUSTER_NAME):
        hll_configs = get_feeds_to_generate_configs()
        if hll_configs:
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
    emr_utils.create_emr_cluster(EMR_CLUSTER_NAME, NUM_NODES, NODE_TYPE,
            EBS_VOLUME_SIZE, 'HLL')

create_cluster = PythonOperator(
    task_id='create_cluster',
    provide_context=True,
    python_callable=do_create_cluster,
    dag=mdag
)

def do_generate_hlls(ds, **kwargs):
    hll_configs = get_feeds_to_generate_configs()

    steps = [MELLON_COPY_STEP]
    emr_utils.run_steps(EMR_CLUSTER_NAME, steps)

    for entry in hll_configs:
        steps = []

        args = ''
        args += '--datafeed, {},'.format(entry.feed_id)
        args += '--modelName, {},'.format(entry.model)
        args += "--inpath, '{}',".format(entry.s3a_url)
        args += '--format, {},'.format(entry.file_format or 'parquet')
        args += '--start, {},'.format(MIN_DATE) if not entry.no_min_cap else ''
        args += '--end, {},'.format(MAX_DATE) if not entry.no_max_cap else ''
        args += "--models, '{}',".format(entry.emr_models) if entry.emr_models else ''
        args += ', '.join((entry.flags or '').split())

        steps.append(HLL_STEP_TEMPLATE.format(entry.feed_id, args))
        steps.append(HLL_COPY_STEP)
        steps.append(HLL_RM_STEP)

        emr_utils.run_steps(EMR_CLUSTER_NAME, steps)

        do_update_log(entry.feed_id)

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

def do_update_log(feed_generated):
    with get_ref_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(UPDATE_GENERATION_LOG, [feed_generated])

    msg =  'Finished generating HLLs for feed ' + feed_generated
    slack.send_message('#logistics', text=msg)

create_cluster.set_upstream(check_pending_requests)
do_nothing.set_upstream(check_pending_requests)
generate_hlls.set_upstream(create_cluster)
delete_cluster.set_upstream(generate_hlls)
