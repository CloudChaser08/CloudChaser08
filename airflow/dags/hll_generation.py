from airflow.models import Variable
from airflow.operators import *
from datetime import datetime, timedelta
from subprocess import check_call
import json

import util.emr_utils as emr_utils
import util.s3_utils as s3_utils
import util.slack as slack
import common.HVDAG as HVDAG
for m in [emr_utils, HVDAG, slack, s3_utils]:
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

def do_download_configs(ds, **kwargs):
    check_call(['mkdir', '-p', '/tmp/hll_generation'])
    s3_utils.copy_file('s3://healthverityreleases/mellon/hlls_config.json', '/tmp/hll_generation/', encrypt=False)
    s3_utils.copy_file('s3://healthverityreleases/mellon/hll_generation_log.json', '/tmp/hll_generation/', encrypt=False)

download_configs = PythonOperator(
    task_id='download_configs',
    provide_context=True,
    python_callable=do_download_configs,
    dag=mdag
)

def do_create_cluster(ds, **kwargs):
    emr_utils.create_emr_cluster(EMR_CLUSTER_NAME, NUM_NODES, NODE_TYPE,
            EBS_VOLUME_SIZE, 'HLL', use_spot_bids=True)

create_cluster = PythonOperator(
    task_id='create_cluster',
    provide_context=True,
    python_callable=do_create_cluster,
    dag=mdag
)

def do_generate_hlls(ds, **kwargs):
    with open('/tmp/hll_generation/hlls_config.json') as fin:
        hll_configs = json.loads(fin)

    feed_config = dict([(c['feed_id'], c) for c in hll_configs])

    with open('/tmp/hll_generation/hll_generation_log.json') as fin:
        hll_generation_log = json.loads(fin)

    for entry in hll_generation_log:
        if entry['date_ran'] < '2050-01-01T12:00:00':
            del feed_config[entry['feed_id']]

    steps = [MELLON_COPY_STEP]
    for feed_id in feed_config:
        config = feed_config[feed_id]
        args = ''
        args += '--datafeed, {},'.format(feed_id)
        args += '--modelName, {},'.format(config['model'])
        args += "--inpath, '{}',".format(config['s3a_path'])
        args += '--format, {},'.format(config.get('file_format', 'parquet'))
        args += '--start, 2015-10-01,' if not config.get('no_min_cap') else ''
        args += '--end, 2017-10-01,' if not config.get('no_max_cap') else ''
        args += "--models, '{}',".format(config.get('emr_models')) if config.get('emr_models') else ''
        args += ', '.join(config.get('flags', '').split())

        steps.append(HLL_STEP_TEMPLATE.format(feed_id, args))
    steps.append(HLL_COPY_STEP)

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
    with open('/tmp/hll_generation/hlls_config.json') as fin:
        hll_configs = json.loads(fin)

    feed_config = dict([(c['feed_id'], c) for c in hll_configs])

    with open('/tmp/hll_generation/hll_generation_log.json') as fin:
        hll_generation_log = json.loads(fin)

    for entry in hll_generation_log:
        if entry['date_ran'] < '2050-01-01T12:00:00':
            del feed_config[entry['feed_id']]

    for f in feed_config:
        hll_generation_log.append({'feed_id': f, 'last_ran' : datetime.now().isoformat()})
    
    with open('/tmp/hll_generation/hll_generation_log.json', 'w') as fout:
        json.dump(hll_generation_log, fout, sort_keys=True, indent=4)

    msg =  'Finished generating HLLs for feed'
    if len(feed_config) > 1:
        msg += 's '
        msg += ', '.join(feed_config.keys()[:-1])
        msg += ', and {}'.format(feed_config.keys()[-1])
    else:
        msg += feed_config.keys()[0]

    slack.send_message('#data-automation', text=msg)
    s3_utils.copy_file('/tmp/hll_generation/hll_generation_log.json', 's3://healthverityreleases/mellon/')

update_log = PythonOperator(
    task_id='update_log',
    provide_context=True,
    python_callable=do_update_log,
    dag=mdag
)

create_cluster.set_upstream(download_configs)
generate_hlls.set_upstream(create_cluster)
delete_cluster.set_upstream(generate_hlls)
update_log.set_upstream(delete_cluster)
