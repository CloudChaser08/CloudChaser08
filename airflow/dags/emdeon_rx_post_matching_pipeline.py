from airflow import DAG
from airflow.models import Variable
from airflow.operators import *
from datetime import datetime, timedelta
from subprocess import check_output, check_call, STDOUT, Popen
import airflow.hooks.S3_hook
import logging
import os
import time
import json

S3_PATH_PREFIX='s3://salusv/matching/prod/payload/86396771-0345-4d67-83b3-7e22fded9e1d/'
S3_PREFIX='matching/prod/payload/86396771-0345-4d67-83b3-7e22fded9e1d/'
S3_PAYLOAD_LOC='s3://salusv/matching/payload/pharmacyclaims/emdeon/'
REDSHIFT_CREATE_COMMAND_TEMPLATE = """/home/airflow/airflow/dags/resources/redshift.py create \\
--identifier {{ params.cluster_id }} --num_nodes {{ params.num_nodes }}"""
REDSHIFT_DELETE_COMMAND_TEMPLATE = """/home/airflow/airflow/dags/resources/redshift.py delete \\
--identifier {{ params.cluster_id }}"""
EMR_CREATE_COMMAND_TEMPLATE = """/home/airflow/airflow/dags/resources/launchEMR \\
{{ params.cluster_name }} {{ params.num_nodes }} {{ params.node_type }} "{{ params.applications }}" \\
{{ params.use_ebs }} {{ params.ebs_volume_size }}"""
EMR_DELETE_COMMAND_TEMPLATE = """/home/airflow/airflow/dags/resources/redshift.py delete \\
--identifier {{ params.cluster_id }}"""
EMR_COPY_MELLON_STEP = ('Type=CUSTOM_JAR,Name="Copy Mellon",Jar="command-runner.jar",'
    'ActionOnFailure=CONTINUE,Args=[aws,s3,cp,s3://healthverityreleases/mellon/mellon-assembly-latest.jar,'
    '/tmp/mellon-assembly-latest.jar]')
EMR_TRANSFORM_TO_PARQUET_STEP = ('Type=Spark,Name="Transform Emdeon RX to Parquet",ActionOnFailure=CONTINUE, '
    'Args=[--class,com.healthverity.parquet.Main,--conf,spark.sql.parquet.compression.codec=gzip,'
    '/tmp/mellon-assembly-latest.jar,{},{},pharmacy,hdfs:///parquet/pharmacyclaims/emdeon/{},'
    's3a://salusv/warehouse/text/pharmacyclaims/emdeon/{},20,"|","false"]')
EMR_DELETE_OLD_PARQUET = ('Type=CUSTOM_JAR,Name="Delete old data from S3",Jar="command-runner.jar",'
    'ActionOnFailure=CONTINUE,Args=[aws,s3,rm,--recursive,s3://salusv/warehouse/parquet/pharmacyclaims/emdeon/{}]')
EMR_DISTCP_TO_S3 = ('Type=CUSTOM_JAR,Name="Distcp to S3",Jar="command-runner.jar",' 
    'ActionOnFailure=CONTINUE,Args=[s3-dist-cp,"--src=hdfs:///parquet/pharmacyclaims/emdeon",'
    '"--dest=s3://salusv/warehouse/parquet/pharmacyclaims/emdeon/"]')
RS_CLUSTER_ID="emdeon-rx-norm"
RS_HOST=RS_CLUSTER_ID + '.cz8slgfda3sg.us-east-1.redshift.amazonaws.com'
RS_USER='hvuser'
RS_DATABASE='dev'
RS_PORT='5439'
RS_NUM_NODES=10
EMR_CLUSTER_NAME="emdeon-rx-norm"
EMR_NUM_NODES='5'
EMR_NODE_TYPE="c4.xlarge"
EMR_APPLICATIONS="Name=Hadoop Name=Hive Name=Presto Name=Ganglia Name=Spark"
EMR_USE_EBS="false"
EMR_EBS_VOLUME_SIZE="0"

def do_move_matching_payload(ds, **kwargs):
    hook = airflow.hooks.S3_hook.S3Hook(s3_conn_id='my_conn_s3')
    deid_filename = kwargs['dag_run'].conf['deid_filename']
    s3_prefix = '{}{}'.format(S3_PREFIX, deid_filename)
    for payload_file in hook.list_keys('salusv', s3_prefix):
        date = '{}/{}/{}'.format(deid_filename[0:4], deid_filename[4:6], deid_filename[6:8])
        check_call(['aws', 's3', 'cp', '--sse', 'AES256', 's3://salusv/' + payload_file, S3_PAYLOAD_LOC + date + '/' + payload_file.split('/')[-1]])

def do_detect_matching_done(ds, **kwargs):
    hook = airflow.hooks.S3_hook.S3Hook(s3_conn_id='my_conn_s3')
    row_count = int(kwargs['dag_run'].conf['row_count'])
    chunk_start = row_count / 1000000 * 1000000
    deid_filename = kwargs['dag_run'].conf['deid_filename']
    template = '{}{}*'
    if row_count >= 1000000:
        template += '{}-{}'
    template += '*'
    s3_key = template.format(S3_PATH_PREFIX, deid_filename, chunk_start, row_count)
    logging.info('Poking for key : {}'.format(s3_key))
    while not hook.check_for_wildcard_key(s3_key, None):
        time.sleep(60)

def do_run_normalization_routine(ds, **kwargs):
    hook = airflow.hooks.S3_hook.S3Hook(s3_conn_id='my_conn_s3')
    file_date = kwargs['dag_run'].conf['ds_yesterday']
    s3_key = hook.list_keys('salusv', 'incoming/pharmacyclaims/emdeon/{}'.format(file_date.replace('-', '/')))[0]
    setid = s3_key.split('/')[-1].replace('.bz2','')[0:-3]

    env = dict(os.environ)
    s3_credentials = 'aws_access_key_id={};aws_secret_access_key={}'.format(
                         env['AWS_ACCESS_KEY_ID'], env['AWS_SECRET_ACCESS_KEY']
                     )
    command = ['/home/airflow/airflow/dags/providers/emdeon/pharmacyclaims/rsNormalizeEmdeonRX.py',
        '--date', file_date, '--setid', setid, '--s3_credentials', s3_credentials, '--first_run']
    env['PGHOST'] = RS_HOST
    env['PGUSER'] = RS_USER
    env['PGDATABASE'] = RS_DATABASE
    env['PGPORT'] = RS_PORT
    env['PGPASSWORD'] = Variable.get('rs_norm_password')
    cwd = '/home/airflow/airflow/dags/providers/emdeon/pharmacyclaims/'
    p = Popen(command, env=env, cwd=cwd)
    p.wait()

def get_emr_cluster_id(cluster_name):
    clusters = json.loads(check_output(['aws', 'emr', 'list-clusters', '--active']))
    for cluster in clusters['Clusters']:
        if cluster['Name'] == cluster_name:
            return cluster['Id']

def do_transform_to_parquet(ds, **kwargs):
    file_date = kwargs['dag_run'].conf['ds_yesterday']
    cluster_id = get_emr_cluster_id(EMR_CLUSTER_NAME)
    transform_steps = []
    delete_steps = []
    env = dict(os.environ)
    for i in xrange(0, 15):
        d = (datetime.strptime(file_date, '%Y-%m-%d') - timedelta(days=i)).strftime('%Y/%m/%d')
        transform_steps.append(EMR_TRANSFORM_TO_PARQUET_STEP.format(
            env['AWS_ACCESS_KEY_ID'], env['AWS_SECRET_ACCESS_KEY'],d,d))
        delete_steps.append(EMR_DELETE_OLD_PARQUET.format(d))
    check_call(['aws', 'emr', 'add-steps', '--cluster-id', cluster_id,
                '--steps', EMR_COPY_MELLON_STEP] + transform_steps + delete_steps +
                [EMR_DISTCP_TO_S3])
    incomplete_steps=1
    failed_steps=0
    while incomplete_steps > 0:
        incomplete_steps=0
        time.sleep(60)
        cluster_steps = json.loads(check_output(['aws', 'emr', 'list-steps', '--cluster-id', cluster_id]))
        for step in cluster_steps['Steps']:
            if step['Status']['State'] == "PENDING" or step['Status']['State'] == "RUNNING":
                incomplete_steps += 1
            elif step['Status']['State'] == "FAILED":
                failed_steps += 1
    if failed_steps > 0:
        logging.info("ITS ALL BROKEN")

def do_create_emr_cluster(ds, **kwargs):
    cluster_details = json.loads(check_output([
        '/home/airflow/airflow/dags/resources/launchEMR',
        EMR_CLUSTER_NAME,
        EMR_NUM_NODES,
        EMR_NODE_TYPE,
        EMR_APPLICATIONS,
        EMR_USE_EBS,
        EMR_EBS_VOLUME_SIZE
    ]))
    check_call(['aws', 'emr', 'wait', 'cluster-running', '--cluster-id', cluster_details['ClusterId']])

def do_delete_emr_cluster(ds, **kwargs):
    cluster_id = get_emr_cluster_id(EMR_CLUSTER_NAME)
    check_call(['aws', 'emr', 'terminate-clusters', '--cluster-ids', cluster_id])

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2017, 1, 1, 12),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(hours=1)
}

mdag = DAG(
    dag_id='emdeon_rx_post_matching_pipeline',
    schedule_interval=None,
    default_args=default_args
)

move_matching_payload = PythonOperator(
    task_id='move_matching_payload',
    provide_context=True,
    python_callable=do_move_matching_payload,
    dag=mdag
)

detect_matching_done = PythonOperator(
    task_id='detect_matching_done',
    provide_context=True,
    python_callable=do_detect_matching_done,
    execution_timeout=timedelta(hours=6),
    dag=mdag
)

create_redshift_cluster = BashOperator(
    task_id='create_redshift_cluster',
    bash_command=REDSHIFT_CREATE_COMMAND_TEMPLATE,
    params={"cluster_id" : RS_CLUSTER_ID, "num_nodes" : RS_NUM_NODES},
    dag=mdag
)

run_normalization_routine = PythonOperator(
    task_id='run_normalization_routine',
    provide_context=True,
    python_callable=do_run_normalization_routine,
    dag=mdag
)

delete_redshift_cluster = BashOperator(
    task_id='delete_redshift_cluster',
    bash_command=REDSHIFT_DELETE_COMMAND_TEMPLATE,
    params={"cluster_id" : RS_CLUSTER_ID},
    dag=mdag
)

create_emr_cluster = PythonOperator(
    task_id='create_emr_cluster',
    provide_context=True,
    python_callable=do_create_emr_cluster,
    dag=mdag
)

transform_to_parquet = PythonOperator(
    task_id='transform_to_parquet',
    provide_context=True,
    python_callable=do_transform_to_parquet,
    dag=mdag
)

delete_emr_cluster = PythonOperator(
    task_id='delete_emr_cluster',
    provide_context=True,
    python_callable=do_delete_emr_cluster,
    dag=mdag
)

move_matching_payload.set_upstream(detect_matching_done)
create_redshift_cluster.set_upstream(detect_matching_done)
run_normalization_routine.set_upstream([create_redshift_cluster, move_matching_payload])
delete_redshift_cluster.set_upstream(run_normalization_routine)
create_emr_cluster.set_upstream(run_normalization_routine)
transform_to_parquet.set_upstream(create_emr_cluster)
delete_emr_cluster.set_upstream(transform_to_parquet)
