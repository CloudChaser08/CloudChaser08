from airflow import DAG
from airflow.models import Variable
from airflow.operators import *
from datetime import timedelta
from subprocess import check_output, check_call, Popen
import logging
import os
import time
import json

import util.s3_utils as s3_utils
import util.emr_utils as emr_utils
reload(s3_utils)
reload(emr_utils)

REDSHIFT_CREATE_COMMAND_TEMPLATE = """/home/airflow/airflow/dags/resources/redshift.py create \\
--identifier {{ params.cluster_id }} --num_nodes {{ params.num_nodes }}"""
REDSHIFT_DELETE_COMMAND_TEMPLATE = """/home/airflow/airflow/dags/resources/redshift.py delete \\
--identifier {{ params.cluster_id }}"""
S3_PREFIX='matching/prod/payload/'
S3_PATH_PREFIX='s3://salusv/' + S3_PREFIX
EMR_COPY_MELLON_STEP = ('Type=CUSTOM_JAR,Name="Copy Mellon",Jar="command-runner.jar",'
    'ActionOnFailure=CONTINUE,Args=[aws,s3,cp,s3://healthverityreleases/mellon/mellon-assembly-latest.jar,'
    '/tmp/mellon-assembly-latest.jar]')
EMR_TRANSFORM_TO_PARQUET_STEP = ('Type=Spark,Name="Transform {} to Parquet",ActionOnFailure=CONTINUE, '
    'Args=[--class,com.healthverity.parquet.Main,--conf,spark.sql.parquet.compression.codec=gzip,'
    '/tmp/mellon-assembly-latest.jar,{},{},{},hdfs:///parquet/{},'
    's3a://salusv/{}{},20,"|","false","false"]')
EMR_DELETE_OLD_PARQUET = ('Type=CUSTOM_JAR,Name="Delete old data from S3",Jar="command-runner.jar",'
    'ActionOnFailure=CONTINUE,Args=[aws,s3,rm,--recursive,s3://salusv/{}{}]')
EMR_DISTCP_TO_S3 = ('Type=CUSTOM_JAR,Name="Distcp to S3",Jar="command-runner.jar",' 
    'ActionOnFailure=CONTINUE,Args=[s3-dist-cp,"--src=hdfs:///parquet",'
    '"--dest=s3a://salusv/{}"]')
RS_HOST_TLD = '.cz8slgfda3sg.us-east-1.redshift.amazonaws.com'
RS_USER='hvuser'
RS_DATABASE='dev'
RS_PORT='5439'
RS_NUM_NODES=10
EMR_CLUSTER_NAME="normalization-cluster"
EMR_NUM_NODES='5'
EMR_NODE_TYPE="c4.xlarge"
EMR_APPLICATIONS="Name=Hadoop Name=Hive Name=Presto Name=Ganglia Name=Spark"
EMR_USE_EBS="false"
EMR_EBS_VOLUME_SIZE="0"

def do_detect_matching_done(ds, **kwargs):
    hook = airflow.hooks.S3_hook.S3Hook(s3_conn_id='my_conn_s3')
    deid_files = kwargs['expected_matching_files_func'](ds, kwargs)
    s3_path_prefix = S3_PATH_PREFIX + kwargs['vendor_uuid'] + '/'
    template = '{}{}*DONE*'
    for deid_file in deid_files:
        s3_key = template.format(s3_path_prefix, deid_file)
        logging.info('Poking for key : {}'.format(s3_key))
        while not s3_utils.s3_key_exists(s3_key):
            time.sleep(60)

def do_move_matching_payload(ds, **kwargs):
    hook = airflow.hooks.S3_hook.S3Hook(s3_conn_id='my_conn_s3')
    deid_files = kwargs['expected_matching_files_func'](ds, kwargs)
    vendor_uuid = kwargs['vendor_uuid']
    for deid_file in deid_files:
        s3_prefix = S3_PREFIX + vendor_uuid + '/' + deid_file
        for payload_file in hook.list_keys('salusv', s3_prefix):
            date = kwargs['file_date_func'](ds, kwargs)
            s3_utils.copy_file('s3://salusv/' + payload_file, kwargs['s3_payload_loc_url'] + date + '/' + payload_file.split('/')[-1])

def do_run_pyspark_normalization_routine(ds, **kwargs):
    emr_utils.normalize(
        EMR_CLUSTER_NAME + '-' + kwargs['vendor_uuid'],
        kwargs['pyspark_normalization_script_name'],
        kwargs['pyspark_normalization_args_func'](ds, kwargs),
        kwargs['text_warehouse'], kwargs['parquet_warehouse'],
        kwargs['part_file_prefix_func'](ds, kwargs), kwargs['model']
    )

def do_run_redshift_normalization_routine(ds, **kwargs):
    hook = airflow.hooks.S3_hook.S3Hook(s3_conn_id='my_conn_s3')
    file_date = kwargs['file_date_func'](ds, kwargs)
    s3_key = hook.list_keys('salusv', kwargs['incoming_path'] + file_date.replace('-', '/') + '/')[0]
    setid = s3_key.split('/')[-1].replace('.bz2','')[0:-3]
    s3_credentials = 'aws_access_key_id={};aws_secret_access_key={}'.format(
                         Variable.get('AWS_ACCESS_KEY_ID'), Variable.get('AWS_SECRET_ACCESS_KEY')
                     )
    command = [kwargs['normalization_routine_script'], '--date', file_date, '--setid', setid,
            '--s3_credentials', s3_credentials, '--first_run']

    env = dict(os.environ)
    env['PGHOST'] = 'norm-' + kwargs['vendor_uuid'] + RS_HOST_TLD
    env['PGUSER'] = RS_USER
    env['PGDATABASE'] = RS_DATABASE
    env['PGPORT'] = RS_PORT
    env['PGPASSWORD'] = Variable.get('rs_norm_password')
    cwd = kwargs['normalization_routine_directory']
    p = Popen(command, env=env, cwd=cwd)
    p.wait()

def get_emr_cluster_id(cluster_name):
    clusters = json.loads(check_output(['aws', 'emr', 'list-clusters', '--active']))
    for cluster in clusters['Clusters']:
        if cluster['Name'] == cluster_name:
            return cluster['Id']

def do_transform_to_parquet(ds, **kwargs):
    file_date = ds
    cluster_id = get_emr_cluster_id(EMR_CLUSTER_NAME + '-' + kwargs['vendor_uuid'])
    transform_steps = []
    delete_steps = []
    for d in kwargs['parquet_dates_func'](ds, kwargs):
        transform_steps.append(EMR_TRANSFORM_TO_PARQUET_STEP.format(kwargs['vendor_description'],
            Variable.get('AWS_ACCESS_KEY_ID'),Variable.get('AWS_SECRET_ACCESS_KEY'),kwargs['feed_data_type'],
            d,kwargs['s3_text_path_prefix'],d))
        delete_steps.append(EMR_DELETE_OLD_PARQUET.format(kwargs['s3_parquet_path_prefix'],d))
    command = ['aws', 'emr', 'add-steps', '--cluster-id', cluster_id,
                  '--steps', EMR_COPY_MELLON_STEP] + \
              transform_steps + \
              delete_steps + \
              [EMR_DISTCP_TO_S3.format(kwargs['s3_parquet_path_prefix'])]
    logging.info(command)
    check_call(command)
                
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
    emr_utils.create_emr_cluster(
        EMR_CLUSTER_NAME + '-' + kwargs['vendor_uuid'],
        EMR_NUM_NODES, EMR_NODE_TYPE, EMR_EBS_VOLUME_SIZE
    )

def do_delete_emr_cluster(ds, **kwargs):
    cluster_id = get_emr_cluster_id(EMR_CLUSTER_NAME + '-' + kwargs['vendor_uuid'])
    check_call(['aws', 'emr', 'terminate-clusters', '--cluster-ids', cluster_id])

def detect_move_normalize(parent_dag_name, child_dag_name, start_date, schedule_interval, dag_config):
    default_args = {
        'owner': 'airflow',
        'depends_on_past': False,
        'retries': 0
    }

    dag = DAG(
        '{}.{}'.format(parent_dag_name, child_dag_name),
        schedule_interval='@daily',
        start_date=start_date,
        default_args=default_args
    )

    move_matching_payload = PythonOperator(
        task_id='move_matching_payload',
        provide_context=True,
        python_callable=do_move_matching_payload,
        op_kwargs=dag_config,
        dag=dag
    )

    detect_matching_done = PythonOperator(
        task_id='detect_matching_done',
        provide_context=True,
        python_callable=do_detect_matching_done,
        execution_timeout=timedelta(hours=6),
        op_kwargs=dag_config,
        dag=dag
    )

    env = dict(os.environ)
    env['AWS_ACCESS_KEY_ID'] = Variable.get('AWS_ACCESS_KEY_ID')
    env['AWS_SECRET_ACCESS_KEY'] = Variable.get('AWS_SECRET_ACCESS_KEY')

    create_emr_cluster = PythonOperator(
        task_id='create_emr_cluster',
        provide_context=True,
        python_callable=do_create_emr_cluster,
        op_kwargs=dag_config,
        dag=dag
    )

    delete_emr_cluster = PythonOperator(
        task_id='delete_emr_cluster',
        provide_context=True,
        python_callable=do_delete_emr_cluster,
        op_kwargs=dag_config,
        dag=dag
    )

    if dag_config.get('pyspark', False):
        run_normalization_routine = PythonOperator(
            task_id='run_normalization_routine',
            provide_context=True,
            python_callable=do_run_pyspark_normalization_routine,
            op_kwargs=dag_config,
            dag=dag
        )

        move_matching_payload.set_upstream(detect_matching_done)
        create_emr_cluster.set_upstream(detect_matching_done)
        run_normalization_routine.set_upstream([create_emr_cluster, move_matching_payload])
        delete_emr_cluster.set_upstream(run_normalization_routine)
    else:
        create_redshift_cluster = BashOperator(
            task_id='create_redshift_cluster',
            bash_command=REDSHIFT_CREATE_COMMAND_TEMPLATE,
            params={"cluster_id" : 'norm-' + dag_config['vendor_uuid'], "num_nodes" : RS_NUM_NODES},
            dag=dag
        )

        run_normalization_routine = PythonOperator(
            task_id='run_normalization_routine',
            provide_context=True,
            python_callable=do_run_redshift_normalization_routine,
            op_kwargs=dag_config,
            dag=dag
        )

        delete_redshift_cluster = BashOperator(
            task_id='delete_redshift_cluster',
            bash_command=REDSHIFT_DELETE_COMMAND_TEMPLATE,
            params={"cluster_id" : 'norm-' + dag_config['vendor_uuid']},
            dag=dag
        )

        transform_to_parquet = PythonOperator(
            task_id='transform_to_parquet',
            provide_context=True,
            python_callable=do_transform_to_parquet,
            op_kwargs=dag_config,
            dag=dag
        )

        move_matching_payload.set_upstream(detect_matching_done)
        create_redshift_cluster.set_upstream(detect_matching_done)
        run_normalization_routine.set_upstream([create_redshift_cluster, move_matching_payload])
        delete_redshift_cluster.set_upstream(run_normalization_routine)
        create_emr_cluster.set_upstream(run_normalization_routine)
        transform_to_parquet.set_upstream(create_emr_cluster)
        delete_emr_cluster.set_upstream(transform_to_parquet)

    return dag
