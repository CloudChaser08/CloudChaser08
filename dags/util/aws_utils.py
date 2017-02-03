#
# Operators for interacting with AWS
#
import json
import time
import os
from subprocess import check_call, Popen, check_output
from airflow.models import Variable
import airflow.hooks.S3_hook


#
# S3
#
def _transform_path_to_bucket_key(path):
    return {
        'bucket': path[5:].split('/')[0],
        'key': '/'.join(path[5:].split('/')[1:])
    }


def _get_s3_hook():
    return airflow.hooks.S3_hook.S3Hook(s3_conn_id='my_conn_s3')


def fetch_file_from_s3(s3_path, local_path):
    """Download a file from S3"""
    check_call([
        'mkdir', '-p', local_path
    ])
    check_call([
        'aws', 's3', 'cp', s3_path, local_path
    ])


def copy_file(src_path, dest_path):
    check_call([
        'aws', 's3', 'cp', src_path, dest_path
    ])


def push_local_dir_to_s3(local_path, s3_path):
    """Push each file in a local directory up to a specified s3 location"""
    check_call([
        'aws', 's3', 'cp', '--recursive', local_path, s3_path
    ])


def list_s3_bucket(path):
    """
    Get a list of keys in an s3 path.
    This function expects a full url: s3://bucket/key/
    """
    bucket_key = _transform_path_to_bucket_key(path)
    return map(
        lambda k: 's3://' + bucket_key['bucket'] + '/' + k,
        _get_s3_hook().list_keys(bucket_key['bucket'], bucket_key['key'])
    )


def get_file_size(path):
    """
    Get the size of a file on s3
    """
    bucket_key = _transform_path_to_bucket_key(path)
    return _get_s3_hook().get_key(bucket_key['key'], bucket_key['bucket']).size


def get_file_last_modified(path):
    """
    Get the size of a file on s3
    """
    bucket_key = _transform_path_to_bucket_key(path)
    return _get_s3_hook().get_key(
        bucket_key('key'), bucket_key('bucket')
    ).last_modified


def s3_key_exists(path):
    """
    Get a list of keys in an s3 path.
    This function expects a full url: s3://bucket/key/
    """
    hook = airflow.hooks.S3_hook.S3Hook(s3_conn_id='my_conn_s3')
    return hook.check_for_wildcard_key(path, None)

#
# Redshift
#
REDSHIFT_HOST_URL_TEMPLATE = '{}.cz8slgfda3sg.us-east-1.redshift.amazonaws.com'
REDSHIFT_USER = 'hvuser'
REDSHIFT_DATABASE = 'dev'
REDSHIFT_PORT = '5439'


def get_rs_env(cluster_name):
    """
    Get an environ instance with aws perms
    and redshift variables set for given cluster name.
    """
    aws_env = dict(os.environ)
    aws_env['PGHOST'] = REDSHIFT_HOST_URL_TEMPLATE.format(cluster_name)
    aws_env['PGUSER'] = REDSHIFT_USER
    aws_env['PGDATABASE'] = REDSHIFT_DATABASE
    aws_env['PGPORT'] = REDSHIFT_PORT
    aws_env['PGPASSWORD'] = Variable.get('rs_norm_password')
    return aws_env


def get_rs_s3_credentials_str():
    return (
        'aws_access_key_id={};aws_secret_access_key={}'
    ).format(
        Variable.get('AWS_ACCESS_KEY_ID'),
        Variable.get('AWS_SECRET_ACCESS_KEY')
    )


def create_redshift_cluster(cluster_name, num_nodes):
    check_call([
        '/home/airflow/airflow/dags/resources/redshift.py', 'create',
        '--identifier', cluster_name, '--num_nodes', num_nodes
    ])


def run_rs_query_file(cluster_name, command, cwd):
    p = Popen(command, env=get_rs_env(cluster_name), cwd=cwd)
    p.wait()


#
# EMR
#
EMR_APPLICATIONS = "Name=Hadoop Name=Hive Name=Presto Name=Ganglia Name=Spark"
EMR_COPY_MELLON_STEP = (
    'Type=CUSTOM_JAR,Name="Copy Mellon",Jar="command-runner.jar",'
    'ActionOnFailure=CONTINUE,Args=['
    'aws,s3,cp,s3://healthverityreleases/mellon/mellon-assembly-latest.jar,'
    '/tmp/mellon-assembly-latest.jar'
    ']'
)
EMR_DISTCP_TO_S3 = (
    'Type=CUSTOM_JAR,Name="Distcp to S3",Jar="command-runner.jar",'
    'ActionOnFailure=CONTINUE,Args=[s3-dist-cp,"--src={}","--dest={}"]'
)


def _get_emr_cluster_id(cluster_name):
    clusters = json.loads(check_output([
        'aws', 'emr', 'list-clusters', '--active'
    ]))
    for cluster in clusters['Clusters']:
        if cluster['Name'] == cluster_name:
            return cluster['Id']
    print("Cluster not found. " + cluster_name)


def _wait_for_steps(cluster_id):
    incomplete_steps = 1
    failed_steps = 0
    while incomplete_steps > 0:
        incomplete_steps = 0
        time.sleep(60)
        cluster_steps = json.loads(check_output([
            'aws', 'emr', 'list-steps', '--cluster-id', cluster_id
        ]))
        for step in cluster_steps['Steps']:
            if step['Status']['State'] == "PENDING" \
               or step['Status']['State'] == "RUNNING":
                incomplete_steps += 1
            elif step['Status']['State'] == "FAILED":
                failed_steps += 1
    if failed_steps > 0:
        print("Step failed on cluster: " + cluster_id)


def create_emr_cluster(cluster_name, num_nodes, node_type, ebs_volume_size):
    """Create an EMR cluster"""
    cluster_details = json.loads(
        check_output([
            '/home/airflow/airflow/dags/resources/launchEMR',
            cluster_name, num_nodes, node_type, EMR_APPLICATIONS,
            "true" if (ebs_volume_size > 0) else "false", str(ebs_volume_size)
        ])
    )
    check_call([
        'aws', 'emr', 'wait', 'cluster-running',
        '--cluster-id', cluster_details['ClusterId']
    ])


def transform_to_parquet(cluster_name, src_file, dest_file, model):
    parquet_step = (
        'Type=Spark,Name="Transform to Parquet",ActionOnFailure=CONTINUE, '
        'Args=[--class,com.healthverity.parquet.Main,'
        '--conf,spark.sql.parquet.compression.codec=gzip,'
        '/tmp/mellon-assembly-latest.jar,{},{},{},hdfs:///parquet/,'
        '{},20,"|"]'
    ).format(
        Variable.get('AWS_ACCESS_KEY_ID'),
        Variable.get('AWS_SECRET_ACCESS_KEY'),
        model, src_file
    )
    cluster_id = _get_emr_cluster_id(cluster_name)
    check_call([
        'aws', 'emr', 'add-steps', '--cluster-id', cluster_id,
        '--steps', EMR_COPY_MELLON_STEP, parquet_step,
        EMR_DISTCP_TO_S3.format(
            "hdfs:///parquet/", dest_file
        )
    ])
    _wait_for_steps(cluster_id)


def delete_emr_cluster(cluster_name):
    cluster_id = _get_emr_cluster_id(cluster_name)
    check_call([
        'aws', 'emr', 'terminate-clusters', '--cluster-ids', cluster_id
    ])
