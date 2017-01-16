#
# Operators for interacting with AWS
#
import os
from subprocess import check_call, Popen
from airflow.models import Variable
import airflow.hooks.S3_hook


def get_aws_env(suffix=""):
    """Get an environ instance with aws perms attached"""
    aws_env = os.environ
    aws_env['AWS_ACCESS_KEY_ID'] = Variable.get(
        'AWS_ACCESS_KEY_ID' + suffix
    )
    aws_env['AWS_SECRET_ACCESS_KEY'] = Variable.get(
        'AWS_SECRET_ACCESS_KEY' + suffix
    )
    return aws_env

#
# S3
#
def fetch_file_from_s3(s3_path, local_path):
    """Download a file from S3"""
    check_call([
        'aws', 's3', 'cp', s3_path, local_path
    ], env=get_aws_env())


def push_local_dir_to_s3(local_path, s3_path):
    """Push each file in a local directory up to a specified s3 location"""
    check_call([
        'aws', 's3', 'cp', '--recursive', local_path, s3_path
    ], env=get_aws_env())


def list_s3_bucket(path):
    """
    Get a list of keys in an s3 path.
    This function expects a full url: s3://bucket/key/
    """
    bucket = path[5:].split('/')[0]
    key = '/'.join(path[5:].split('/')[1:])
    hook = airflow.hooks.S3_hook.S3Hook(s3_conn_id='my_conn_s3')
    return hook.list_keys(bucket, key)


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
    aws_env = get_aws_env()
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
    ], env=get_aws_env())


def run_rs_query_file(cluster_name, command, cwd):
    p = Popen(command, env=get_rs_env(cluster_name), cwd=cwd)
    p.wait()


#
# EMR
#
EMR_APPLICATIONS = "Name=Hadoop Name=Hive Name=Presto Name=Ganglia Name=Spark"


def create_emr_cluster(cluster_name, num_nodes, node_type, ebs_volume_size):
    """Create an EMR cluster"""
    check_call([
        '/home/airflow/airflow/dags/resources/launchEMR',
        cluster_name, num_nodes, node_type, '"' + EMR_APPLICATIONS + '"',
        (ebs_volume_size > 0), ebs_volume_size
    ], env=get_aws_env())
