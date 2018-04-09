#
# Operators for interacting with Redshift
#
import os
from subprocess import check_call, Popen
from airflow.models import Variable


def get_aws_env(suffix=""):
    """Get an environ instance with aws perms attached"""
    aws_env = dict(os.environ)
    aws_env['AWS_ACCESS_KEY_ID'] = Variable.get(
        'AWS_ACCESS_KEY_ID' + suffix
    )
    aws_env['AWS_SECRET_ACCESS_KEY'] = Variable.get(
        'AWS_SECRET_ACCESS_KEY' + suffix
    )
    return aws_env

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
        '--identifier', cluster_name, '--num_nodes', str(num_nodes)
    ], env=get_rs_env(cluster_name))


def run_rs_query_file(cluster_name, command, cwd):
    p = Popen(command, env=get_rs_env(cluster_name), cwd=cwd)
    p.wait()


def delete_redshift_cluster(cluster_name):
    check_call([
        '/home/airflow/airflow/dags/resources/redshift.py', 'delete',
        '--identifier', cluster_name
    ], env=get_rs_env(cluster_name))
