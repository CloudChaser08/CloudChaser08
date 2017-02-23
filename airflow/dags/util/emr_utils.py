#
# Operators for interacting with EMR
#
import json
import time
import os
from subprocess import check_call, check_output
from airflow.models import Variable

#
# EMR
#
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
    env = dict(os.environ)
    parquet_step = (
        'Type=Spark,Name="Transform to Parquet",ActionOnFailure=CONTINUE, '
        'Args=[--class,com.healthverity.parquet.Main,'
        '--conf,spark.sql.parquet.compression.codec=gzip,'
        '/tmp/mellon-assembly-latest.jar,{},{},{},hdfs:///parquet/,'
        '{},20,"|"]'
    ).format(
        env['AWS_ACCESS_KEY_ID'],
        env['AWS_SECRET_ACCESS_KEY'],
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
