#
# Operators for interacting with EMR
#
import json
import time
import os
from subprocess import check_call, check_output
from airflow.models import Variable
import util.s3_utils as s3_utils

reload(s3_utils)


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


EMR_APPLICATIONS = 'Name=Hadoop Name=Hive Name=Presto Name=Ganglia Name=Spark'
EMR_DISTCP_TO_S3 = (
    'Type=CUSTOM_JAR,Name="Distcp to S3",Jar="command-runner.jar",'
    'ActionOnFailure=CONTINUE,Args=[s3-dist-cp,"--src={}","--dest={}",'
    '"--s3ServerSideEncryption"]'
)


def _get_emr_cluster_id(cluster_name):
    clusters = json.loads(check_output([
        'aws', 'emr', 'list-clusters', '--active'
    ], env=get_aws_env()))
    for cluster in clusters['Clusters']:
        if cluster['Name'] == cluster_name:
            return cluster['Id']
    print("Cluster not found. " + cluster_name)


def _get_emr_cluster_ip_address(cluster_id):
    return '.'.join(
        (
            json.loads(check_output([
                'aws', 'emr', 'describe-cluster', '--cluster-id', cluster_id
            ]))['Cluster']['MasterPublicDnsName']
        ).split('.')[0].split('-')[1:]
    )


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
            '{}/dags/resources/launchEMR'.format(
                os.getenv('AIRFLOW_HOME')
            ),
            cluster_name, num_nodes, node_type, EMR_APPLICATIONS,
            "true" if (int(ebs_volume_size) > 0) else "false",
            str(ebs_volume_size)
        ])
    )
    check_call([
        'aws', 'emr', 'wait', 'cluster-running',
        '--cluster-id', cluster_details['ClusterId']
    ])


#
# Normalize
#
def _build_dewey(cluster_id):
    spark_dir = os.path.abspath(
        os.path.join(
            os.getenv('HOME'), 'spark/'
        )
    )
    check_call([
        'make', 'build-notest'
    ], cwd=spark_dir)
    check_call([
        'scp', '-i', os.getenv('HOME') + '/.ssh/emr_deployer',
        '-o', 'StrictHostKeyChecking no', '-r', '.',
        'hadoop@' + _get_emr_cluster_ip_address(cluster_id) + ':spark/'
    ], cwd=spark_dir)


def normalize(cluster_name, script_name, args,
              s3_text_warehouse, s3_parquet_warehouse, model):
    """Run normalization and parquet processes in EMR"""
    text_staging_dir = 'hdfs:///text-out/'

    normalize_step = (
        'Type=Spark,Name="Normalize",ActionOnFailure=CONTINUE, '
        'Args=[--jars,'
        '/home/hadoop/spark/common/json-serde-1.3.7-jar-with-dependencies.jar,'
        '--py-files, /home/hadoop/spark/target/dewey.zip, {}]'
    ).format(
        script_name + ',' + ','.join(args)
    )
    cluster_id = _get_emr_cluster_id(cluster_name)
    _build_dewey(cluster_id)
    check_call([
        'aws', 'emr', 'add-steps', '--cluster-id', cluster_id,
        '--steps', normalize_step
    ])
    _wait_for_steps(cluster_id)

    # get directories that will need to be transformed to parquet by
    # listing the directories created in hdfs by
    # normalized_records_unloader
    all_directories = check_output(' '.join([
        'ssh', '-i', '~/.ssh/emr_deployer',
        '-o', '"StrictHostKeyChecking no"',
        'hadoop@' + _get_emr_cluster_ip_address(cluster_id),
        'hdfs', 'dfs', '-ls', '-R', text_staging_dir, '|', 'rev', '|',
        'cut', '-d/', '-f1', '|', 'rev'
    ]), shell=True).split('\n')

    modified_dates = filter(
        lambda path: 'part_best_date' in path,
        all_directories
    )
    provider = filter(
        lambda path: 'part_provider' in path,
        all_directories
    )[0]

    for directory in modified_dates:
        _transform_to_parquet(
            cluster_name, s3_text_warehouse + provider + '/' + directory,
            s3_parquet_warehouse + provider + '/' + directory,
            model
        )


#
# Parquet
#
EMR_COPY_MELLON_STEP = (
    'Type=CUSTOM_JAR,Name="Copy Mellon",Jar="command-runner.jar",'
    'ActionOnFailure=CONTINUE,Args=['
    'aws,s3,cp,s3://healthverityreleases/mellon/mellon-assembly-latest.jar,'
    '/tmp/mellon-assembly-latest.jar'
    ']'
)


def _transform_to_parquet(cluster_name, src_file, dest_file, model):
    env = dict(os.environ)

    # remove target directory
    s3_utils.delete_path(dest_file)

    parquet_step = (
        'Type=Spark,Name="Transform to Parquet",ActionOnFailure=CONTINUE, '
        'Args=[--class,com.healthverity.parquet.Main,'
        '--conf,spark.sql.parquet.compression.codec=gzip,'
        '--conf,spark.executor.memory=10G,'
        '--conf,spark.executor.cores=4,'
        '--conf,spark.executor.instances=5,'
        '/tmp/mellon-assembly-latest.jar,{},{},{},{},'
        '{},20,"|","true","true"]'
    ).format(
        env['AWS_ACCESS_KEY_ID'],
        env['AWS_SECRET_ACCESS_KEY'],
        model, dest_file, src_file
    )
    cluster_id = _get_emr_cluster_id(cluster_name)
    check_call([
        'aws', 'emr', 'add-steps', '--cluster-id', cluster_id,
        '--steps', EMR_COPY_MELLON_STEP, parquet_step
    ])
    _wait_for_steps(cluster_id)


def delete_emr_cluster(cluster_name):
    cluster_id = _get_emr_cluster_id(cluster_name)
    check_call([
        'aws', 'emr', 'terminate-clusters', '--cluster-ids', cluster_id
    ])
