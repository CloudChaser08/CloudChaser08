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

class EMRStep:
    """An EMR cluster step"""

    def __init__(self, step_id, name, failed=False, incomplete=False):
        self.step_id = step_id
        self.name = name
        self.failed = failed
        self.incomplete = incomplete

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


def _get_cluster_steps_by_id(cluster_id):
    cluster_steps_raw_output = json.loads(check_output([
        'aws', 'emr', 'list-steps', '--cluster-id', cluster_id
    ]))

    steps = []

    for raw_step in cluster_steps_raw_output['Steps']:
        step = EMRStep(raw_step['Id'], raw_step['Name'])
        if raw_step['Status']['State'] == "PENDING" or raw_step['Status']['State'] == "RUNNING":
            step.incomplete = True
        elif raw_step['Status']['State'] == "FAILED":
            step.failed = True
        steps.append(step)

    return steps


def get_cluster_steps(cluster_name):
    return _get_cluster_steps_by_id(_get_emr_cluster_id(cluster_name))


def step_list_contains_failed_step(steps):
    if [s for s in steps if s.failed]:
        return True
    else:
        return False


def step_list_contains_incomplete_step(steps):
    if [s for s in steps if s.incomplete]:
        return True
    else:
        return False


def _wait_for_steps(cluster_id):
    cluster_steps = _get_cluster_steps_by_id(cluster_id)

    while step_list_contains_incomplete_step(cluster_steps):
        time.sleep(60)
        cluster_steps = _get_cluster_steps_by_id(cluster_id)

    if step_list_contains_failed_step(cluster_steps):
        raise Exception(
            "Steps failed on cluster {}: {}".format(
                cluster_id, ', '.join([s.name for s in cluster_steps if s.failed])
            )
        )


def create_emr_cluster(cluster_name, num_nodes, node_type, ebs_volume_size, purpose, connected_to_metastore=False, use_spot_bids=True):
    """Create an EMR cluster"""
    cluster_details = json.loads(
        check_output([
            '{}/dags/resources/launchEMR.py'.format(
                os.getenv('AIRFLOW_HOME')
            ),
            cluster_name, str(num_nodes), node_type, EMR_APPLICATIONS,
            "true" if (int(ebs_volume_size) > 0) else "false",
            str(ebs_volume_size),
            "true" if use_spot_bids else "false",
            "true" if connected_to_metastore else "false",
            purpose
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


def run_hive_queries(cluster_name, queries):
    query = '; '.join(queries)

    query_step = (
        'Type=Hive,Name="Hive Query",ActionOnFailure=CONTINUE,'
        'Args=[-e,"{}"]'
    ).format(query)

    cluster_id = _get_emr_cluster_id(cluster_name)
    check_call([
        'aws', 'emr', 'add-steps', '--cluster-id', cluster_id,
        '--steps', query_step
    ])
    _wait_for_steps(cluster_id)

def run_steps(cluster_name, steps):
    cluster_id = _get_emr_cluster_id(cluster_name)
    for step in steps:
        check_call([
            'aws', 'emr', 'add-steps', '--cluster-id', cluster_id,
            '--steps', step
        ])
    _wait_for_steps(cluster_id)

def run_script(cluster_name, script_name, args, spark_conf_args):
    """Run spark normalization script in EMR"""
    if spark_conf_args is None:
        spark_conf_args = []

    normalize_step = (
        'Type=Spark,Name="Normalize",ActionOnFailure=CONTINUE, '
        'Args=[--jars,'
        '"/home/hadoop/spark/common/json-serde-1.3.7-jar-with-dependencies.jar,'
        '/home/hadoop/spark/common/HiveJDBC41.jar",'
        '--py-files, /home/hadoop/spark/target/dewey.zip, {}]'
    ).format(
        ','.join(spark_conf_args + [script_name] + args)
    )
    _build_dewey(_get_emr_cluster_id(cluster_name))
    run_steps(cluster_name, [normalize_step])


def normalize(cluster_name, script_name, args, spark_conf_args=None):
    run_script(cluster_name, script_name, args, spark_conf_args)


def export(cluster_name, script_name, args, spark_conf_args=None):
    run_script(cluster_name, script_name, args, spark_conf_args)


def delete_emr_cluster(cluster_name):
    cluster_id = _get_emr_cluster_id(cluster_name)
    check_call([
        'aws', 'emr', 'terminate-clusters', '--cluster-ids', cluster_id
    ])
