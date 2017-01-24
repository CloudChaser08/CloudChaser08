#
# Operators for interacting with S3
#
import os
from subprocess import check_call
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


def _transform_path_to_bucket_key(path):
    return {
        'bucket': path[5:].split('/')[0],
        'key': '/'.join(path[5:].split('/')[1:])
    }


def _get_s3_hook():
    return airflow.hooks.S3_hook.S3Hook(s3_conn_id='my_conn_s3')


def fetch_file_from_s3(s3_path, local_path):
    """Download a file from S3"""
    bucket_key = _transform_path_to_bucket_key(s3_path)
    key = _get_s3_hook().get_key(bucket_key['key'], bucket_key['bucket'])
    key.get_contents_to_filename(local_path)


def copy_file(src_path, dest_path):
    check_call([
        'aws', 's3', 'cp', src_path, dest_path
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
    return _get_s3_hook().get_key(bucket_key['key'], bucket_key['bucket']).content_length


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
