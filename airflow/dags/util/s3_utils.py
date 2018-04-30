#
# Operators for interacting with S3
#
import os
from subprocess import check_call, Popen
from airflow.models import Variable
import airflow.hooks.S3_hook

DEFAULT_CONNECTION_ID = 'my_conn_s3'


def get_aws_env(suffix=None, prefix=None):
    """Get an environ instance with aws perms attached"""
    aws_env = dict(os.environ)
    if suffix or prefix:
        suffix = suffix or ""
        prefix = prefix or ""
        aws_env['AWS_ACCESS_KEY_ID'] = Variable.get(
            prefix + 'AWS_ACCESS_KEY_ID' + suffix
        )
        aws_env['AWS_SECRET_ACCESS_KEY'] = Variable.get(
            prefix + 'AWS_SECRET_ACCESS_KEY' + suffix
        )
    return aws_env


def _transform_path_to_bucket_key(path):
    return {
        'bucket': path[5:].split('/')[0],
        'key': '/'.join(path[5:].split('/')[1:])
    }


def _get_s3_hook(s3_connection_id=DEFAULT_CONNECTION_ID):
    return airflow.hooks.S3_hook.S3Hook(s3_conn_id=s3_connection_id)


def fetch_file_from_s3(
        s3_path, local_path, s3_connection_id=DEFAULT_CONNECTION_ID
):
    """Download a file from S3"""
    bucket_key = _transform_path_to_bucket_key(s3_path)
    key = _get_s3_hook(s3_connection_id).get_key(
        bucket_key['key'], bucket_key['bucket']
    )
    key.get_contents_to_filename(local_path)


def copy_file(src_path, dest_path, encrypt=True, env=get_aws_env()):
    cmd = ['aws', 's3', 'cp'] + \
        (['--sse', 'AES256'] if encrypt else []) + \
        [src_path, dest_path]

    check_call(cmd, env=env)


def copy_file_async(src_path, dest_path, encrypt=True, env=get_aws_env()):
    cmd = ['aws', 's3', 'cp'] + \
        (['--sse', 'AES256'] if encrypt else []) + \
        [src_path, dest_path]

    return Popen(cmd, env=env)


def delete_path(target_path, env=get_aws_env()):
    """
    This function will only remove files (not directories) one level deep
    """
    path = target_path if target_path.endswith('/') \
        else target_path + '/'
    if s3_key_exists(path + '*'):
        # filter out keys that are more than one level deeper than
        # input path
        for f in filter(
                lambda key: '/' not in key.replace(path, ''),
                list_s3_bucket(path)
        ):
            check_call([
                'aws', 's3', 'rm', f
            ], env=env)


def delete_path_recursive(target_path, env=get_aws_env()):
    '''
    This function will delete both files AND directories from a given s3 key
    '''
    path = target_path if target_path.endswith('/') else target_path + '/'
    if s3_key_exists(path + '*'):
        check_call(['aws', 's3', 'rm', '--recursive', target_path])


def move_path_recursive(source_path, target_path, env=get_aws_env()):
    '''
    This function will recursively move the contents of a source s3 path
    to a target s3 path
    '''
    s_path = source_path if source_path.endswith('/') else source_path + '/'
    t_path = target_path if target_path.endswith('/') else target_path + '/'
    if s3_key_exists(s_path + '*'):
        check_call(['aws', 's3', 'mv', '--recursive',
                    '--sse', 'AES256', s_path, t_path])
    else:
        raise Exception("S3 Key: {} does not exist".format(s_path))


def list_s3_bucket(path, s3_connection_id=DEFAULT_CONNECTION_ID):
    """
    Get a list of keys in an s3 path.
    This function expects a full url: s3://bucket/key/
    """
    bucket_key = _transform_path_to_bucket_key(path)
    return map(
        lambda k: 's3://' + bucket_key['bucket'] + '/' + k,
        _get_s3_hook(s3_connection_id).list_keys(
            bucket_key['bucket'], bucket_key['key']
        ) or []
    )


def list_s3_bucket_files(path, s3_connection_id=DEFAULT_CONNECTION_ID):
    """
    List just the filenames in the current path
    """
    return map(
        lambda x: x.replace(path, ''),
        list_s3_bucket(path, s3_connection_id)
    )


def get_file_size(path, s3_connection_id=DEFAULT_CONNECTION_ID):
    """
    Get the size of a file on s3
    """
    bucket_key = _transform_path_to_bucket_key(path)
    return int(_get_s3_hook(s3_connection_id).get_key(
        bucket_key['key'], bucket_key['bucket']
    ).content_length)


def s3_key_exists(path, s3_connection_id=DEFAULT_CONNECTION_ID):
    """
    Get a list of keys in an s3 path.
    This function expects a full url: s3://bucket/key/
    """
    return _get_s3_hook(s3_connection_id).check_for_wildcard_key(path, None)
