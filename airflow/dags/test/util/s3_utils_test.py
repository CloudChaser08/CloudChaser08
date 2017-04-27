import pytest

import os
import shutil
import airflow
import boto
import mock
import util.s3_utils as s3_utils

test_bucket = 'healthveritydev'
test_key = 'musifer/scratch/dewey-airflow-testing/'
test_path = 's3://' + test_bucket + '/' + test_key
test_bucket_contents = ['test_file1', 'test_file2']
bucket = boto.s3.connection.S3Connection().get_bucket(
    test_bucket, validate=False
)

# save state of mocked objects
real_s3_utils__get_s3_hook = s3_utils._get_s3_hook
real_s3_utils_get_aws_env = s3_utils.get_aws_env

scratch_directory = os.path.dirname('./scratch/')


@pytest.fixture(autouse=True)
def clean_up():
    # avoid looking for a connection string or variables in
    # airflow
    hook = airflow.hooks.S3_hook.S3Hook
    obj = mock.MagicMock()
    obj.extra_dejson = {}
    hook.get_connection = mock.MagicMock(return_value=obj)
    s3_utils._get_s3_hook = mock.MagicMock(return_value=hook())
    s3_utils.get_aws_env = mock.MagicMock(return_value=os.environ)

    try:
        os.stat(scratch_directory)
    except:
        os.mkdir(scratch_directory)

    yield  # run test

    # after:
    # remove all files created in the scratch directory
    try:
        shutil.rmtree(scratch_directory)
    except Exception as e:
        print(e)

    # remove any new files in the s3 scratch directory
    for k in bucket.list(prefix=test_key):
        if k.name.split('/')[-1] not in test_bucket_contents:
            bucket.delete_key(k)

    # reset mocked objects
    s3_utils._get_s3_hook = real_s3_utils__get_s3_hook
    s3_utils.get_aws_env = real_s3_utils_get_aws_env


def test_transform_path_to_bucket_key():
    """
    Ensure that a full s3 path is correctly split into a bucket and a
    key
    """
    bucket_key = s3_utils._transform_path_to_bucket_key(
        "s3://bucket/this/is/the/key"
    )
    assert bucket_key['bucket'] == 'bucket'
    assert bucket_key['key'] == 'this/is/the/key'


def test_fetch_file_from_s3():
    """
    Ensure that files are fetched from s3
    """
    s3_utils.fetch_file_from_s3(
        test_path + test_bucket_contents[0],
        os.path.join(scratch_directory, test_bucket_contents[0])
    )

    assert test_bucket_contents[0] in os.listdir(scratch_directory)


def test_copy_file():
    """
    Test that files are copied in s3
    """
    s3_utils.copy_file(
        test_path + test_bucket_contents[0],
        test_path + test_bucket_contents[0] + '_copy'
    )

    # the copy should exist
    assert (
        test_bucket_contents[0] + '_copy' in
        map(
            lambda p: p.name.split('/')[-1],
            bucket.list(prefix=test_key)
        )
    )

    # the original file should still be there
    assert (
        test_bucket_contents[0] in
        map(
            lambda p: p.name.split('/')[-1],
            bucket.list(prefix=test_key)
        )
    )


def test_list_bucket():
    """
    Test that bucket contents are listed
    """
    assert s3_utils.list_s3_bucket(test_path) == map(
        lambda k: test_path + k,
        test_bucket_contents
    )


def test_list_bucket_files():
    """
    Test that bucket contents are listed
    """
    assert s3_utils.list_s3_bucket_files(test_path) == test_bucket_contents


def test_get_file_size():
    """
    Test that file size is calculated correctly
    """
    assert s3_utils.get_file_size(
        test_path + test_bucket_contents[0]
    ) == '0'


def test_s3_key_exists():
    assert s3_utils.s3_key_exists(
        test_path + test_bucket_contents[0]
    )
