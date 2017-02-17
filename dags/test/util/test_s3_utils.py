import unittest

import os
import shutil
import airflow
import boto
import mock
import datetime
import dags.util.s3_utils as s3_utils


class TestS3Utils(unittest.TestCase):

    def setUp(self):
        self.test_bucket = 'healthveritydev'
        self.test_key = 'musifer/scratch/dewey-airflow-testing/'
        self.test_path = 's3://' + self.test_bucket + '/' + self.test_key
        self.test_bucket_contents = ['test_file1', 'test_file2']
        self.boto = boto.s3.connection.S3Connection().get_bucket(
            self.test_bucket
        )

        # save state of mocked objects
        self.real_s3_utils__get_s3_hook = s3_utils._get_s3_hook
        self.real_s3_utils_get_aws_env = s3_utils.get_aws_env

        # avoid looking for a connection string or variables in
        # airflow
        hook = airflow.hooks.S3_hook.S3Hook
        obj = mock.MagicMock()
        obj.extra_dejson = {}
        hook.get_connection = mock.MagicMock(return_value=obj)
        s3_utils._get_s3_hook = mock.MagicMock(return_value=hook())
        s3_utils.get_aws_env = mock.MagicMock(return_value=os.environ)

        # create scratch dir
        self.scratch_directory = os.path.dirname('./scratch/')
        try:
            os.stat(self.scratch_directory)
        except:
            os.mkdir(self.scratch_directory)

    def tearDown(self):
        # remove all files created in the scratch directory
        try:
            shutil.rmtree(self.scratch_directory)
        except Exception as e:
            print(e)

        # remove any new files in the s3 scratch directory
        for k in self.boto.list(prefix=self.test_key):
            if k.name.split('/')[-1] not in self.test_bucket_contents:
                self.boto.delete_key(k)

        # reset mocked objects
        s3_utils._get_s3_hook = self.real_s3_utils__get_s3_hook
        s3_utils.get_aws_env = self.real_s3_utils_get_aws_env

    def test_transform_path_to_bucket_key(self):
        """
        Ensure that a full s3 path is correctly split into a bucket and a
        key
        """
        bucket_key = s3_utils._transform_path_to_bucket_key(
            "s3://bucket/this/is/the/key"
        )
        self.assertEqual(bucket_key['bucket'], 'bucket')
        self.assertEqual(bucket_key['key'], 'this/is/the/key')

    def test_fetch_file_from_s3(self):
        """
        Ensure that files are fetched from s3
        """
        s3_utils.fetch_file_from_s3(
            self.test_path + self.test_bucket_contents[0],
            os.path.join(self.scratch_directory, self.test_bucket_contents[0])
        )

        self.assertTrue(
            self.test_bucket_contents[0]
            in os.listdir(self.scratch_directory)
        )

    def test_copy_file(self):
        """
        Test that files are copied in s3
        """
        s3_utils.copy_file(
            self.test_path + self.test_bucket_contents[0],
            self.test_path + self.test_bucket_contents[0] + '_copy'
        )

        # the copy should exist
        self.assertTrue(
            self.test_bucket_contents[0] + '_copy' in
            map(
                lambda p: p.name.split('/')[-1],
                self.boto.list(prefix=self.test_key)
            )
        )

        # the original file should still be there
        self.assertTrue(
            self.test_bucket_contents[0] in
            map(
                lambda p: p.name.split('/')[-1],
                self.boto.list(prefix=self.test_key)
            )
        )

    def test_list_bucket(self):
        """
        Test that bucket contents are listed
        """
        self.assertEqual(
            s3_utils.list_s3_bucket(self.test_path),
            map(
                lambda k: self.test_path + k,
                self.test_bucket_contents
            )
        )

    def test_list_bucket_files(self):
        """
        Test that bucket contents are listed
        """
        self.assertEqual(
            s3_utils.list_s3_bucket_files(self.test_path),
            self.test_bucket_contents
        )

    def test_get_file_size(self):
        """
        Test that file size is calculated correctly
        """
        self.assertEqual(
            s3_utils.get_file_size(
                self.test_path + self.test_bucket_contents[0]
            ),
            '0'
        )

    def test_s3_key_exists(self):
        self.assertTrue(
            s3_utils.s3_key_exists(
                self.test_path + self.test_bucket_contents[0]
            )
        )


if __name__ == '__main__':
    unittest.main()
