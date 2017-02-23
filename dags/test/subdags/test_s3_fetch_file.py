import unittest

import mock
import dags.util.s3_utils as s3_utils
import dags.subdags.s3_fetch_file as s3_fetch_file


class TestS3FetchFileSubDag(unittest.TestCase):

    def setUp(self):
        # global conf
        self.filename_template = "TEST.csv"
        self.ds = {}
        self.kwargs = {
            # test input args
            's3_prefix': 'samplefeeds/',
            'tmp_path_template': 'dummypath/',
            'expected_file_name_func':
            lambda ds, k: self.filename_template,
            'new_file_name_func':
            lambda ds, k: self.filename_template + '_local',

            # kwargs conf
            'ds_nodash': '',
        }

        # save the state of s3_utils before mocking to prevent mocks
        # bleeding into other tests
        self.real_s3_utils_fetch_file_from_s3 = s3_utils.fetch_file_from_s3

    def tearDown(self):
        s3_utils.fetch_file_from_s3 = self.real_s3_utils_fetch_file_from_s3

    def test_func_call(self):
        """
        Ensure that the correct call was made into s3_utils
        """

        s3_utils.fetch_file_from_s3 = mock.MagicMock()

        s3_fetch_file.do_fetch_file(self.ds, **self.kwargs)

        s3_utils.fetch_file_from_s3.assert_called_with(
            's3://healthverity/' + self.kwargs['s3_prefix']
            + self.filename_template,
            self.kwargs['tmp_path_template']
            + self.filename_template + '_local',
            s3_utils.DEFAULT_CONNECTION_ID
        )


if __name__ == '__main__':
    unittest.main()
