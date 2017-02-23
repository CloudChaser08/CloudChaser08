import unittest

import mock
import airflow.models
import dags.util.s3_utils as s3_utils
import dags.subdags.decrypt_file as decrypt_file


class TestDecryptFileSubDag(unittest.TestCase):

    def setUp(self):
        # global conf
        self.ds = {}
        self.kwargs = {
            # test input args
            'tmp_path_template': 'base/'
        }
        # save the state of s3_utils before mocking to prevent mocks
        # bleeding into other tests
        self.real_s3_utils_fetch_file_from_s3 = s3_utils.fetch_file_from_s3
        self.real_variable_get = airflow.models.Variable.get

        airflow.models.Variable.get = mock.MagicMock(return_value='location')

    def tearDown(self):
        s3_utils.fetch_file_from_s3 = self.real_s3_utils_fetch_file_from_s3
        airflow.models.Variable.get = self.real_variable_get

    def test_func_call(self):
        """
        Ensure that the correct call was made into s3_utils
        """

        s3_utils.fetch_file_from_s3 = mock.MagicMock()

        decrypt_file.do_fetch_decryption_files(self.ds, **self.kwargs)

        s3_utils.fetch_file_from_s3.assert_called_with(
            'location', 'base/'
        )


if __name__ == '__main__':
    unittest.main()
