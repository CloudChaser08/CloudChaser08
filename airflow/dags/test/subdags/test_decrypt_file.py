import unittest

import mock
import airflow.models
import util.s3_utils as s3_utils
import subdags.decrypt_files as decrypt_files

reload(decrypt_files)


class TestDecryptFileSubDag(unittest.TestCase):

    def setUp(self):
        # global conf
        self.ds = {}
        self.kwargs = {
            # test input args
            'tmp_path_template': 'base/{}/',
            'ds_nodash': '20160101',
            'tmp_dir_func': lambda ds, k: k['tmp_path_template'].format(
                k['ds_nodash']
            )
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

        decrypt_files.do_fetch_decryption_files(self.ds, **self.kwargs)

        s3_utils.fetch_file_from_s3.assert_has_calls([
            mock.call(
                'location',
                'base/' + self.kwargs['ds_nodash']
                + '/' + decrypt_files.DECRYPTION_KEY
            ),
            mock.call(
                'location',
                'base/' + self.kwargs['ds_nodash']
                + '/' + decrypt_files.DECRYPTOR_JAR
            )
        ], any_order=True)


if __name__ == '__main__':
    unittest.main()
