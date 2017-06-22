import pytest

import mock
import airflow.models
import util.s3_utils as s3_utils
import subdags.decrypt_files as decrypt_files

ds = {}
kwargs = {
    # test input args
    'tmp_path_template': 'base/{}/',
    'ds_nodash': '20160101',
    'tmp_dir_func': lambda ds, k: k['tmp_path_template'].format(
        k['ds_nodash']
    )
}

real_s3_utils_fetch_file_from_s3 = s3_utils.fetch_file_from_s3
real_airflow_models_Variable_get = airflow.models.Variable.get


@pytest.fixture(autouse=True)
def setup_teardown():

    yield  # run test

    # reset mocked objects
    s3_utils.fetch_file_from_s3 = real_s3_utils_fetch_file_from_s3
    airflow.models.Variable.get = real_airflow_models_Variable_get


def test_func_call():
    """
    Ensure that the correct call was made into s3_utils
    """
    airflow.models.Variable.get = mock.MagicMock(return_value='location')
    s3_utils.fetch_file_from_s3 = mock.MagicMock()

    decrypt_files.do_fetch_decryption_files(ds, **kwargs)

    s3_utils.fetch_file_from_s3.assert_has_calls([
        mock.call(
            'location',
            'base/' + kwargs['ds_nodash']
            + '/' + decrypt_files.DECRYPTION_KEY
        ),
        mock.call(
            'location',
            'base/' + kwargs['ds_nodash']
            + '/' + decrypt_files.DECRYPTOR_JAR
        )
    ], any_order=True)
