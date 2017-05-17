import pytest

import mock
import util.s3_utils as s3_utils
import subdags.s3_fetch_file as s3_fetch_file

# global conf
filename_template = "TEST.csv"
ds = {}
kwargs = {
    # test input args
    's3_bucket': 'healthverity',
    's3_prefix': 'samplefeeds/',
    'tmp_path_template': 'dummypath/',
    'expected_file_name_func':
    lambda ds, k: filename_template,
    'new_file_name_func':
    lambda ds, k: filename_template + '_local',

    # kwargs conf
    'ds_nodash': '',
}


@pytest.fixture(autouse=True)
def setup_teardown():
    real_s3_utils_fetch_file_from_s3 = s3_utils.fetch_file_from_s3
    s3_utils.fetch_file_from_s3 = mock.MagicMock()

    yield

    s3_utils.fetch_file_from_s3 = real_s3_utils_fetch_file_from_s3


def test_func_call():
    """
    Ensure that the correct call was made into s3_utils
    """
    s3_fetch_file.do_fetch_file(ds, **kwargs)

    s3_utils.fetch_file_from_s3.assert_called_with(
        's3://healthverity/' + kwargs['s3_prefix']
        + filename_template,
        kwargs['tmp_path_template']
        + filename_template + '_local',
        s3_utils.DEFAULT_CONNECTION_ID
    )
