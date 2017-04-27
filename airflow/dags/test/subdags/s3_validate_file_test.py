import pytest

import mock
import util.s3_utils as s3_utils
import subdags.s3_validate_file as s3_validate_file

# global conf
filename_template = "DUMMY_FILENAME_{}"
ds = {}
kwargs = {
    # test input args
    's3_bucket': 'healthverity',
    's3_prefix': 'dummyprefix/',
    'file_name_pattern_func':
    lambda ds, k: filename_template.format(
        '\d{4}'
    ),
    'expected_file_name_func':
    lambda ds, k: filename_template.format(
        kwargs['ds_nodash'][0:4]
    ),
    'minimum_file_size': 100,

    # kwargs conf
    'ds_nodash': '20170101',
    'is_new_valid': 'is_new_valid',
    'is_not_valid': 'is_not_valid',
    'is_not_new': 'is_not_new',
    'is_bad_name': 'is_bad_name'
}


@pytest.fixture(autouse=True)
def clean_up():

    real_s3_utils_list_s3_bucket_files = s3_utils.list_s3_bucket_files
    real_s3_utils_get_file_size = s3_utils.get_file_size

    yield

    s3_utils.list_s3_bucket_files = real_s3_utils_list_s3_bucket_files
    s3_utils.get_file_size = real_s3_utils_get_file_size


def test_func_call():
    s3_utils.list_s3_bucket_files = mock.MagicMock(return_value=[])
    s3_utils.get_file_size = mock.MagicMock(return_value=0)

    # run the subdag
    s3_validate_file.do_is_valid_new_file(
        ds, **kwargs
    )

    # make sure the correct call was made
    s3_utils.list_s3_bucket_files.assert_called_with(
        's3://healthverity/' + kwargs['s3_prefix'],
        s3_utils.DEFAULT_CONNECTION_ID
    )


def test_valid_file():
    """
    Ensure that valid files reach the 'is_new_valid' step
    """

    s3_utils.list_s3_bucket_files = mock.MagicMock(return_value=[
        filename_template.format('2016'),
        filename_template.format('2017')  # this is our file
    ])
    s3_utils.get_file_size = mock.MagicMock(return_value=200)

    # run the subdag
    out = s3_validate_file.do_is_valid_new_file(
        ds, **kwargs
    )

    assert out == 'is_new_valid'


def test_invalid_file_bad_name():
    """
    Ensure that the 'is_bad_name' step is called if no file exists
    which matches the expected pattern
    """

    s3_utils.list_s3_bucket_files = mock.MagicMock(return_value=[
        filename_template.format('BADVAL')
    ])
    s3_utils.get_file_size = mock.MagicMock(return_value=0)

    # run the subdag
    out = s3_validate_file.do_is_valid_new_file(
        ds, **kwargs
    )

    assert out == 'is_bad_name'


def test_invalid_file_not_new():
    """
    Ensure that the 'is_not_new' step is called if no file exists
    with the expected name
    """

    s3_utils.list_s3_bucket_files = mock.MagicMock(return_value=[
        filename_template.format('2016')  # wrong year
    ])
    s3_utils.get_file_size = mock.MagicMock(return_value=0)

    # run the subdag
    out = s3_validate_file.do_is_valid_new_file(
        ds, **kwargs
    )

    assert out == 'is_not_new'


def test_invalid_file_bad_size():
    """
    Ensure that the 'is_not_valid' step is called if the file has an
    invalid size
    """

    s3_utils.list_s3_bucket_files = mock.MagicMock(return_value=[
        filename_template.format('2017')
    ])
    s3_utils.get_file_size = mock.MagicMock(return_value=99)  # expects >= 100

    # run the subdag
    out = s3_validate_file.do_is_valid_new_file(
        ds, **kwargs
    )

    assert out == 'is_not_valid'
