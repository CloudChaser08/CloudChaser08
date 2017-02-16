from mock import MagicMock
import dags.util.s3_utils as s3_utils


def test_is_valid_new_file():
    s3_utils.list_s3_bucket_files = MagicMock(return_value=['k1', 'k2'])

    s3_utils.list_s3_bucket_files('s3://')

    s3_utils.list_s3_bucket_files.assert_called_with('s3://')
