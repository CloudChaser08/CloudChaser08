import unittest

from mock import MagicMock
import dags.util.s3_utils as s3_utils
import dags.subdags.s3_validate_file as s3_validate_file


class TestFileValidatorSubDag(unittest.TestCase):

    def setUp(self):
        # global conf
        self.filename_template = "DUMMY_FILENAME_{}"
        self.ds = {}
        self.kwargs = {
            # test input args
            's3_prefix': 'dummyprefix/',
            'file_name_pattern_func':
            lambda ds, k: self.filename_template.format(
                '\d{4}'
            ),
            'expected_file_name_func':
            lambda ds, k: self.filename_template.format(
                self.kwargs['ds_nodash'][0:4]
            ),
            'minimum_file_size': 100,

            # kwargs conf
            'ds_nodash': '20170101',
            'is_new_valid': 'is_new_valid',
            'is_not_valid': 'is_not_valid',
            'is_not_new': 'is_not_new',
            'is_bad_name': 'is_bad_name'

        }

    def test_func_call(self):
        s3_utils.list_s3_bucket_files = MagicMock(return_value=[])
        s3_utils.get_file_size = MagicMock(return_value=0)

        # run the subdag
        s3_validate_file.do_is_valid_new_file(
            self.ds, **self.kwargs
        )

        # make sure the correct call was made
        s3_utils.list_s3_bucket_files.assert_called_with(
            's3://healthverity/' + self.kwargs['s3_prefix'],
            s3_utils.DEFAULT_CONNECTION_ID
        )

    def test_valid_file(self):
        """
        Ensure that valid files reach the 'is_new_valid' step
        """

        s3_utils.list_s3_bucket_files = MagicMock(return_value=[
            self.filename_template.format('2016'),
            self.filename_template.format('2017')  # this is our file
        ])
        s3_utils.get_file_size = MagicMock(return_value=200)

        # run the subdag
        out = s3_validate_file.do_is_valid_new_file(
            self.ds, **self.kwargs
        )

        self.assertEqual(out, 'is_new_valid')

    def test_invalid_file_bad_name(self):
        """
        Ensure that the 'is_bad_name' step is called if no file exists
        which matches the expected pattern
        """

        s3_utils.list_s3_bucket_files = MagicMock(return_value=[
            self.filename_template.format('BADVAL')
        ])
        s3_utils.get_file_size = MagicMock(return_value=0)

        # run the subdag
        out = s3_validate_file.do_is_valid_new_file(
            self.ds, **self.kwargs
        )

        self.assertEqual(out, 'is_bad_name')

    def test_invalid_file_not_new(self):
        """
        Ensure that the 'is_not_new' step is called if no file exists
        with the expected name
        """

        s3_utils.list_s3_bucket_files = MagicMock(return_value=[
            self.filename_template.format('2016')  # wrong year
        ])
        s3_utils.get_file_size = MagicMock(return_value=0)

        # run the subdag
        out = s3_validate_file.do_is_valid_new_file(
            self.ds, **self.kwargs
        )

        self.assertEqual(out, 'is_not_new')

    def test_invalid_file_bad_size(self):
        """
        Ensure that the 'is_not_valid' step is called if the file has an
        invalid size
        """

        s3_utils.list_s3_bucket_files = MagicMock(return_value=[
            self.filename_template.format('2017')
        ])
        s3_utils.get_file_size = MagicMock(return_value=99)  # expects >= 100

        # run the subdag
        out = s3_validate_file.do_is_valid_new_file(
            self.ds, **self.kwargs
        )

        self.assertEqual(out, 'is_not_valid')


if __name__ == '__main__':
    unittest.main()
