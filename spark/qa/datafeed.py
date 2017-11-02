import pytest

import spark.qa.conf as qa_conf
import spark.helpers.file_utils as file_utils

# this will be set when a Datafeed instance is created. This instance
# is used as a fixture for pytest tests.
active_datafeed = None

class Datafeed:
    """
    A datafeed instance on which to run checks. This datafeed will
    contain all attributes that are required for testing.
    """

    def __init__(
            self,
            datatype,                                        # datatype
            source_data=None,                                # map of table name to a dataframe representing the data for that table
            target_data=None,                                # dataframe containing the normalized data
            source_data_hvid_full_name=None,                 # name of the hvid in the source data - in the format table_name.column_name
            source_data_claim_full_name=None,                # name of the claim_id in the source_data - in the format table_name.column_name
            source_data_service_line_full_name=None,         # name of the service_line_id in the source_data - in the format table_name.column_name
            target_data_hvid_column_name='hvid',             # name of hvid column in target_data
            target_data_claim_column_name=None,              # name of claim_id column in target_data
            target_data_service_line_column_name=None,       # name of service_line_id column in target_data
            target_data_gender_column_name='patient_gender'  # name of service_line_id column in target_data
    ):

        # set this instance as the active datafeed
        global active_datafeed
        active_datafeed = self

        # properties
        self.datatype = datatype
        self.source_data = source_data
        self.target_data = target_data

        # parse out table/column names from full names
        if source_data_claim_full_name:
            self.source_data_claim_table_name = source_data_claim_full_name.split('.')[0]
            self.source_data_claim_column_name = source_data_claim_full_name.split('.')[1]
        else:
            self.source_data_claim_table_name = None
            self.source_data_claim_column_name = None

        if source_data_service_line_full_name:
            self.source_data_service_line_table_name = source_data_service_line_full_name.split('.')[0]
            self.source_data_service_line_column_name = source_data_service_line_full_name.split('.')[1]
        else:
            self.source_data_service_line_table_name = None
            self.source_data_service_line_column_name = None

        if source_data_hvid_full_name:
            self.source_data_hvid_table_name = source_data_hvid_full_name.split('.')[0]
            self.source_data_hvid_column_name = source_data_hvid_full_name.split('.')[1]
        else:
            self.source_data_hvid_table_name = None
            self.source_data_hvid_column_name = None

        self.target_data_hvid_column_name = target_data_hvid_column_name
        self.target_data_claim_column_name = target_data_claim_column_name
        self.target_data_service_line_column_name = target_data_service_line_column_name
        self.target_data_gender_column_name = target_data_gender_column_name

    def run_checks(self):
        """
        Kick off a pytest session that will run all checks relevant to
        this datafeed.
        """
        pytest.main([
            '-v',

            # only run tests that apply to this datatype
            '-k', '{}'.format(' or '.join(qa_conf.datatype_config[self.datatype])),

            # only look at tests in the 'checks' dir
            '{}'.format(file_utils.get_abs_path(__file__, './checks/')),

            # summarize passed, failed and skipped tests
            '-r', 'p f s'
        ])
