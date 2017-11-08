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

            # datatype
            datatype,

            # map of table name to a dataframe representing the data for that table
            source_data=None,

            # dataframe containing the normalized data
            target_data=None,

            # name of the hvid in the source data - in the format table_name.column_name
            source_data_hvid_full_name=None,

            # name of the claim_id in the source_data - in the format table_name.column_name
            source_data_claim_full_name=None,

            # name of the service_line_id in the source_data - in the format table_name.column_name
            source_data_service_line_full_name=None,

            # name of hvid column in target_data
            target_data_hvid_column_name='hvid',

            # name of claim_id column in target_data
            target_data_claim_column_name=None,

            # name of service_line_id column in target_data
            target_data_service_line_column_name=None,

            # name of gender column in target_data
            target_data_gender_column_name='patient_gender',

            # name of patient_state column in target_data
            target_data_patient_state_column_name='patient_state',

            # name of record_id column in target_data
            target_data_record_id_column_name='record_id',

            # name of created_date column in target_data
            target_data_created_date_column_name='created',

            # name of model_version column in target_data
            target_data_model_version_column_name='model_version',

            # name of data_set column in target_data
            target_data_data_set_column_name='data_set',

            # name of data_feed column in target_data
            target_data_data_feed_column_name='data_feed',

            # name of data_vendor column in target_data
            target_data_data_vendor_column_name='data_vendor',

            # name of provider_partition column in target_data
            target_data_provider_partition_column_name='part_provider',

            # name of date_partition column in target_data
            target_data_date_partition_column_name='part_best_date'
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
        self.target_data_patient_state_column_name = target_data_patient_state_column_name
        self.target_data_record_id_column_name = target_data_record_id_column_name
        self.target_data_created_date_column_name = target_data_created_date_column_name
        self.target_data_model_version_column_name = target_data_model_version_column_name
        self.target_data_data_set_column_name = target_data_data_set_column_name
        self.target_data_data_feed_column_name = target_data_data_feed_column_name
        self.target_data_data_vendor_column_name = target_data_data_vendor_column_name
        self.target_data_provider_partition_column_name = target_data_provider_partition_column_name
        self.target_data_date_partition_column_name = target_data_date_partition_column_name

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
