import pytest
import logging

import spark.qa.conf as qa_conf
import spark.qa.datatypes as types
import spark.helpers.file_utils as file_utils
import spark.helpers.constants as constants

# this will be set when a Datafeed instance is created. This instance
# is used as a fixture for pytest tests.
active_datafeed = None

class Comparison:
    """
    An object to coordinate unique column value comparisons between
    source and target data
    """
    def __init__(self, source_full_name, target_column_name):

        if not source_full_name or '.' not in source_full_name:
            raise ValueError('The source_full_name must be in the form <table_name>.<column_name>')

        self.source_table_name = source_full_name.split('.')[0]
        self.source_column_name = source_full_name.split('.')[1]
        self.target_column_name = target_column_name


class Validation:
    """
    An object to coordinate column value validation using a unique
    column name and a list of valid values for that column
    """
    def __init__(self, column_name, whitelist):
        self.column_name = column_name
        self.whitelist = whitelist


def gender_validation(column_name):
    return Validation(column_name, constants.genders)


def state_validation(column_name):
    return Validation(column_name, constants.states)


def age_validation(column_name):
    return Validation(column_name, [str(num) for num in range(0, 85)])


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

            # a list of columns that should be 100% populated in the target data
            target_full_fill_columns=None,

            # a list of validation objects - used to validate values in the given column using the given whitelist
            validations=None,

            # a list of comparison objects - used to compare source to target for a single column
            unique_match_pairs=None
    ):

        # set this instance as the active datafeed
        global active_datafeed
        active_datafeed = self

        # properties
        self.datatype = datatype
        self.source_data = source_data
        self.target_data = target_data

        self.target_full_fill_columns = target_full_fill_columns if target_full_fill_columns else []
        self.validations = validations if validations else []
        self.unique_match_pairs = unique_match_pairs if unique_match_pairs else []

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


def standard_datafeed(
        datatype,
        source_data=None,
        target_data=None,

        source_claim_id_full_name=None,
        source_hvid_full_name=None,

        additional_target_full_fill_columns=None,
        skip_target_full_fill_columns=None,
        additional_validations=None,
        skip_validations=None,
        additional_unique_match_pairs=None,
        skip_unique_match_pairs=None,
):
    if not additional_target_full_fill_columns:
        additional_target_full_fill_columns = []
    if not skip_target_full_fill_columns:
        skip_target_full_fill_columns = []
    if not additional_validations:
        additional_validations = []
    if not skip_validations:
        skip_validations = []
    if not additional_unique_match_pairs:
        additional_unique_match_pairs = []
    if not skip_unique_match_pairs:
        skip_unique_match_pairs = []

    if not source_claim_id_full_name:
        logging.warn('No source claim provided, claim test will be skipped')

    if not source_hvid_full_name:
        logging.warn('No source hvid provided, hvid test will be skipped')

    return Datafeed(
        datatype, source_data, target_data,
        target_full_fill_columns = [
            column for column in [
                'record_id', 'created', 'model_version', 'data_set', 'data_feed',
                'data_vendor', 'part_provider', 'part_best_date'
            ] if column not in skip_target_full_fill_columns
        ] + additional_target_full_fill_columns,
        validations=[
            validation for validation in [
                gender_validation('patient_gender'),
                state_validation('patient_state'),
                age_validation('patient_age')
            ] if validation.column_name not in skip_validations
        ] + additional_validations,
        unique_match_pairs=[
            unique_match_pair for unique_match_pair in [
                Comparison(source_claim_id_full_name, 'claim_id') if source_claim_id_full_name else None,
                Comparison(source_hvid_full_name, 'hvid') if source_hvid_full_name else None
            ] if unique_match_pair and unique_match_pair.target_column_name not in skip_unique_match_pairs
        ] + additional_unique_match_pairs
    )


def standard_medicalclaims_datafeed(
        source_data=None,
        target_data=None,
        source_claim_id_full_name=None,
        source_hvid_full_name=None,
        source_procedure_code_full_name=None,
        source_service_line_number_full_name=None,
        source_ndc_code_full_name=None,
        source_procedure_modifier_1_full_name=None,
        source_prov_rendering_npi_full_name=None,
        source_payer_name_full_name=None,
        additional_target_full_fill_columns=None,
        skip_target_full_fill_columns=None,
        additional_validations=None,
        skip_validations=None,
        additional_unique_match_pairs=None,
        skip_unique_match_pairs=None
):

    if not additional_validations:
        additional_validations = []
    if not skip_validations:
        skip_validations = []
    if not additional_unique_match_pairs:
        additional_unique_match_pairs = []
    if not skip_unique_match_pairs:
        skip_unique_match_pairs = []

    return standard_datafeed(
        datatype = types.MEDICALCLAIMS,
        source_data = source_data,
        target_data = target_data,

        source_claim_id_full_name = source_claim_id_full_name,
        source_hvid_full_name = source_hvid_full_name,
        additional_target_full_fill_columns = additional_target_full_fill_columns,
        skip_target_full_fill_columns = skip_target_full_fill_columns,

        additional_validations = [
            validation for validation in [
                state_validation('prov_rendering_state'),
                state_validation('prov_billing_state'),
                state_validation('prov_referring_state'),
                state_validation('prov_facility_state')
            ] if validation.column_name not in skip_validations
        ] + additional_validations,
        skip_validations=skip_validations,

        additional_unique_match_pairs = [
            comparison for comparison in [
                Comparison(source_procedure_code_full_name, 'procedure_code')
                if source_procedure_code_full_name else None,
                Comparison(source_service_line_number_full_name, 'service_line_number')
                if source_service_line_number_full_name else None,
                Comparison(source_ndc_code_full_name, 'ndc_code')
                if source_ndc_code_full_name else None,
                Comparison(source_procedure_modifier_1_full_name, 'procedure_modifier_1')
                if source_procedure_modifier_1_full_name else None,
                Comparison(source_prov_rendering_npi_full_name, 'prov_rendering_npi')
                if source_prov_rendering_npi_full_name else None,
                Comparison(source_payer_name_full_name, 'payer_name')
                if source_payer_name_full_name else None,
            ] if comparison and comparison.target_column_name not in skip_unique_match_pairs
        ] + additional_unique_match_pairs,
        skip_unique_match_pairs=skip_unique_match_pairs
    )
