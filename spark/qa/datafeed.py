import pytest
import logging
import sys

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
    return Validation(column_name, [str(num) for num in list(range(0, 85)) + [90]])


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

    def run_checks(self, output_file=None):
        """
        Kick off a pytest session that will run all checks relevant to
        this datafeed.
        """
        logging.getLogger("py4j").setLevel(logging.ERROR)

        # point stdout to output_file if it was set
        original_stdout = sys.stdout
        output_file_handle = open(output_file, 'w') if output_file else sys.stdout
        sys.stdout = output_file_handle

        pytest.main([
            '-v',

            # only run tests that apply to this datatype
            '-k', '{}'.format(' or '.join(qa_conf.datatype_config[self.datatype])),

            # only look at tests in the 'checks' dir
            '{}'.format(file_utils.get_abs_path(__file__, './checks/')),

            # summarize passed, failed and skipped tests
            '-r', 'p f s'
        ])

        # close file and reset stdout
        if output_file:
            output_file_handle.close()
            sys.stdout = original_stdout


def build_test_list(default_tests, skip_tests, additional_tests):
    """
    Build a list of tests to run given a list of default tests, a list
    of tests to add to the default, and a list of default tests to
    skip.

    Tests can be one of several types:
      full fill tests     - string
      validation tests    - Validation
      unique match tests  - Comparison
    """

    def get_test_id(test):
        """
        Given a test, return a string id for that test that can be used to
        compare against the skip test list
        """
        if isinstance(test, str):
            return test
        elif isinstance(test, Comparison):
            return test.target_column_name
        elif isinstance(test, Validation):
            return test.column_name

    return [
        test for test in default_tests if test and get_test_id(test) not in skip_tests
    ] + additional_tests


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
        logging.warning('No source claim provided, claim test will be skipped')

    if not source_hvid_full_name:
        logging.warning('No source hvid provided, hvid test will be skipped')

    return Datafeed(
        datatype, source_data, target_data,
        target_full_fill_columns=build_test_list([
            'record_id', 'created', 'model_version', 'data_set', 'data_feed', 'data_vendor'
        ], skip_target_full_fill_columns, additional_target_full_fill_columns),
        validations=build_test_list([
            gender_validation('patient_gender'), state_validation('patient_state'), age_validation('patient_age')
        ], skip_validations, additional_validations),
        unique_match_pairs=build_test_list([
            Comparison(source_claim_id_full_name, 'claim_id') if source_claim_id_full_name else None,
            Comparison(source_hvid_full_name, 'hvid') if source_hvid_full_name else None
        ], skip_unique_match_pairs, additional_unique_match_pairs)
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
        datatype=types.MEDICALCLAIMS,
        source_data=source_data,
        target_data=target_data,

        source_claim_id_full_name=source_claim_id_full_name,
        source_hvid_full_name=source_hvid_full_name,
        additional_target_full_fill_columns=additional_target_full_fill_columns,
        skip_target_full_fill_columns=skip_target_full_fill_columns,

        additional_validations=build_test_list([
            state_validation('prov_rendering_state'),
            state_validation('prov_billing_state'),
            state_validation('prov_referring_state'),
            state_validation('prov_facility_state')
        ], skip_validations, additional_validations),
        skip_validations=skip_validations,

        additional_unique_match_pairs=build_test_list([
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
        ], skip_unique_match_pairs, additional_unique_match_pairs),
        skip_unique_match_pairs=skip_unique_match_pairs
    )


def standard_pharmacyclaims_datafeed(
        source_data=None,
        target_data=None,
        source_claim_id_full_name=None,
        source_hvid_full_name=None,
        source_procedure_code_full_name=None,
        source_ndc_code_full_name=None,
        source_payer_name_full_name=None,
        source_prov_prescribing_npi_full_name=None,
        source_bin_number_full_name=None,
        additional_target_full_fill_columns=None,
        skip_target_full_fill_columns=None,
        additional_validations=None,
        skip_validations=None,
        additional_unique_match_pairs=None,
        skip_unique_match_pairs=None
):

    if not additional_unique_match_pairs:
        additional_unique_match_pairs = []
    if not skip_unique_match_pairs:
        skip_unique_match_pairs = []

    return standard_datafeed(
        datatype=types.PHARMACYCLAIMS,
        source_data=source_data,
        target_data=target_data,

        source_claim_id_full_name=source_claim_id_full_name,
        source_hvid_full_name=source_hvid_full_name,
        additional_target_full_fill_columns=additional_target_full_fill_columns,
        skip_target_full_fill_columns=skip_target_full_fill_columns,

        additional_validations=additional_validations,
        skip_validations=skip_validations,

        additional_unique_match_pairs=build_test_list([
            Comparison(source_procedure_code_full_name, 'procedure_code')
            if source_procedure_code_full_name else None,
            Comparison(source_ndc_code_full_name, 'ndc_code')
            if source_ndc_code_full_name else None,
            Comparison(source_prov_prescribing_npi_full_name, 'prov_prescribing_npi')
            if source_prov_prescribing_npi_full_name else None,
            Comparison(source_bin_number_full_name, 'bin_number')
            if source_bin_number_full_name else None,
            Comparison(source_payer_name_full_name, 'payer_name')
            if source_payer_name_full_name else None,
        ], skip_unique_match_pairs, additional_unique_match_pairs),
        skip_unique_match_pairs=skip_unique_match_pairs
    )


def standard_labtests_datafeed(
        source_data=None,
        target_data=None,
        source_claim_id_full_name=None,
        source_hvid_full_name=None,
        source_payer_name_full_name=None,
        source_procedure_code_full_name=None,
        source_loinc_code_full_name=None,
        source_ordering_npi_full_name=None,
        source_test_ordered_name_full_name=None,
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
        datatype=types.LABTESTS,
        source_data=source_data,
        target_data=target_data,

        source_claim_id_full_name=source_claim_id_full_name,
        source_hvid_full_name=source_hvid_full_name,
        additional_target_full_fill_columns=additional_target_full_fill_columns,
        skip_target_full_fill_columns=skip_target_full_fill_columns,

        additional_validations=build_test_list([
            state_validation('ordering_state'),
        ], skip_validations, additional_validations),
        skip_validations=skip_validations,

        additional_unique_match_pairs=build_test_list([
            Comparison(source_procedure_code_full_name, 'procedure_code')
            if source_procedure_code_full_name else None,
            Comparison(source_ordering_npi_full_name, 'ordering_npi')
            if source_ordering_npi_full_name else None,
            Comparison(source_loinc_code_full_name, 'loinc_code')
            if source_loinc_code_full_name else None,
            Comparison(source_test_ordered_name_full_name, 'test_ordered_name')
            if source_test_ordered_name_full_name else None,
            Comparison(source_payer_name_full_name, 'payer_name')
            if source_payer_name_full_name else None,
        ], skip_unique_match_pairs, additional_unique_match_pairs),
        skip_unique_match_pairs=skip_unique_match_pairs
    )


def standard_events_datafeed(
        source_data=None,
        target_data=None,
        source_claim_id_full_name=None,
        source_hvid_full_name=None,
        source_event_count_full_name=None,
        source_event_val_full_name=None,
        additional_target_full_fill_columns=None,
        skip_target_full_fill_columns=None,
        additional_validations=None,
        skip_validations=None,
        additional_unique_match_pairs=None,
        skip_unique_match_pairs=None
):

    if not additional_unique_match_pairs:
        additional_unique_match_pairs = []
    if not skip_unique_match_pairs:
        skip_unique_match_pairs = []

    return standard_datafeed(
        datatype=types.EVENTS,
        source_data=source_data,
        target_data=target_data,

        source_claim_id_full_name=source_claim_id_full_name,
        source_hvid_full_name=source_hvid_full_name,
        additional_target_full_fill_columns=additional_target_full_fill_columns,
        skip_target_full_fill_columns=skip_target_full_fill_columns,

        additional_validations=additional_validations,
        skip_validations=skip_validations,

        additional_unique_match_pairs=build_test_list([
            Comparison(source_event_count_full_name, 'event_count')
            if source_event_count_full_name else None,
            Comparison(source_event_val_full_name, 'event_val')
            if source_event_val_full_name else None,
        ], skip_unique_match_pairs, additional_unique_match_pairs),
        skip_unique_match_pairs=skip_unique_match_pairs
    )


def standard_enrollment_datafeed(
        source_data=None,
        target_data=None,
        source_claim_id_full_name=None,
        source_hvid_full_name=None,
        source_benefit_type_full_name=None,
        additional_target_full_fill_columns=None,
        skip_target_full_fill_columns=None,
        additional_validations=None,
        skip_validations=None,
        additional_unique_match_pairs=None,
        skip_unique_match_pairs=None
):
    if not additional_unique_match_pairs:
        additional_unique_match_pairs = []
    if not skip_unique_match_pairs:
        skip_unique_match_pairs = []

    return standard_datafeed(
        datatype=types.EVENTS,
        source_data=source_data,
        target_data=target_data,

        source_claim_id_full_name=source_claim_id_full_name,
        source_hvid_full_name=source_hvid_full_name,
        additional_target_full_fill_columns=additional_target_full_fill_columns,
        skip_target_full_fill_columns=skip_target_full_fill_columns,

        additional_validations=additional_validations,
        skip_validations=skip_validations,

        additional_unique_match_pairs=build_test_list([
            Comparison(source_benefit_type_full_name, 'benefit_type')
            if source_benefit_type_full_name else None
        ], skip_unique_match_pairs, additional_unique_match_pairs),
        skip_unique_match_pairs=skip_unique_match_pairs
    )


def emr_datafeed(
        datatype,
        source_data=None,
        target_data=None,

        source_enc_id_full_name=None,
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

    if not source_enc_id_full_name:
        logging.warning('No source enc_id provided, enc_id test will be skipped')

    if not source_hvid_full_name:
        logging.warning('No source hvid provided, hvid test will be skipped')

    return Datafeed(
        datatype, source_data, target_data,
        target_full_fill_columns=build_test_list([
            'row_id', 'crt_dt', 'mdl_vrsn_num', 'data_set_nm', 'hvm_vdr_feed_id', 'hvm_vdr_id'
        ], skip_target_full_fill_columns, additional_target_full_fill_columns),
        validations=build_test_list([
            gender_validation('ptnt_gender_cd'),
            state_validation('ptnt_state_cd'),
            age_validation('ptnt_age_num')
        ], skip_validations, additional_validations),
        unique_match_pairs=build_test_list([
            Comparison(source_enc_id_full_name, 'enc_id') if source_enc_id_full_name else None,
            Comparison(source_hvid_full_name, 'hvid') if source_hvid_full_name else None
        ], skip_unique_match_pairs, additional_unique_match_pairs)
    )


def emr_encounter_datafeed(
        source_data=None,
        target_data=None,
        source_enc_id_full_name=None,
        source_hvid_full_name=None,
        source_enc_typ_cd_full_name=None,
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

    return emr_datafeed(
        datatype=types.EMR_ENCOUNTER,
        source_data=source_data,
        target_data=target_data,

        source_enc_id_full_name=source_enc_id_full_name,
        source_hvid_full_name=source_hvid_full_name,
        additional_target_full_fill_columns=additional_target_full_fill_columns,
        skip_target_full_fill_columns=skip_target_full_fill_columns,

        additional_validations=build_test_list([
            state_validation('enc_rndrg_fclty_state_cd'),
            state_validation('enc_rndrg_prov_state_cd'),
            state_validation('enc_rfrg_prov_state_cd')
        ], skip_validations, additional_validations),
        skip_validations=skip_validations,

        additional_unique_match_pairs=build_test_list([
            Comparison(source_enc_typ_cd_full_name, 'enc_typ_cd')
            if source_enc_typ_cd_full_name else None
        ], skip_unique_match_pairs, additional_unique_match_pairs),
        skip_unique_match_pairs=skip_unique_match_pairs
    )


def emr_diagnosis_datafeed(
        source_data=None,
        target_data=None,
        source_enc_id_full_name=None,
        source_hvid_full_name=None,
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

    return emr_datafeed(
        datatype=types.EMR_DIAGNOSIS,
        source_data=source_data,
        target_data=target_data,

        source_enc_id_full_name=source_enc_id_full_name,
        source_hvid_full_name=source_hvid_full_name,
        additional_target_full_fill_columns=additional_target_full_fill_columns,
        skip_target_full_fill_columns=skip_target_full_fill_columns,

        additional_validations=build_test_list([
            state_validation('diag_rndrg_fclty_state_cd'),
            state_validation('diag_rndrg_prov_state_cd')
        ], skip_validations, additional_validations),
        skip_validations=skip_validations,

        additional_unique_match_pairs=additional_unique_match_pairs,
        skip_unique_match_pairs=skip_unique_match_pairs
    )


def emr_procedure_datafeed(
        source_data=None,
        target_data=None,
        source_enc_id_full_name=None,
        source_hvid_full_name=None,
        source_proc_cd_full_name=None,
        source_proc_cd_1_modfr_full_name=None,
        source_proc_unit_qty_full_name=None,
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

    return emr_datafeed(
        datatype=types.EMR_PROCEDURE,
        source_data=source_data,
        target_data=target_data,

        source_enc_id_full_name=source_enc_id_full_name,
        source_hvid_full_name=source_hvid_full_name,
        additional_target_full_fill_columns=additional_target_full_fill_columns,
        skip_target_full_fill_columns=skip_target_full_fill_columns,

        additional_validations=build_test_list([
            state_validation('prov_rndrg_fclty_state_cd'),
            state_validation('prov_rndrg_prov_state_cd')
        ], skip_validations, additional_validations),
        skip_validations=skip_validations,

        additional_unique_match_pairs=build_test_list([
            Comparison(source_proc_cd_full_name, 'proc_cd')
            if source_proc_cd_full_name else None,
            Comparison(source_proc_cd_1_modfr_full_name, 'proc_cd_1_modfr')
            if source_proc_cd_1_modfr_full_name else None,
            Comparison(source_proc_unit_qty_full_name, 'proc_unit_qty')
            if source_proc_unit_qty_full_name else None
        ], skip_unique_match_pairs, additional_unique_match_pairs),
        skip_unique_match_pairs=skip_unique_match_pairs
    )


def emr_provider_order_datafeed(
        source_data=None,
        target_data=None,
        source_enc_id_full_name=None,
        source_hvid_full_name=None,
        source_prov_ord_cd_full_name=None,
        source_prov_ord_snomed_cd_full_name=None,
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

    return emr_datafeed(
        datatype=types.EMR_PROVIDER_ORDER,
        source_data=source_data,
        target_data=target_data,

        source_enc_id_full_name=source_enc_id_full_name,
        source_hvid_full_name=source_hvid_full_name,
        additional_target_full_fill_columns=additional_target_full_fill_columns,
        skip_target_full_fill_columns=skip_target_full_fill_columns,

        additional_validations=build_test_list([
            state_validation('ordg_prov_state_cd')
        ], skip_validations, additional_validations),
        skip_validations=skip_validations,

        additional_unique_match_pairs=build_test_list([
            Comparison(source_prov_ord_cd_full_name, 'prov_ord_cd')
            if source_prov_ord_cd_full_name else None,
            Comparison(source_prov_ord_snomed_cd_full_name, 'prov_ord_snomed_cd')
            if source_prov_ord_snomed_cd_full_name else None
        ], skip_unique_match_pairs, additional_unique_match_pairs),
        skip_unique_match_pairs=skip_unique_match_pairs
    )


def emr_lab_order_datafeed(
        source_data=None,
        target_data=None,
        source_enc_id_full_name=None,
        source_hvid_full_name=None,
        source_lab_ord_loinc_cd_full_name=None,
        source_lab_ord_test_nm_full_name=None,
        source_lab_ord_panel_nm_full_name=None,
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

    return emr_datafeed(
        datatype=types.EMR_LAB_ORDER,
        source_data=source_data,
        target_data=target_data,

        source_enc_id_full_name=source_enc_id_full_name,
        source_hvid_full_name=source_hvid_full_name,
        additional_target_full_fill_columns=additional_target_full_fill_columns,
        skip_target_full_fill_columns=skip_target_full_fill_columns,

        additional_validations=build_test_list([
            state_validation('lab_ord_ordg_prov_state_cd')
        ], skip_validations, additional_validations),
        skip_validations=skip_validations,

        additional_unique_match_pairs=build_test_list([
            Comparison(source_lab_ord_loinc_cd_full_name, 'lab_ord_loinc_cd')
            if source_lab_ord_loinc_cd_full_name else None,
            Comparison(source_lab_ord_test_nm_full_name, 'lab_ord_test_nm')
            if source_lab_ord_test_nm_full_name else None,
            Comparison(source_lab_ord_panel_nm_full_name, 'lab_ord_panel_nm')
            if source_lab_ord_panel_nm_full_name else None
        ], skip_unique_match_pairs, additional_unique_match_pairs),
        skip_unique_match_pairs=skip_unique_match_pairs
    )


def emr_lab_result_datafeed(
        source_data=None,
        target_data=None,
        source_enc_id_full_name=None,
        source_hvid_full_name=None,
        source_lab_test_nm_full_name=None,
        source_lab_test_loinc_cd_full_name=None,
        source_lab_test_snomed_cd_full_name=None,
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

    return emr_datafeed(
        datatype=types.EMR_LAB_RESULT,
        source_data=source_data,
        target_data=target_data,

        source_enc_id_full_name=source_enc_id_full_name,
        source_hvid_full_name=source_hvid_full_name,
        additional_target_full_fill_columns=additional_target_full_fill_columns,
        skip_target_full_fill_columns=skip_target_full_fill_columns,

        additional_validations=build_test_list([
            state_validation('lab_test_ordg_prov_state_cd'),
            state_validation('lab_test_exectg_fclty_state_cd')
        ], skip_validations, additional_validations),
        skip_validations=skip_validations,

        additional_unique_match_pairs=build_test_list([
            Comparison(source_lab_test_nm_full_name, 'lab_test_nm')
            if source_lab_test_nm_full_name else None,
            Comparison(source_lab_test_loinc_cd_full_name, 'lab_test_loinc_cd')
            if source_lab_test_loinc_cd_full_name else None,
            Comparison(source_lab_test_snomed_cd_full_name, 'lab_test_snomed_cd')
            if source_lab_test_snomed_cd_full_name else None
        ], skip_unique_match_pairs, additional_unique_match_pairs),
        skip_unique_match_pairs=skip_unique_match_pairs
    )


def emr_medication_datafeed(
        source_data=None,
        target_data=None,
        source_enc_id_full_name=None,
        source_hvid_full_name=None,
        source_medctn_ndc_full_name=None,
        source_medctn_brd_nm_full_name=None,
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

    return emr_datafeed(
        datatype=types.EMR_MEDICATION,
        source_data=source_data,
        target_data=target_data,

        source_enc_id_full_name=source_enc_id_full_name,
        source_hvid_full_name=source_hvid_full_name,
        additional_target_full_fill_columns=additional_target_full_fill_columns,
        skip_target_full_fill_columns=skip_target_full_fill_columns,

        additional_validations=build_test_list([
            state_validation('medctn_rndrg_fclty_state_cd'),
            state_validation('medctn_ordg_prov_state_cd'),
            state_validation('medctn_adminrg_fclty_state_cd')
        ], skip_validations, additional_validations),
        skip_validations=skip_validations,

        additional_unique_match_pairs=build_test_list([
            Comparison(source_medctn_ndc_full_name, 'medctn_ndc')
            if source_medctn_ndc_full_name else None,
            Comparison(source_medctn_brd_nm_full_name, 'medctn_brd_nm')
            if source_medctn_brd_nm_full_name else None
        ], skip_unique_match_pairs, additional_unique_match_pairs),
        skip_unique_match_pairs=skip_unique_match_pairs
    )


def emr_clinical_observation_datafeed(
        source_data=None,
        target_data=None,
        source_enc_id_full_name=None,
        source_hvid_full_name=None,
        source_clin_obsn_typ_nm_full_name=None,
        source_clin_obsn_snomed_cd_full_name=None,
        source_clin_obsn_result_nm_full_name=None,
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

    return emr_datafeed(
        datatype=types.EMR_CLINICAL_OBSERVATION,
        source_data=source_data,
        target_data=target_data,

        source_enc_id_full_name=source_enc_id_full_name,
        source_hvid_full_name=source_hvid_full_name,
        additional_target_full_fill_columns=additional_target_full_fill_columns,
        skip_target_full_fill_columns=skip_target_full_fill_columns,

        additional_validations=build_test_list([
            state_validation('clin_obsn_rndrg_fclty_state_cd'),
            state_validation('clin_obsn_rndrg_prov_state_cd')
        ], skip_validations, additional_validations),
        skip_validations=skip_validations,

        additional_unique_match_pairs=build_test_list([
            Comparison(source_clin_obsn_typ_nm_full_name, 'clin_obsn_typ_nm')
            if source_clin_obsn_typ_nm_full_name else None,
            Comparison(source_clin_obsn_snomed_cd_full_name, 'clin_obsn_snomed_cd')
            if source_clin_obsn_snomed_cd_full_name else None,
            Comparison(source_clin_obsn_result_nm_full_name, 'clin_obsn_result_nm')
            if source_clin_obsn_result_nm_full_name else None
        ], skip_unique_match_pairs, additional_unique_match_pairs),
        skip_unique_match_pairs=skip_unique_match_pairs
    )


def emr_vital_sign_datafeed(
        source_data=None,
        target_data=None,
        source_enc_id_full_name=None,
        source_hvid_full_name=None,
        source_vit_sign_typ_cd_full_name=None,
        source_vit_sign_snomed_cd_full_name=None,
        source_vit_sign_msrmt_full_name=None,
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

    return emr_datafeed(
        datatype=types.EMR_VITAL_SIGN,
        source_data=source_data,
        target_data=target_data,

        source_enc_id_full_name=source_enc_id_full_name,
        source_hvid_full_name=source_hvid_full_name,
        additional_target_full_fill_columns=additional_target_full_fill_columns,
        skip_target_full_fill_columns=skip_target_full_fill_columns,

        additional_validations=build_test_list([
            state_validation('vit_sign_rndrg_fclty_state_cd'),
            state_validation('vit_sign_rndrg_prov_state_cd')
        ], skip_validations, additional_validations),
        skip_validations=skip_validations,

        additional_unique_match_pairs=build_test_list([
            Comparison(source_vit_sign_typ_cd_full_name, 'vit_sign_typ_cd')
            if source_vit_sign_typ_cd_full_name else None,
            Comparison(source_vit_sign_snomed_cd_full_name, 'vit_sign_snomed_cd')
            if source_vit_sign_snomed_cd_full_name else None,
            Comparison(source_vit_sign_msrmt_full_name, 'vit_sign_msrmt')
            if source_vit_sign_msrmt_full_name else None
        ], skip_unique_match_pairs, additional_unique_match_pairs),
        skip_unique_match_pairs=skip_unique_match_pairs
    )
