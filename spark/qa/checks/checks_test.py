import pytest

from pyspark.sql.functions import col

import spark.qa.checks.utils as check_utils
import spark.helpers.constants as constants


@pytest.mark.usefixtures("datafeed")
def test_all_claims_in_src_and_target(datafeed):
    """
    All claims that exist in the source must exist in the target, and
    vice-versa
    """

    # check dependencies
    check_utils.check_dependencies(datafeed, [
        'source_data',
        'target_data',
        'source_data_claim_column_name',
        'source_data_claim_table_name',
        'target_data_claim_column_name'
    ])

    check_utils.assert_all_values_in_src_and_target(
        datafeed.source_data[datafeed.source_data_claim_table_name],
        datafeed.target_data,
        datafeed.source_data_claim_column_name,
        datafeed.target_data_claim_column_name,
        "Claim"
    )


def test_all_service_lines_in_src_and_target(datafeed):
    """
    All service_lines that exist in the source must exist in the target, and
    vice-versa
    """

    check_utils.check_dependencies(datafeed, [
        'source_data',
        'target_data',
        'source_data_service_line_column_name',
        'source_data_service_line_table_name',
        'target_data_service_line_column_name'
    ])

    check_utils.assert_all_values_in_src_and_target(
        datafeed.source_data[datafeed.source_data_service_line_table_name],
        datafeed.target_data,
        datafeed.source_data_service_line_column_name,
        datafeed.target_data_service_line_column_name,
        "Service Line"
    )


def test_all_hvids_in_src_and_target(datafeed):
    """
    All hvids that exist in the source must exist in the target, and
    vice-versa
    """

    check_utils.check_dependencies(datafeed, [
        'source_data',
        'target_data',
        'source_data_hvid_column_name',
        'source_data_hvid_table_name',
        'target_data_hvid_column_name'
    ])

    check_utils.assert_all_values_in_src_and_target(
        datafeed.source_data[datafeed.source_data_hvid_table_name],
        datafeed.target_data,
        datafeed.source_data_hvid_column_name,
        datafeed.target_data_hvid_column_name,
        "HVID"
    )


def test_valid_gender_values_in_target(datafeed):
    """
    All gender values in the target data are valid
    """

    # check dependencies
    check_utils.check_dependencies(datafeed, [
        'target_data', 'target_data_gender_column_name'
    ])

    patient_gender_values = set([
        row[datafeed.target_data_gender_column_name] for row in
        datafeed.target_data.select(
            col(datafeed.target_data_gender_column_name)
        ).distinct().take(100)
    ])
    assert patient_gender_values.difference(['M', 'F', 'U', None]) == set()


def test_valid_patient_state_values_in_target(datafeed):
    """
    All patient_state values in the target data are validvalid
    """

    # check dependencies
    check_utils.check_dependencies(datafeed, [
        'target_data', 'target_data_patient_state_column_name'
    ])

    distinct_patient_state_values = set([
        row[datafeed.target_data_patient_state_column_name] for row in
        datafeed.target_data.filter(
            col(datafeed.target_data_patient_state_column_name).isNotNull()
        ).select(
            col(datafeed.target_data_patient_state_column_name)
        ).distinct().take(100)
    ])

    assert distinct_patient_state_values.difference(constants.states) == set()


def test_full_fill_rate_for_record_id(datafeed):
    """
    record_id should never be null or blank
    """
    check_utils.check_dependencies(
        datafeed, [
            'target_data', 'target_data_record_id_column_name'
        ]
    )

    check_utils.assert_full_fill_in_target(
        datafeed.target_data, datafeed.target_data_record_id_column_name
    )


def test_full_fill_rate_for_created_date(datafeed):
    """
    created_date should never be null or blank
    """
    check_utils.check_dependencies(
        datafeed, [
            'target_data', 'target_data_created_date_column_name'
        ]
    )

    check_utils.assert_full_fill_in_target(
        datafeed.target_data, datafeed.target_data_created_date_column_name
    )


def test_full_fill_rate_for_model_version(datafeed):
    """
    model_version should never be null or blank
    """
    check_utils.check_dependencies(
        datafeed, [
            'target_data', 'target_data_model_version_column_name'
        ]
    )

    check_utils.assert_full_fill_in_target(
        datafeed.target_data, datafeed.target_data_model_version_column_name
    )


def test_full_fill_rate_for_data_set(datafeed):
    """
    data_set should never be null or blank
    """
    check_utils.check_dependencies(
        datafeed, [
            'target_data', 'target_data_data_set_column_name'
        ]
    )

    check_utils.assert_full_fill_in_target(
        datafeed.target_data, datafeed.target_data_data_set_column_name
    )


def test_full_fill_rate_for_data_feed(datafeed):
    """
    data_feed should never be null or blank
    """
    check_utils.check_dependencies(
        datafeed, [
            'target_data', 'target_data_data_feed_column_name'
        ]
    )

    check_utils.assert_full_fill_in_target(
        datafeed.target_data, datafeed.target_data_data_feed_column_name
    )


def test_full_fill_rate_for_data_vendor(datafeed):
    """
    data_vendor should never be null or blank
    """
    check_utils.check_dependencies(
        datafeed, [
            'target_data', 'target_data_data_vendor_column_name'
        ]
    )

    check_utils.assert_full_fill_in_target(
        datafeed.target_data, datafeed.target_data_data_vendor_column_name
    )


def test_full_fill_rate_for_provider_partition(datafeed):
    """
    provider_partition should never be null or blank
    """
    check_utils.check_dependencies(
        datafeed, [
            'target_data', 'target_data_provider_partition_column_name'
        ]
    )

    check_utils.assert_full_fill_in_target(
        datafeed.target_data, datafeed.target_data_provider_partition_column_name
    )


def test_full_fill_rate_for_date_partition(datafeed):
    """
    date_partition should never be null or blank
    """
    check_utils.check_dependencies(
        datafeed, [
            'target_data', 'target_data_date_partition_column_name'
        ]
    )

    check_utils.assert_full_fill_in_target(
        datafeed.target_data, datafeed.target_data_date_partition_column_name
    )
