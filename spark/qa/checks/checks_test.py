import pytest

from pyspark.sql.functions import col

import spark.qa.checks.utils as check_utils


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

    patient_gender_values = datafeed.target_data.select(
        col(datafeed.target_data_gender_column_name)
    ).distinct().take(10)

    for row in patient_gender_values:
        assert row[datafeed.target_data_gender_column_name] in ['M', 'F', 'U', None]
