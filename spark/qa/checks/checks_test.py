import pytest

from pyspark.sql.functions import col

import spark.qa.checks.utils as check_utils


@pytest.mark.usefixtures("datafeed")
def test_validations(datafeed, validation):
    """
    For every validation object defined in this datafeed, match the
    target values against the whitelist.
    """

    # check dependencies
    check_utils.check_dependencies(datafeed, ['target_data'])

    example_values = set([
        row[validation.column_name] for row in
        datafeed.target_data.select(
            col(validation.column_name)
        ).distinct().take(100)
    ])
    assert example_values.difference(validation.whitelist + [None]) == set()


def test_full_fill_rates(datafeed, full_fill_column):
    """
    For every column in this datafeed that should have a full fill
    rate, ensure that it does.
    """
    check_utils.check_dependencies(
        datafeed, ['target_data']
    )

    check_utils.assert_full_fill_in_target(
        datafeed.target_data, full_fill_column
    )


def test_all_unique_vals_in_src_and_target(datafeed, unique_match_pair):

    """
    For every comparison object defined in this datafeed, confirm
    that all unique source values exist in the target
    """
    check_utils.check_dependencies(datafeed, [
        'source_data',
        'target_data'
    ])

    check_utils.assert_all_values_in_src_and_target(
        datafeed.source_data[unique_match_pair.source_table_name],
        datafeed.target_data,
        unique_match_pair.source_column_name,
        unique_match_pair.target_column_name,
        unique_match_pair.column_name
    )
