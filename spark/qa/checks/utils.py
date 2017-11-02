import pytest

from pyspark.sql.functions import col, trim


def check_dependencies(datafeed, dependencies):
    for dependency in dependencies:
        if not getattr(datafeed, dependency):
            pytest.skip("Requirement missing from datafeed: {}".format(dependency))
            return


def assert_all_values_in_src_and_target(
        source_table, target_table, source_column_name, target_column_name, value_display_name
):
    """
    All claims that exist in the source must exist in the target, and
    vice-versa
    """

    src_values = source_table.select(col(source_column_name)).filter(
        (trim(col(source_column_name)) != '') & col(source_column_name).isNotNull()
    ).distinct().rdd
    src_value_count = src_values.count()

    target_values = target_table.select(col(target_column_name)).filter(
        (trim(col(target_column_name)) != '') & col(target_column_name).isNotNull()
    ).distinct().rdd
    target_value_count = target_values.count()

    common_value_count = src_values.intersection(target_values).count()

    assert common_value_count == src_value_count, \
        "{} values in source did not exist in target. Examples: {}".format(
            value_display_name, ', '.join([
                r[source_column_name] for r in src_values.subtract(target_values).take(10)
            ]))

    assert common_value_count == target_value_count, \
        "{} values in target did not exist in source. Examples: {}".format(
            value_display_name, ', '.join([
                r[target_column_name] for r in target_values.subtract(src_values).take(10)
            ]))
