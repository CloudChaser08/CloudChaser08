import pytest

from pyspark.sql.functions import col, trim

import spark.helpers.postprocessor as postprocessor


def check_dependencies(datafeed, dependencies):
    for dependency in dependencies:
        if not getattr(datafeed, dependency):
            pytest.skip("Requirement missing from datafeed: {}".format(dependency))
            return


def remove_nulls_and_blanks(table, column):
    return table.filter(
        (trim(col(column)) != '') & col(column).isNotNull()
    )


def assert_all_values_in_src_and_target(
        source_table, target_table, source_column_name, target_column_name
):
    """
    All values that exist in the source exist in the target, and vice-versa
    """

    src_values = postprocessor.nullify(source_table).select(col(source_column_name)).distinct().rdd
    src_value_count = src_values.count()

    target_values = target_table.select(col(target_column_name)).distinct().rdd
    target_value_count = target_values.count()

    common_value_count = src_values.intersection(target_values).count()

    assert common_value_count == src_value_count, \
        "{} values in source did not exist in target. Examples: {}".format(
            target_column_name, ', '.join([
                r[source_column_name] for r in src_values.subtract(target_values).take(10)
            ]))

    assert common_value_count == target_value_count, \
        "{} values in target did not exist in source. Examples: {}".format(
            target_column_name, ', '.join([
                r[target_column_name] for r in target_values.subtract(src_values).take(10)
            ]))


def assert_full_fill_in_target(target_table, target_column_name):
    """
    The target_column_name is never null or blank
    """
    relevant_column = target_table.select(col(target_column_name))
    assert relevant_column.count() == \
           remove_nulls_and_blanks(relevant_column, target_column_name).count()
