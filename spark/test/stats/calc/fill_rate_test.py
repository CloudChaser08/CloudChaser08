import pytest

from pyspark.sql import Row

import spark.stats.calc.fill_rate as fill_rate

EXPECTED_RESULTS = [0.5, 0.0, 0.75, 0.25, 0.75]

@pytest.fixture(scope='module', name='dataframe')
def _get_dataframe(spark):
    data_row = Row('a', 'b', 'c', 'd', 'e')
    yield spark['spark'].sparkContext.parallelize([
        data_row('1', ' ', '', 'null', ' '),
        data_row('2', '', '2', '', 'hey'),
        data_row(None, None, 'a', None, 'hi'),
        data_row('', '', 'b', '', 'hello')
    ]).toDF()


@pytest.fixture(scope='module', name='results')
def _get_results(dataframe):
    yield fill_rate.calculate_fill_rate(dataframe)


def test_num_columns_equal(dataframe, results):
    num_dataframe_cols = len(dataframe.columns)

    # We should have 1 result per column in the original dataframe
    num_results = len(results)
    assert num_dataframe_cols == num_results


def test_column_names_equal(dataframe, results):
    dataframe_cols = dataframe.columns
    results_cols = [r['field'] for r in results]
    assert dataframe_cols == results_cols


def test_expected_values(results):
    # This tests has some holes in it, so we need to make sure
    # the arrays identical even if this passes
    assert set(EXPECTED_RESULTS) - set([r['fill'] for r in results]) == set()
    assert EXPECTED_RESULTS == [r['fill'] for r in results]


@pytest.mark.usefixtures("spark")
def test_gender_unknown_filetered(spark):
    """Test that the unknown gender values are no included when fill
    rate is calculated"""

    data_row = Row('claim_id', 'patient_gender', 'ptnt_gender_cd')
    dataframe = spark['spark'].sparkContext.parallelize([
        data_row('1', 'F', 'M'),
        data_row('2', 'F', 'F'),
        data_row('3', 'M', None),
        data_row('4', 'U', 'U') # should be filtered out
    ]).toDF()

    assert [r['fill'] for r in fill_rate.calculate_fill_rate(dataframe)] == [1.0, 0.75, 0.5]
