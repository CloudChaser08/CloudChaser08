"""
    Test cases for spark.stats.datamodels
"""

import pytest
from mock import Mock

from pyspark.sql import Row

from spark.stats.models import Column

from spark.stats.datamodel import get_columns_for_model

_COMMENT_FMT = '''
included_in_customer_std_data_model={} |
top_values={} |
description={}
'''

DW_ROW = Row('col_name', 'data_type', 'comment')

def _create_mock_sql_context(spark, rows):
    dataframe = spark['spark'].sparkContext.parallelize([
        DW_ROW(*r) for r in rows
    ]).toDF()

    sql_context = Mock(spec=spark['sqlContext'])
    sql_context.sql.return_value = dataframe
    return sql_context


def test_get_columns_for_model(spark):
    """ Tests the get_columns_for_model function """
    rows = [
        ('col_a', 'bigint', _COMMENT_FMT.format('Yes', 'Yes', 'Column A')),
        ('col_b', 'string', _COMMENT_FMT.format('Yes', 'No', 'Column B')),
        ('col_c', 'string', _COMMENT_FMT.format('No', 'No', 'Column C'))
    ]
    sql_context = _create_mock_sql_context(spark, rows)
    res = get_columns_for_model(sql_context, 'emr_enc')

    assert 'col_a' in res
    assert 'col_b' in res
    # C is ignored because the included_in_customer_std_data_model is False
    assert 'col_c' not in res

    assert res['col_a'] == Column(
        name='col_a',
        field_id='1',
        sequence='0',
        datatype='bigint',
        top_values=True,
        description='Column A',
    )
    assert res['col_b'] == Column(
        name='col_b',
        field_id='2',
        sequence='1',
        datatype='string',
        top_values=False,
        description='Column B',
    )


def test_bad_warehouse_columns(spark):
    """ Tests the get_columns_for_model function fails because of a bad bool
        field value
    """
    badval = 'NotYesNo'
    rows = [
        ('col_a', 'bigint', _COMMENT_FMT.format(badval, 'Yes', 'Column A')),
    ]
    sql_context = _create_mock_sql_context(spark, rows)
    with pytest.raises(ValueError):
        get_columns_for_model(sql_context, 'emr_enc')
