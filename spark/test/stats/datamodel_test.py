"""
    Test cases for spark.stats.datamodels
"""

import csv

import pytest
from mock import Mock

from pyspark.sql import Row

from spark.stats.models import Column, TableMetadata

from spark.stats.datamodel import get_table_metadata


DW_ROW = Row('col_name', 'data_type', 'comment')

def _create_mock_sql_context(spark, fname):
    with open(fname) as csv_file:
        dataframe = spark['spark'].sparkContext.parallelize([
            DW_ROW(*r) for r in csv.reader(csv_file)
        ]).toDF()

    sql_context = Mock(spec=spark['sqlContext'])
    sql_context.sql.return_value = dataframe
    return sql_context


def test_get_columns_for_model(spark):
    """ Tests the get_columns_for_model function """
    sql_context = _create_mock_sql_context(
        spark,
        'test/resources/stats/warehouse_describe_table.csv'
    )
    res = get_table_metadata(sql_context, 'emr_enc')

    assert res == TableMetadata(
        name='mytablename',
        description='My cool view',
        columns=[
            Column(
                name='a',
                field_id='1',
                sequence='0',
                datatype='bigint',
                top_values=True,
                description='Column A',
            ),
            Column(
                name='b',
                field_id='2',
                sequence='1',
                datatype='string',
                top_values=False,
                description='Column B',
            )
        ]
    )


def test_bad_warehouse_columns(spark):
    """ Tests the get_table_metadata function fails because of a bad bool
        field value
    """
    sql_context = sql_context = _create_mock_sql_context(
        spark,
        'test/resources/stats/warehouse_describe_bad_comment.csv'
    )
    with pytest.raises(ValueError):
        get_table_metadata(sql_context, 'emr_enc')
