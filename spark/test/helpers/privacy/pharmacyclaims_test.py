import pytest
import spark.helpers.privacy.pharmacyclaims as pharmacy_priv
from pyspark.sql.types import StructField, StructType, StringType, Row

@pytest.mark.usefixtures("spark")
def test_filter(spark):
    # test df including commonly filtered fields
    test_df = spark['spark'].sparkContext.parallelize([
        ['1', 'rxnum', 'notransform']
    ]).toDF(StructType([
        StructField('id', StringType()),
        StructField('rx_number', StringType()),
        StructField('notransform', StringType())
    ]))

    # assert privacy filtering is being applied
    assert pharmacy_priv.filter(test_df).collect() \
        == [Row('1', '7e13d20348e5c163f11944938f1984f9', 'notransform')]


def test_level_of_service_filters(spark):
    # test df including commonly filtered fields
    test_df = spark['spark'].sparkContext.parallelize([
        ['1', '6', 'don\'t nullify me'] + [
            'nullify me' for _ in pharmacy_priv.columns_to_nullify
        ],
        ['2', 'safe level of service', 'don\'t nullify me'] + [
            'don\'t nullify me' for _ in pharmacy_priv.columns_to_nullify
        ]
    ]).toDF(StructType([
        StructField('id', StringType()),
        StructField('level_of_service', StringType()),
        StructField('other_col', StringType()),
    ] + [
        StructField(column_name, StringType())
        for column_name in pharmacy_priv.columns_to_nullify
    ]))

    # assert privacy filtering is being applied
    assert sorted(pharmacy_priv.filter(test_df).collect()) \
        == [
            Row(id='1', level_of_service='6', other_col='don\'t nullify me', **dict([(c, None) for c in pharmacy_priv.columns_to_nullify])),
            Row('2', 'safe level of service', 'don\'t nullify me', *['don\'t nullify me' for _ in pharmacy_priv.columns_to_nullify])
        ]
