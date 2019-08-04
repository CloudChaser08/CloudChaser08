import pytest

from pyspark.sql import Row
from pyspark.sql.functions import col

import spark.stats.calc.key_stats as key_stats

df = None
results = None

@pytest.mark.usefixtures("spark")
def test_init(spark):
    global df, results
    conf = { 'date_field'       : ['date'],
             'record_field'     : 'c',
             'index_all_dates'  : False
            }
    data_row = Row('coalesced_date', 'hvid', 'c', 'd', 'e')
    df = spark['spark'].sparkContext.parallelize([
        data_row('1975-12-11', 'b', 'c', 'd', 'e'),
        data_row('2017-11-08', 'b', 'e', 'f', 'g'),
        data_row('2017-11-07', 'a', 's', 'u', 'x'),
        data_row('2017-11-01', 'c', 's', 'a', 'b'),
        data_row('2017-12-21', 'j', 'o', 'e', 'y'),
        data_row('2016-11-07', 'z', 'y', 'x', 'w')
    ]).toDF()
    results = key_stats.calculate_key_stats(df,
                '2015-11-01', '2017-01-01',
                '2017-12-01', conf)


def test_counts_are_correct_for_date_range():
    assert [x for x in results if x['field'] == 'total_patient'][0]['value'] == 4
    assert [x for x in results if x['field'] == 'total_24_month_patient'][0]['value'] == 3
    assert [x for x in results if x['field'] == 'total_record'][0]['value'] == 3
    assert [x for x in results if x['field'] == 'total_24_month_record'][0]['value'] == 2
    assert [x for x in results if x['field'] == 'total_row'][0]['value'] == 4
    assert [x for x in results if x['field'] == 'total_24_month_row'][0]['value'] == 3
