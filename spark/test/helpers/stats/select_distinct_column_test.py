import pytest

from pyspark.sql import Row
from pyspark.sql.functions import col, isnan, trim

import spark.helpers.stats.select_distinct_column as select_distinct_column

df = None
results = None
expected_df = None

distinct_column_name = None

def cleanup(spark):
    pass


@pytest.mark.usefixtures('spark')
def test_init(spark):
    global df, results, expected_df, distinct_column_name
    distinct_column_name = 'claim_id'
    data_row = Row('claim_id', 'col_1', 'col_2', 'col_3')
    df = spark['spark'].sparkContext.parallelize([
        data_row('0', 'a', None, None),
        data_row('0', None, 'b', None),
        data_row('2828', '   ', 'hey', 'hi'),
        data_row('2929', None, '', ' '),
        data_row('2828', None, None, 'oh no'),
        data_row('0', float('Nan'), None, 'c')
    ]).toDF()


def test_something():
    df.show()
    test = lambda x: col(x).isNotNull() & ~isnan(x) & (trim(col(x)) != '')
    test_df = df.select(col(distinct_column_name),
        *[ test(c).cast('integer').alias(c) for c in df.columns if c != distinct_column_name]) \
                .groupBy(distinct_column_name).sum().toDF(*df.columns).na.replace(0, '', list(filter(lambda x: x != distinct_column_name, df.columns)))
    test_df.show()


