from pyspark.sql.functions import udf, col
from pyspark.sql.types import IntegerType

def _convert_values(c):
    '''
    Map values in a column to 1 if not null, 0 if null
    Input:
        - col: dataframe column of type pyspark.sql.Column
    Output:
        - mapped: column of type pyspark.sql.Column
    '''

    is_not_null = col(c).isNotNull() & ~isnan(c) & (trim(col(c)) != '')
    mapped = is_not_null.cast("integer").alias(c)
    return mapped


def select_distinct_column(distinct_column_field):
    def out(df):
        mapped_df = df.select(col(distinct_column_name),
        *[ test(c).cast('integer').alias(c) for c in df.columns if c != distinct_column_name]) \
                .groupBy(distinct_column_name).sum().toDF(*df.columns).na.replace(0, '')
        return mapped_df


    return out


