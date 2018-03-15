from pyspark.sql.functions import col, sum, isnan, count, trim, lit

def _col_fill_rate(c, row_count):
    '''
    Calculate the fill rate for a given column.
    "null" values can be the following:
        - NULL
        - ''
        - ' '
        - NaN
    Input:
        - col: dataframe column of type pyspark.sql.Column
        - row_count: the number of rows in the column as an int
    Output:
        - fr: the fill rate for that column of type pyspark.sql.Column
    '''
    is_not_null = col(c).isNotNull() & (trim(col(c)) != '')
    fr = (sum(is_not_null.cast("integer")) / row_count).alias(c)
    return fr


def calculate_fill_rate(df):
    '''
    Calculates the fill rate for a dataframe.
    Input:
        - df: a pyspark.sql.DataFrame
    Output:
        fr_df: the fill rates for each column as a pyspark.sql.DataFrame
    '''
    row_count = df.count()

    # Emperical data shows that 16 columns leads to optimal performance
    BATCH_SIZE = 16

    i = 0
    res = []
    while i < len(df.columns):
        df_tmp = df.agg(*[_col_fill_rate(c, row_count) for c in df.columns[i:i+BATCH_SIZE]]).cache()
        res += reduce(
            lambda df1, df2: df1.union(df2),
            [df_tmp.select(lit(c).alias('field'), col(c).alias('fill')) for c in df_tmp.columns]
        ).collect()
        df_tmp.unpersist()
        i += BATCH_SIZE

    stats = map(lambda r: {'field': r.field, 'fill': r.fill}, res)

    return stats
