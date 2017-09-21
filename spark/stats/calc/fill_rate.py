from pyspark.sql.functions import col, sum, isnan, count, trim

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
    is_not_null = col(c).isNotNull() & ~isnan(c) & (trim(col(c)) != '')
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
    fr_df = df.agg(*[_col_fill_rate(c, row_count) for c in df.columns])
    return fr_df


