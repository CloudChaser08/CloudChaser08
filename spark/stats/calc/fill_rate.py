from pyspark.sql.functions import col, sum, isnan, count, trim

def _col_fill_rate(c):
    '''
    Calculate the fill rate for a given column.
    "null" values can be the following:
        - NULL
        - ''
        - ' '
        - NaN
    Input:
        - col: dataframe column of type pyspark.sql.Column
    Output:
        - fr: the fill rate for that column of type pyspark.sql.Column
    '''

    row_count = count(col(c))
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

    fr_df = df.agg(*[_col_fill_rate(c) for c in df.columns])
    return fr_df


