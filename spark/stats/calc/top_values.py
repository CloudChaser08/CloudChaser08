from pyspark.sql.functions import col, countDistinct, lit 

def _col_top_values(df, c, num, distinct_column=None):
    '''
    Calculate the top values for a given column
    Input:
        - df: the dataframe to calculate the top value on.
        - c: dataframe column of type pyspark.sql.Column
        - num: number of top values to get.
        - distinct_column: column name of column to count distinct values of
                           for group counts.
    Output:
        - tv: the top values for that column in descending order
              along with the count
            
              i.e. 
              +----------------+-----------+--------------+
              |      name      |    col    |     count    |
              +----------------+-----------+--------------+
              |      name      |   val_1   |      450     |
              |      name      |   val_2   |      400     |
              |      name      |   val_3   |      200     |
              |      name      |   val_4   |       27     |
              |       ...      |    ...    |      ...     |
              |      name      |    val_n  |       1      |
              +----------------+--------------------------+
    '''

    if (distinct_column):
        return df.withColumnRenamed(c, 'col') \
                 .groupBy('col') \
                 .agg(countDistinct(col(distinct_column)).alias('count')) \
                 .withColumn('name', lit(c)) \
                 .select('name', 'col', 'count') \
                 .dropna(subset=['col']) \
                 .sort(col('count').desc()) \
                 .limit(num)
    else:
        return df.withColumnRenamed(c, 'col') \
                 .groupBy('col') \
                 .count() \
                 .withColumn('name', lit(c)) \
                 .select('name', 'col', 'count') \
                 .dropna(subset=['col']) \
                 .sort(col('count').desc()) \
                 .limit(num)


def calculate_top_values(df, max_top_values, distinct_column=None):
    '''
    Calculate the top values of a dataframe
    Input:
        - df: a pyspark.sql.DataFrame
        - max_top_values: the max number of values to 
                          store for each column
        - distinct_column: if not None, consider COUNT(DISTINCT distinct_column)
                           as the count for top values
    Output:
        - tv_df: a pyspark.sql.DataFrame of each columns top values
                 and its associated counts
    '''

    columns = df.columns
    if len(columns) == 0:
        raise Exception('Dataframe with no columns passed in for top values calculation')

    col = columns[0]
    tv_df = _col_top_values(df, col, max_top_values, distinct_column)
    for col in columns[1:]:
        ctv_df = _col_top_values(df, col, max_top_values, distinct_column)
        tv_df = tv_df.union(ctv_df)

    return tv_df


