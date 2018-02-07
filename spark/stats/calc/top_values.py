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

    # Group the DataFrame by the column we want to calculate
    # top values for.
    result_df = df.withColumnRenamed(c, 'col') \
                  .groupBy('col')
    # Aggregate the DataFrame based on whether or not
    # to count based on a distinct value or not
    if (distinct_column):
        result_df = result_df.agg(countDistinct(col(distinct_column)).alias('count'))
    else:
        result_df = result_df.count()
    # Build the output from the aggregation
    return result_df.withColumn('name', lit(c)) \
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
    if distinct_column:
        columns = df.drop(distinct_column).columns
    else:
        columns = df.columns
    if len(columns) == 0:
        raise ValueError('Dataframe with no columns passed in for top values calculation')

    BATCH_SIZE = 10
    i = 0
    top_values_res = []
    while i < len(columns):
        top_values_res += reduce(
            lambda df1, df2: df1.union(df2),
            [_col_top_values(df, c, max_top_values, distinct_column) for c in columns[i:i+BATCH_SIZE]]
        ).collect()
        i = i + BATCH_SIZE

    stats = map(lambda r: {'column': r.name, 'value': r.col, 'count': r['count']}, top_values_res)

    return stats


