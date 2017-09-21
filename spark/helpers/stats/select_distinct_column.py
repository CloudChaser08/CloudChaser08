from pyspark.sql.functions import col, isnan, trim, udf, collect_list, when
from pyspark.sql.types import ArrayType, StringType

def _convert_values(c):
    '''
    Map values in a column to 1 if not a null value, 0 if a null value
    Input:
        - col: dataframe column of type pyspark.sql.Column
    Output:
        - mapped: column of type pyspark.sql.Column
    '''

    is_not_null = col(c).isNotNull() & ~isnan(c) & (trim(col(c)) != '')
    mapped = is_not_null.cast("integer").alias(c)
    return mapped


def select_distinct_column(distinct_column_name):
    '''
    Select a row for each distinct value distinct_column_name.
    Each field in the row that is not the distinct_column_name
    will be NULL if none of the rows for a given distinct column
    value are not NULL, else it will be a non-negative number
    Input:
        distinct_column_name: string name of the column you want distinct rows of
    Output:
        out: function that inputs a dataframe and returns the distinct rows
             for each distinct value in the distinct_column_name column
    '''
#    def out(df):
#        mapped_df = df.select(col(distinct_column_name),
#        *[ _convert_values(c) for c in df.columns if c != distinct_column_name]) \
#                .groupBy(distinct_column_name).sum() \
#                .toDF(*df.columns).na.replace(0, '', 
#                    list(filter(lambda x: x != distinct_column_name, df.columns)))
#        return mapped_df
    def out(df):
        is_not_null = lambda c: col(c).isNotNull() & ~isnan(c) & (trim(col(c)) != '')
        non_distinct_columns = list(filter(lambda x: x != distinct_column_name, df.columns))
        # Nullify all possible "empty" values
        mapped_df = df.withColumn(distinct_column_name, col(distinct_column_name))
        for c in non_distinct_columns:
            mapped_df = mapped_df.withColumn(c, when(is_not_null(c), col(c)).otherwise(None))
        # Group by the distinct column and aggregate each column together 
        # as a list
        mapped_df = mapped_df.groupBy(distinct_column_name) \
                      .agg(*[collect_list(c).alias(c) for c in non_distinct_columns])
        # Convert empty lists to None
        remove_empty_list_udf = udf(lambda x: None if len(x) == 0 else x)
        for c in non_distinct_columns:
            mapped_df = mapped_df.withColumn(c, remove_empty_list_udf(col(c)))
        return mapped_df

    return out


