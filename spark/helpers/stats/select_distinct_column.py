from pyspark.sql.functions import col, isnan, trim
from pyspark.sql.types import IntegerType

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
    def out(df):
        mapped_df = df.select(col(distinct_column_name),
        *[ _convert_values(c) for c in df.columns if c != distinct_column_name]) \
                .groupBy(distinct_column_name).sum() \
                .toDF(*df.columns).na.replace(0, '', 
                    list(filter(lambda x: x != distinct_column_name, df.columns)))
        return mapped_df


    return out


