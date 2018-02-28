from pyspark.sql.functions import col, trim, udf, collect_list, when

def select_distinct_values_from_column(column_name):
    '''
    For each distinct value in the column column_name,
    Create a row with the distinct value and a list for each
    other column containing all the values from rows where
    column_name == distinct value.  Any empty list
    will be converted to None.
    Input:
        column_name: string name of the column you want distinct rows of
    Output:
        out: function that inputs a dataframe and returns the distinct rows
             for each distinct value in the column_name column
    '''
    def out(df):
        is_not_null = lambda c: col(c).isNotNull() & (trim(col(c)) != '')
        non_distinct_columns = list(filter(lambda x: x != column_name, df.columns))
        # Nullify all possible "empty" values
        mapped_df = df.withColumn(column_name, col(column_name))
        for c in non_distinct_columns:
            mapped_df = mapped_df.withColumn(c, when(is_not_null(c), col(c)).otherwise(None))
        # Group by the distinct column and aggregate each column together
        # as a list
        mapped_df = mapped_df.groupBy(column_name) \
                      .agg(*[collect_list(c).alias(c) for c in non_distinct_columns])
        # Convert empty lists to None
        remove_empty_list_udf = udf(lambda x: None if len(x) == 0 else x)
        for c in non_distinct_columns:
            mapped_df = mapped_df.withColumn(c, remove_empty_list_udf(col(c)))
        return mapped_df

    return out


def select_data_in_date_range(start_date, end_date, date_column_name, include_nulls=False):
    '''
    Filters the dataframe to contains rows between the inputed date range
    for the specified date column
    Input:
        - start_date: string of form YYYY-mm-dd
        - end_date: string of form YYYY-mm-dd
        - date_column_name: string specifying which column to filter by
                            for the date range

    Output:
        - out: function that inputs a dataframe and returns a dataframe
               within the specified date range
    '''
    def out(df):
        is_in_range = ((col(date_column_name) >= start_date) & (col(date_column_name) < end_date))
        if include_nulls:
            is_in_range = is_in_range | (col(date_column_name).isNull() | (trim(col(date_column_name)) == ''))

        limited_date_df = df.filter(is_in_range)
        return limited_date_df

    return out


def get_provider_data(sqlContext, datatype, provider_name):
    '''
    Fetch all the provider data for a provider from the warehouse.
    Input:
        - sqlContext:       Spark SqlContext for accessing warehouse
        - datatype:         The datatype for the provider (i.e. pharmacyclaims, events, etc...)
        - provider_name:    The name of the provider in the part_provider partition
    Output:
        - df:   pyspark.sql.DataFrame of the data
    '''
    df = sqlContext.sql("SELECT * FROM {} WHERE part_provider='{}'" \
                        .format(datatype, provider_name))
    return df
