from pyspark.sql.functions import col

def limit_date_range(start_date, end_date, date_column_name):
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
        is_in_range = (col(date_column_name) >= start_date) & (col(date_column_name) <= end_date)
        limited_date_df = df.filter(is_in_range)
        return limited_date_df


    return out


