from pyspark.sql.functions import col

def limit_date_range(start_date, end_date, date_column_name):
    def out(df):
        is_in_range = (col(date_column_name) >= start_date) & (col(date_column_name) <= end_date)
        limited_date_df = df.filter(is_in_range)
        return limited_date_df


    return out


