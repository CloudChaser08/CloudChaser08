from pyspark.sql.functions import col, trim, udf, collect_list, when, broadcast
from functools import reduce


def select_distinct_values_from_column(column_name):
    """
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
    """
    def out(df):
        is_not_null = lambda c: col(c).isNotNull() & (trim(col(c)) != '')
        non_distinct_columns = [x for x in df.columns if x != column_name]
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
    """
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
    """
    def out(df):
        is_in_range = ((col(date_column_name) >= start_date) & (col(date_column_name) < end_date))
        if include_nulls:
            is_in_range = is_in_range | (col(date_column_name).isNull() | (trim(col(date_column_name)) == ''))

        limited_date_df = df.filter(is_in_range)
        return limited_date_df

    return out


def select_data_sample_in_date_range(start_date, end_date, date_column_name, include_nulls=False, max_sample_size=7000000, record_field=None):
    """
    Filters the dataframe to contains rows between the inputed date range
    for the specified date column
    Input:
        - start_date: string of form YYYY-mm-dd
        - end_date: string of form YYYY-mm-dd
        - date_column_name: string specifying which column to filter by
                            for the date range
        - include_nulls: Flag for whether or not to include nulls in the result set
        - max_sample_size: Maximum number of records to include in the sample
        - record_field: The field name containing record ids, in case we want the
                        calculation based on the number of records rather than the
                        number of rows

    Output:
        - out: function that inputs a dataframe and returns a (1) multiplication factor
               for projecting how much data should be in the full set based on the sample
               and (2) dataframe with a random sample within the specified date range
    """
    def out(df):
        limited_date_df = select_data_in_date_range(start_date, end_date, date_column_name, include_nulls)(df)
        if record_field:
            distinct_records = limited_date_df.select(record_field).distinct()
            total_count = distinct_records.count()
            if total_count:
                fraction = float(min(max_sample_size, total_count)) / float(total_count)
            else:
                fraction = 0.0
            records_sample = distinct_records.sample(False, fraction)

            sample = records_sample.alias('sample')
            full = limited_date_df.alias('full')
            # Broadcast sample to avoid slow mergesort joins
            res = full.join(broadcast(sample), col('full.{}'.format(record_field)) == col('sample.{}'.format(record_field)), 'leftsemi')
        else:
            total_count = limited_date_df.count()
            if total_count:
                fraction = float(min(max_sample_size, total_count)) / float(total_count)
            else:
                fraction = 0.0
            res = limited_date_df.sample(False, fraction)

        if fraction:
            return 1.0 / fraction, res
        else:
            return 0.0, res

    return out


def get_provider_data(sqlContext, table_name, provider_partition, custom_schema=None, custom_table=None):
    """
    Fetch all the provider data for a provider from the warehouse.
    Input:
        - sqlContext:                   Spark SqlContext for accessing warehouse
        - table_name:                   The table_name for the data (i.e. pharmacyclaims, events, etc...)
        - provider_partition:           The name of the provider partition
        - custom_schema (Optional):     The custom schema where the data can be found
        - custom_table (Optional):      The custom table where the data can be found
    Output:
        - df:   pyspark.sql.DataFrame of the data
    """
    table_in_new_schema = table_name.startswith(('emr', 'pharmacyclaims', 'medicalclaims'))

    data_schema = custom_schema if custom_schema else 'dw' if table_in_new_schema else 'default'
    data_table = custom_table if custom_table else 'hvm_' + table_name if table_in_new_schema else table_name
    df = sqlContext.sql(
        "SELECT * FROM {}.{} WHERE {}='{}'".format(
            data_schema, data_table,
            'part_hvm_vdr_feed_id' if table_name.startswith('emr') else 'part_provider',
            provider_partition
        )
    )
    return df


def get_emr_union(sqlContext, model_confs, provider_id):
    return reduce(
        lambda df1, df2: df1.union(df2),
        [
            sqlContext.sql('''
                SELECT hvid, hv_enc_id, coalesce({}) as coalesced_emr_date
                FROM dw.hvm_{}
                WHERE part_hvm_vdr_feed_id='{}'
            '''.format(
                ','.join(model_conf.date_fields), model_conf.datatype, provider_id
            )) for model_conf in model_confs
        ]
    )
