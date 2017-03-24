from pyspark.sql.functions import explode, col, split, \
    monotonically_increasing_id


def explode_dates(
        runner, table, date_start_column, date_end_column, primary_key=None
):
    """
    This function will explode days into rows for all days between
    date_start_column and date_end_column on the given table

    If the optional `primary_key` param is provided, this function
    will also reset the primary_key to a new
    monotonically_increasing_id
    """
    # explode date start/end ranges that are less than 1 year apart
    # register as a temporary table in order to use date_add SQL function
    runner.run_spark_query((
        "SELECT *, "
        + "  create_range(datediff("
        + "    {date_end_column}, {date_start_column}"
        + "  ) + 1) as raw_range "
        + "FROM {table} "
        + "WHERE datediff("
        + "{date_end_column}, {date_start_column}"
        + ") BETWEEN 1 AND 365"
    ).format(
        table=table,
        date_start_column=date_start_column,
        date_end_column=date_end_column
    ), True).withColumn(
        'days_to_add',
        explode(split(col('raw_range'), ','))
    ).registerTempTable(table + '_exploded')

    # use date_add to create a new_date column that represents
    # date_start + some number from the explosion
    with_new_date = runner.run_spark_query((
        "SELECT *, date_add({date_start_column}, days_to_add) as new_date "
        + "FROM {table}_exploded "
    ).format(
        table=table,
        date_start_column=date_start_column,
    ), True)

    # replace date_start_column and date_end_column with new_date,
    # remove all columns that were added above, and union with the
    # rest of the table
    full_exploded_table = with_new_date.select(*map(
        lambda column: col('new_date').alias(column)
        if column in [date_start_column, date_end_column] else col(column),
        filter(
            lambda column: column not in ('new_date', 'raw_range', 'days_to_add'),
            with_new_date.columns
        )
    )).union(
        runner.run_spark_query((
            "SELECT * "
            + "FROM {table} "
            + "WHERE datediff("
            + "{date_end_column}, {date_start_column}"
            + ") NOT BETWEEN 1 AND 365 "
            + "OR {date_start_column} IS NULL "
            + "OR {date_end_column} IS NULL"
        ).format(
            table=table,
            date_start_column=date_start_column,
            date_end_column=date_end_column
        ), True)
    )

    # replace old pk with monotonically_increasing_id if necessary
    if primary_key:
        full_exploded_table = full_exploded_table.withColumn(
            primary_key, monotonically_increasing_id()
        )

    full_exploded_table.createTempView(
        '{table}_replacement'.format(table=table)
    )

    # replace old table with new
    runner.run_spark_query("DROP TABLE {table}".format(
        table=table
    ))
    runner.run_spark_query(
        "CREATE TABLE {table} AS SELECT * FROM {table}_replacement".format(
            table=table
        )
    )
