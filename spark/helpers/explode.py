from pyspark.sql.types import *
from pyspark.sql.functions import explode, col, split, \
    monotonically_increasing_id


def generate_exploder_table(spark, length, name='exploder'):
    """
    Generate a table with a single column (n) of incrementing integers
    from 0 to length to be used in cross joins for explosion

    Force the number of partitions to 1 to avoid large numbers of
    tasks on cross joins
    """
    spark.sparkContext.parallelize([[i] for i in range(0, length)], 1).toDF(
        StructType([StructField('n', LongType(), True)])
    ).registerTempTable(name)


def explode_medicalclaims_dates(runner):
    date_start_column = 'date_service'
    date_end_column = 'date_service_end'
    table = 'medicalclaims_common_model'
    primary_key = 'record_id'

    explosion_filter_condition = col('claim_type') == 'P'

    explode_dates(
        runner, table, date_start_column, date_end_column,
        primary_key, '365', explosion_filter_condition
    )


def explode_dates(
        runner, table, date_start_column, date_end_column,
        primary_key=None, max_days='365',
        explosion_filter_condition=None
):
    """
    This function will explode days into rows for all days between
    date_start_column and date_end_column on the given table

    This function will also reset the primary_key to a new
    monotonically_increasing_id

    """
    if explosion_filter_condition is None:
        explosion_filter_condition = (
            col(date_start_column) == col(date_start_column)
        )

    # explode date start/end ranges that are less than {max_days} days apart
    # register as a temporary table in order to use date_add SQL function
    runner.run_spark_query((
        "SELECT *, "
        + "  create_range(datediff("
        + "    {date_end_column}, {date_start_column}"
        + "  ) + 1) as raw_range "
        + "FROM {table} "
        + "WHERE datediff("
        + "{date_end_column}, {date_start_column}"
        + ") BETWEEN 1 AND {max_days}"
    ).format(
        table=table,
        date_start_column=date_start_column,
        date_end_column=date_end_column,
        max_days=max_days
    ), True).filter(explosion_filter_condition).withColumn(
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
    full_exploded_table = with_new_date.select(*[
        col('new_date').alias(column)
            if column in [date_start_column, date_end_column] else col(column)
        for column in
        [
            column for column in with_new_date.columns
            if column not in ('new_date', 'raw_range', 'days_to_add')
        ]
    ]).union(
        runner.run_spark_query((
            "SELECT * "
            + "FROM {table} "
            + "WHERE datediff("
            + "{date_end_column}, {date_start_column}"
            + ") NOT BETWEEN 1 AND {max_days} "
            + "OR {date_start_column} IS NULL "
            + "OR {date_end_column} IS NULL"
        ).format(
            table=table,
            date_start_column=date_start_column,
            date_end_column=date_end_column,
            max_days=max_days
        ), True)
    ).union(
        runner.run_spark_query((
            "SELECT * "
            + "FROM {table} "
            + "WHERE datediff("
            + "{date_end_column}, {date_start_column}"
            + ") BETWEEN 1 AND {max_days}"
        ).format(
            table=table,
            date_start_column=date_start_column,
            date_end_column=date_end_column,
            max_days=max_days
        ), True).filter(~explosion_filter_condition)
    )

    # replace old pk with monotonically_increasing_id
    if primary_key:
        full_exploded_table = full_exploded_table.withColumn(
            primary_key, monotonically_increasing_id()
        )

    # replace old table with new
    full_exploded_table.createOrReplaceTempView(table)
