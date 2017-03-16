from datetime import timedelta
from pyspark.sql import Row


def explode_dates(
        runner, table, date_start_column, date_end_column,
):
    DateRow = Row()

    def replace_dates_in_row(row, date):
        global DateRow
        if DateRow == Row():
            DateRow = Row(*row.asDict().keys())

        return DateRow(
            *map(
                lambda key: date if key in [
                    date_start_column, date_end_column
                ] else row[key],
                row.asDict().keys()
            )
        )

    def explode(row):
        return map(lambda i: replace_dates_in_row(
            row,
            getattr(row, date_start_column) + timedelta(i)
        ), range(int((
            getattr(row, date_end_column) -
            getattr(row, date_start_column)
        ).days)))

    runner.run_spark_query((
        "SELECT * "
        + "FROM {table} "
        + "WHERE datediff("
        + "date_format({date_end}, 'YYYY-MM-dd'), "
        + "date_format({date_start}, 'YYYY-MM-dd')"
        + ") BETWEEN 0 AND 365"
    ).format(
        table=table,
        date_start=date_start_column,
        date_end=date_end_column
    ), True).rdd.flatMap(explode).toDF(DateRow.columns).union(
        runner.run_spark_query((
            "SELECT * "
            + "FROM {table} "
            + "WHERE datediff("
            + "date_format({date_end}, 'YYYY-MM-dd'), "
            + "date_format({date_start}, 'YYYY-MM-dd')"
            + ") NOT BETWEEN 0 AND 365"
        ).format(
            table=table,
            date_start=date_start_column,
            date_end=date_end_column
        ), True)).registerTempTable('{table}_temp'.format(table=table))

    runner.run_spark_query("DROP TABLE {table}".format(table=table))
    runner.run_spark_query(
        "ALTER TABLE {table}_temp RENAME TO {table}".format(table=table)
    )
