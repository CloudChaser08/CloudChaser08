from pyspark.sql.functions import col, lit, when


def nullify(df, null_val=''):
    def convert_to_null(column_name):
        return when(col(column_name) == null_val, lit(None)) \
            .otherwise(col(column_name)).alias(column_name)

    return df.select(*map(convert_to_null, df.columns))
