# Functions to be applied to all columns in a dataframe

from pyspark.sql.functions import col, lit, when, trim

def _apply_to_all_columns(f, df):
    return df.select(*map(f, df.columns))


def nullify(df, null_val=''):
    "Convert all columns matching null_val to null"
    def convert_to_null(column_name):
        return when(col(column_name) == null_val, lit(None)) \
            .otherwise(col(column_name)).alias(column_name)

    return _apply_to_all_columns(convert_to_null, df)


def trimmify(df):
    "Trim all string columns"
    def get_type(col_name):
        return str(filter(
            lambda f: f.name == col_name,
            df.schema.fields
        )[0].dataType)

    def trim_col(column_name):
        if get_type(column_name) == 'StringType':
            return trim(col(column_name))
        else:
            return col(column_name)

    return _apply_to_all_columns(trim_col, df)
