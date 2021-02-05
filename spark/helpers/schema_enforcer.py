from pyspark.sql.functions import col, lit

def has_column(df, col):
    try:
        df[col]
        return True
    except AnalysisException:
        return False

def apply_schema(df, schema, columns_to_fill=None, columns_to_keep=None):
    return apply_schema_func(schema, columns_to_fill, columns_to_keep)(df)

def apply_schema_func(schema, cols_to_fill=None, cols_to_keep=None):
    """
        Apply/enforce a schema on df using one of 3 strategies:
            1. The input DataFrame column names match a subset of the schema
               column names
            2. The input DataFrame contains n columns, the schema contains N
               columns (where N >= n), the columns_to_fill list contains the
               names of the n columns from the schema that are populated by
               the input DataFrame
            3. The input DataFrame contains N columns, which matches the number
               of columns in the schema
        input
            df - DataFrame with an implicit schema (probably the result of a sql select)
            schema - The schema to enforce. A schema is a StructType object
                     made up of a sequence of StructFields
            cols_to_fill (optional) - A list of the columns in the schema
                                      that are populated in input DataFrame
            cols_to_keep (optional) - A list of columns that are not in the schema,
                                      but should be kept in the output dataframe
        output
            DataFrame with the schema applied. Any columns not specified in the
            input DataFrame are filld with nulls
    """

    def out(df):
        columns_to_fill = cols_to_fill
        if not columns_to_fill:
            columns_to_fill = []

        columns_to_keep = cols_to_keep
        if not columns_to_keep:
            columns_to_keep = []

        for c in columns_to_fill:
            if not c in schema.names:
                raise ValueError("Column {} is not part of the schema".format(c))

        if len(df.columns) - len(columns_to_keep) != len(schema) and not columns_to_fill:
            for c in df.columns:
                if c not in schema.names and not columns_to_keep and c not in columns_to_keep:
                    raise ValueError("Column {} is not part of the schema".format(c))

            columns_to_fill = df.columns

        if not columns_to_fill:
            columns_to_fill = schema.names

        new_columns = []
        for field in schema:
            if field.name in columns_to_fill:
                if has_column(df, field.name): # use the column name if it exists in df, and the column position if it does not
                    col_value = df[field.name]
                else:
                    col_value = col(df.columns[columns_to_fill.index(field.name)])
            else:
                col_value = lit(None)
            new_columns.append(col_value.cast(field.dataType).alias(field.name))

        return df.select(*(new_columns + columns_to_keep))

    return out
