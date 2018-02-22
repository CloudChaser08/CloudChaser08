from pyspark.sql.functions import col, lit

def apply_schema(df, schema, columns_to_fill=None):
    return apply_schema_func(schema, columns_to_fill)(df)

def apply_schema_func(schema, cols_to_fill=None):
    '''
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
            columns_to_fill (optional) - A list of the columns in the schema
                                         that are populated in input DataFrame
        output
            DataFrame with the schema applied. Any columns not specified in the
            input DataFrame are filld with nulls
    '''

    def out(df):
        columns_to_fill = cols_to_fill
        if not columns_to_fill:
            columns_to_fill = []

        for c in columns_to_fill:
            if not c in schema.names:
                raise ValueError("Column {} is not part of the schema".format(c))

        if len(df.columns) != len(schema) and not columns_to_fill:
            for c in df.columns:
                if not c in schema.names:
                        raise ValueError("Column {} is not part of the schema".format(c))

            columns_to_fill = df.columns

        if not columns_to_fill:
            columns_to_fill = schema.names

        new_columns = []
        for field in schema:
            if field.name in columns_to_fill:
                col_value = col(df.columns[columns_to_fill.index(field.name)])
            else:
                col_value = lit(None)
            new_columns.append(col_value.cast(field.dataType).alias(field.name))

        return df.select(*new_columns)

    return out
