from spark.helpers.source_table import PayloadTable

TABLE_CONF = {
    'foo' : PayloadTable(extra_columns=['my_special_column'])
}
