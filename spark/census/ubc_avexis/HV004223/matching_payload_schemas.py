from spark.helpers.source_table import PayloadTable

TABLE_CONF = {
    'matching_payload': PayloadTable(extra_columns=['privateIdOne'])
}
