from spark.helpers.source_table import PayloadTable

TABLE_CONF = {
    'cardinal_pds': PayloadTable(extra_columns=["errors"])
}
