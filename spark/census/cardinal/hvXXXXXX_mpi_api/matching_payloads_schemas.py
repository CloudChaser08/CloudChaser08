"""
cardinal mpi api payload schema
"""
from spark.helpers.source_table import PayloadTable

TABLE_CONF = {
    'cardinal_api': PayloadTable(extra_columns=["errors"])
}
