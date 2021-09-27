"""
cardinal hvXXXXXX mpi api matching payloads
"""
from spark.helpers.source_table import PayloadTable

TABLE_CONF = {
    'cardinal_api': PayloadTable(extra_columns=["errors"])
}
