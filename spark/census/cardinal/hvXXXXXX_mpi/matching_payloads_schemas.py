"""
cardinal mpi payload schema
"""
from spark.helpers.source_table import PayloadTable

TABLE_CONF = {
    'cardinal_mpi': PayloadTable(extra_columns=["topCandidates"])
}
