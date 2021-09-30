"""
8451 hvXXXXXX grocery matching payload
"""
from spark.helpers.source_table import PayloadTable

TABLE_CONF = {
    '8451_groccery_payloads': PayloadTable(['join_keys'])
}
