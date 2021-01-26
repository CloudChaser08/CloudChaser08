"""
HV000710 HC1/MPH matching payload table definitions
"""
from spark.helpers.source_table import PayloadTable

TABLE_CONF = {
    'hc1': PayloadTable(),
    'mph': PayloadTable()
}
