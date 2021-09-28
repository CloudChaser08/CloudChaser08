"""
Janssen HV003628 payload table definition
"""
from spark.helpers.source_table import PayloadTable

TABLE_CONF = {
    'studyhub': PayloadTable(['eConsentHVID']),
    'econsent': PayloadTable()
}
