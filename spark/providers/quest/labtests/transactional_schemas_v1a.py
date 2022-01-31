"""
quest labtests schema
"""
from spark.helpers.source_table import SourceTable

TABLE_CONF = {
    'transactions_provider_addon': SourceTable(
        'csv',
        trimmify_nullify=True,
        separator='\t',
        columns=[
            'accn_id',
            'dosid',
            'lab_code',
            'acct_zip',
            'npi'
        ]
    )
}