"""
CDC Helix Genomics Schema
"""
from spark.helpers.source_table import SourceTable

TABLE_CONF = {
    'txn': SourceTable(
        'csv',
        separator='|',
        trimmify_nullify=True,
        columns=[
            'claimid',
            'unknown_column2',
            'unknown_column3',
            'unknown_column4',
            'unknown_column5',
            'hvjoinkey'
        ]
    )
}
