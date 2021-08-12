from spark.helpers.source_table import SourceTable

TABLE_CONF = {
    'txn': SourceTable(
        'csv',
        separator='|',
        confirm_schema=True,
        columns=[
            'claim_id',
            'token1',
            'token2'
        ]
    )
}
