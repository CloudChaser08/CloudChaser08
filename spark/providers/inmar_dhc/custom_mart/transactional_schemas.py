from spark.helpers.source_table import SourceTable

TABLE_CONF = {
    'txn': SourceTable(
        'csv',
        separator='|',
        confirm_schema=True,
        columns=[
            'token_1',
            'token_2',
            'claim_id',
            'column4'
        ]
    )
}
