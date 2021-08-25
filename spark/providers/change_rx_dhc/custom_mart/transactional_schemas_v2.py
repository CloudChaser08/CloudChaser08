from spark.helpers.source_table import SourceTable

TABLE_CONF = {
    'txn': SourceTable(
        'csv',
        separator='|',
        confirm_schema=True,
        columns=[
            'rx_transaction_id',
            'column2',
            'column3',
            'column4',
            'column5',
            'column6',
            'column7',
            'column8',
            'column9',
            'column10',
            'column11',
            'column12',
            'column13',
            'column14',
            'column15',
            'column16',
            'column17',
            'column18',
            'token_1',
            'token_2'
        ]
    )
}
