from spark.helpers.source_table import SourceTable

TABLE_CONF = {
    'txn': SourceTable(   
        'csv',
        separator='|',
        columns=[
            'concat_unique_fields',
            'column_2',
            'column_3',
            'column_4',
            'column_5',
            'column_6',
            'column_7',
            'column_8',
            'column_9',
            'column_10',
            'column_11',
            'column_12',
            'column_13',
            'column_14',
            'column_15',
            'column_16',
            'column_17',
            'column_18',
            'column_19',
            'column_20',
            'date_filled',
            'rx_number',
            'cob_count',
            'pharmacy_ncpdp_number',
            'claim_indicator',
            'ndc_code',
            'datavant_token1',
            'datavant_token2'
        ]
    )
}
