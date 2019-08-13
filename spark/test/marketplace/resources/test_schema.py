from spark.helpers.source_table import SourceTable

TABLE_CONF = {
    'test_claims' : SourceTable(
        'csv',
        separator='|',
        columns=[
            'hvid',
            'claimID',
            'test3',
            'test4',
            'test5'
        ]
    )
}

