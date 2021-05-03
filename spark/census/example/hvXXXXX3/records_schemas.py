from spark.helpers.source_table import SourceTable

TABLE_CONF = {
    'records': SourceTable(
        'csv',
        separator='|',
        columns=[
            'claim_id',
            'date_of_service',
            'icd_code'
        ]
    )
}
