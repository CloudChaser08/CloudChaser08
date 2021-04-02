from spark.helpers.source_table import SourceTable

TABLE_CONF = {
    'liquidhub_raw': SourceTable(
        'csv',
        separator='|',
        columns=[
            'records_code',
            'last_name',
            'first_name',
            'filler_1',
            'patient_zip_code',
            'date_of_birth',
            'gender',
            'filler_2',
            'source_name',
            'source_patient_id',
            'manufacturer',
            'brand',
            'filler_3',
            'filler_4',
            'filler_5',
            'hvjoinkey'
        ]
    ),
}
