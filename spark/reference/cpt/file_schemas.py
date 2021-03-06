from spark.helpers.source_table import SourceTable

TABLE_CONF = {
    'long': SourceTable(
        'csv',
        separator='\t',
        columns=[
            'long_code',
            'long_description'
        ]
    ),
    'short': SourceTable(
        'csv',
        separator='\t',
        columns=[
            'short_code',
            'short_description'
        ]
    ),
    'pla': SourceTable(
        'csv',
        separator='\t',
        columns=[
            'pla_code',
            'long_description',
            'medium_description',
            'short_description',
            'clinical_description',
            'consumer_description',
            'published_date',
            'effective_date',
            'test_name',
            'lab_name',
            'manufacturer_name'
        ]
    ),
    'mod': SourceTable(
        'csv',
        separator='\t',
        columns=[
            'mod_code',
            'long_description'
        ]
    )
}
