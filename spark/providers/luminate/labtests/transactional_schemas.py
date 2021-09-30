"""
Luminate Transactional schema
"""
from spark.helpers.source_table import SourceTable

TABLE_CONF = {
    'txn': SourceTable(
        'csv',
        separator='|',
        confirm_schema=True,
        columns=[
            'first_name',
            'last_name',
            'dob',
            'zipcode',
            'test_name',
            'date_of_service',
            'result',
            'abnormal_flags',
            'hvjoinkey'
        ]
    )
}
