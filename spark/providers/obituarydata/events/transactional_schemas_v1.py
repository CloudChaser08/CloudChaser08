"""
schema
"""
from spark.helpers.source_table import SourceTable

TABLE_CONF = {
    'txn': SourceTable(
        'csv',
        separator='|',
        confirm_schema=True,
        columns=[
            'id',
            'last_name',
            'first_name',
            'middle_init',
            'date_of_death',
            'date_of_birth',
            'age',
            'suffix',
            'city',
            'state',
            'date_of_record_entry',
            'date_of_record_update',
            'hv_join_key'
        ]
    )
}
