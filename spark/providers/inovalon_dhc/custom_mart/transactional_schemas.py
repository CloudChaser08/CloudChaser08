"""
inovalon dhc source schema
"""
from spark.helpers.source_table import SourceTable

TABLE_CONF = {
    'upk': SourceTable(
        'csv',
        separator='|',
        confirm_schema=True,
        columns=[
            'upk_key_1',
            'upk_key_2',
            'memberuid'
        ]
    )
}
