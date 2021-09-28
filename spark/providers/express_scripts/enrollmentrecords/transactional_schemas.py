"""
express scripts enr source schema
"""
from spark.helpers.source_table import SourceTable

TABLE_CONF = {
    'transaction': SourceTable(
        'fixedwidth',
        trimmify_nullify=True,
        columns=[
            ('patient_id', 15),
            ('start_date', 8),
            ('end_date', 8),
            ('operation_date', 8),
            ('status', 1),
            ('hvjoinkey', 36)
        ]
    )
}
