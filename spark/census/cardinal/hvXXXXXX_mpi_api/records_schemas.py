"""
cardinal hvXXXX records schemas
"""
from spark.helpers.source_table import SourceTable

TABLE_CONF = {
    'cardinal_mpi_api_transactions': SourceTable(
        'csv',
        separator='|',
        columns=[
            'deid_payload',
            'job_id',
            'client_id',
            'callback_data'
        ]
    )
}
