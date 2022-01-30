"""
quest labtests schema
"""
from spark.helpers.source_table import SourceTable

TABLE_CONF = {
    'txn': SourceTable(
        'csv',
        separator='\t',
        trimmify_nullify=True,
        columns=[
            'accn_id',
            'dosid',
            'local_order_code',
            'standard_order_code',
            'order_name',
            'loinc_code',
            'local_result_code',
            'result_name',
            'lab_id',
            'date_of_service',
            'date_collected',
            'diagnosis_code',
            'icd_codeset_ind'
        ]
    )
}
