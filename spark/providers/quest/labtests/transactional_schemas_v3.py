"""
quest labtests schema
"""
from spark.helpers.source_table import SourceTable

TABLE_CONF = {
    'trunk': SourceTable(
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
            'result_name'
        ]
    ),
    'addon': SourceTable(
        'csv',
        separator='\t',
        trimmify_nullify=True,
        columns=[
            'accn_id',
            'date_of_service',
            'dosid',
            'lab_id',
            'date_collected',
            'patient_first_name',
            'patient_middle_name',
            'patient_last_name',
            'address1',
            'address2',
            'city',
            'state',
            'zip_code',
            'date_of_birth',
            'patient_age',
            'gender',
            'diagnosis_code',
            'icd_codeset_ind',
            'acct_zip',
            'npi',
            'hv_join_key'
        ]
    )
}
