from spark.helpers.source_table import SourceTable

TABLE_CONF = {
    'txn': SourceTable(
        'csv',
        separator='|',
        columns=[
            'deidentified_id',
            'patient_last_name',
            'patient_first_name',
            'date_of_birth',
            'gender',
            'patient_zip',
            'patient_state',
            'order_date',
            'genes_tested',
            'contact_name',
            'npi',
            'organization_name',
            'organization_address',
            'contact_email',
            'office_phone',
            'organization_state',
            'organization_zip',
            'icd_10_codes',
            'hvjoinkey'
        ]
    )
}