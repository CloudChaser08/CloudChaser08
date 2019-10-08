from spark.helpers.source_table import SourceTable

TABLE_CONF = {
    'clm': SourceTable(
        'csv',
        separator='|',
        columns=[
            'patient_id',
            'claim_id',
            'account_id',
            'admit_date_of_service',
            'discharge_date',
            'patient_status',
            'emergent_status',
            'facility_id',
            'facility_zip',
            'medicare_provider_number',
            'admission_source_code',
            'admission_type_code',
            'patient_status_code',
            '340b_id',
            'bill_type',
            'imported_on',
            'hvjoinkey'
        ]
    ),
    'dgn': SourceTable(
        'csv',
        separator='|',
        columns=[
            'patient_id',
            'claim_id',
            'diagnosis_code',
            'diagnosis_description',
            'hvjoinkey'
        ]
    ),
    'dsp': SourceTable(
        'csv',
        separator='|',
        columns=[
            'patient_id',
            'dispensation_id',
            'service_date',
            'charge_amount',
            'ndc',
            'ndc_drug_description',
            'molecule',
            'brand_generic_name',
            'dispensation_quantity',
            'hcpcs_quantity',
            'hcpcs_code',
            'hvjoinkey'
        ]
    ),
    'lin': SourceTable(
        'csv',
        separator='|',
        columns=[
            'patient_id',
            'claim_id',
            'charge_date',
            'charge_quantity',
            'charge_amount',
            'charge_type',
            'code',
            'code_description',
            'hvjoinkey'
        ]
    ),
    'loc': SourceTable(
        'csv',
        separator='|',
        columns=[
            'patient_id',
            'claim_id',
            'location_code',
            'date_time',
            'hvjoinkey'
        ]
    ),
    'prv': SourceTable(
        'csv',
        separator='|',
        columns=[
            'patient_id',
            'claim_id',
            'npi_number',
            'physician_role',
            'hvjoinkey'
        ]
    )
}