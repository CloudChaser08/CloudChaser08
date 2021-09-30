"""
schema
"""
from spark.helpers.source_table import SourceTable

TABLE_CONF = {
    'txn': SourceTable(
        'csv',
        separator='|',
        columns=[
            'first_name',
            'last_name',
            'date_of_birth',
            'zip5',
            'gender',
            'ethnicity',
            'race',
            'year_of_birth',
            'patient_zip3',
            'patient_city',
            'patient_state',
            'care_site_name',
            'care_site_zip',
            'physician_name',
            'physician_npi',
            'test_type_name',
            'sample_collection_date',
            'report_creation_date',
            'payor_name',
            'target_name',
            'result',
            'requisition_id',
            'lab_id',
            'instrument',
            'kit_name',
            'loinc_code',
            'column28',
            'column29',
            'hvjoinkey'
        ]
    )
}
