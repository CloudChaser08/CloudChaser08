"""
schema
"""
from spark.helpers.source_table import SourceTable

TABLE_CONF = {
    'txn': SourceTable(
        'csv',
        separator='|',
        columns=[
            'patient_first_name',
            'patient_last_name',
            'patient_id',
            'gender',
            'street',
            'city',
            'state',
            'zip_code',
            'dob',
            'phone_number',
            'registry',
            'disease_activity',
            'tnf_exposure',
            'npi',
            'scipher_practice_account_number',
            'performing_lab',
            'sample_collection_date',
            'crp',
            'crp_less_greater_than_threshold',
            'crp_loinc',
            'anti_ccp',
            'anti_ccp_less_greater_than_threshold',
            'anti_ccp_loinc',
            'bmi',
            'sample_token',
            'non_response_signal',
            'prism_ra_score',
            'prism_ra_raw_score',
            'unknown_column',
            'hvjoinkey'
        ]
    )
}
