"""
HV000710 HC1/MPH record table definitions
"""
from spark.helpers.source_table import SourceTable

TABLE_CONF = {
    'tests': SourceTable(
        'csv',
        separator='|',
        columns=[
            'result_id',
            'patient_last_name',
            'patient_first_name',
            'patient_middle_name',
            'patient_dob',
            'patient_ssn',
            'patient_mailing_address_street',
            'patient_mailing_address_street2',
            'patient_mailing_address_city',
            'patient_mailing_address_state',
            'patient_mailing_address_postal_code',
            'patient_age_group',
            'patient_id',
            'result_positivity',
            'result_mix',
            'patient_gender',
            'patient_county_code',
            'result_date_month',
            'lab_order_id',
            'provider_id',
            'location_id',
            'test_group',
            'drug_grouping',
            'loinc_component',
            'provider_specialty',
            'hvjoinkey'
        ]
    )
}
