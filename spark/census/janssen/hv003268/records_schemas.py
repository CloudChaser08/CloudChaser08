'''
Janseen HV003268 record table definitions (eConsent and StudyHub)
'''
from spark.helpers.source_table import SourceTable

TABLE_CONF = {
    'econsent': SourceTable(
        'csv',
        separator='|',
        columns=[
            'study_id',
            'subject_id',
            'full_name',
            'dob_of_birth',
            'site_id',
            'site_number',
            'consent_indicator',
            'pii_consent_indicator',
            'hv_join_key'
        ]
    ),
    'studyhub': SourceTable(
        'csv',
        separator='|',
        columns=[
            'study_id',
            'subject_id',
            'subject_number',
            'first_name',
            'last_name',
            'dob_of_birth',
            'zip',
            'site_id',
            'site_number',
            'hv_join_key'
        ]
    )
}
