"""
labcorp results source schema
"""
from spark.helpers.source_table import SourceTable

TABLE_CONF = {
    'txn': SourceTable(
        'csv',
        separator='|',
        confirm_schema=True,
        columns=[
            'patient_first_name'
            , 'patient_last_name'
            , 'patient_dob'
            , 'patient_sex'
            , 'patient_zip5'
            , 'patient_street1'
            , 'patient_street2'
            , 'patient_city'
            , 'patient_state'
            , 'patient_memberid'
            , 'patient_groupid'
            , 'patient_ID'
            , 'specimen_number'
            , 'hvjoinkey'
        ]
    ),
    'results': SourceTable(
        'csv',
        separator='|',
        confirm_schema=True,
        columns=[
            'test_name',
            'test_number',
            'loinc_code',
            'normal_dec_low',
            'normal_dec_high',
            'result_dec',
            'result_abn_code',
            'result_abbrv',
            'result_date',
            'pat_dos',
            'perf_lab_code',
            'rslt_comments',
            'test_ordered_code',
            'test_ordered_name',
            'npi',
            'specialty_code',
            'report_zip',
            'specimen_number',
            'icd_code_1',
            'icd_code_ind_1',
            'icd_code_2',
            'icd_code_ind_2',
            'icd_code_3',
            'icd_code_ind_3',
            'icd_code_4',
            'icd_code_ind_4',
            'icd_code_5',
            'icd_code_ind_5',
            'icd_code_6',
            'icd_code_ind_6'
        ]
    )
}


