"""
change 837 schema v2
"""
from spark.helpers.source_table import SourceTable

TABLE_CONF = {
    'plainout': SourceTable(
        'csv',
        separator='|',
        columns=[
            'claim_number',
            'patient_zip',
            'patient_dob',
            'group_number',
            'patient_gender',
            'subscriber_dob',
            'patient_address_line2',
            'subscriber_lname',
            'subscriber_fname',
            'patient_address_line1',
            'patient_adr_city',
            'patient_id',
            'patient_fname',
            'patient_lname',
            'group_name',
            'patient_control',
            'subscriber_gender',
            'subscriber_id',
            'hvjoinkey'
        ]
    ),
    'claim': SourceTable(
        'csv',
        separator='|',
        columns=[
            'claim_tcn_id',
            'record_type',
            'payer_id',
            'coding_type',
            'received_date',
            'claim_type',
            'column_7',
            'payer_name',
            'column_9',
            'column_10',
            'column_11',
            'column_12',
            'column_13',
            'column_14',
            'member_birth_year',
            'column_16',
            'column_17',
            'column_18',
            'patient_state',
            'patient_zip3',
            'column_21',
            'patient_relationship_code',
            'column_23',
            'column_24',
            'patient_gender_code',
            'patient_birth_year',
            'column_27',
            'billing_prov_id',
            'billing_prov_npi',
            'billing_prov_org_name',
            'billing_prov_ind_name',
            'billing_prov_street_address_1',
            'billing_prov_street_address_2',
            'billing_prov_city',
            'billing_prov_state',
            'billing_prov_zip',
            'column_37',
            'referring_prov_npi',
            'column_39',
            'referring_prov_name',
            'column_41',
            'rendering_attending_prov_npi',
            'rendering_prov_org_name',
            'rendering_attending_prov_ind_name',
            'column_45',
            'facility_name',
            'column_47',
            'facility_street_address_1',
            'facility_street_address_2',
            'facility_city',
            'facility_state',
            'facility_zip',
            'statement_from_date',
            'statement_to_date',
            'total_claim_charge_amt',
            'column_56',
            'drg_code',
            'column_58',
            'bill_type',
            'patient_signature_source_cd',
            'bebefits_assignment_cert_ind',
            'column_62',
            'principal_icd_procedure_code',
            'admitting_diagnosis_code',
            'principal_diagnosis_code',
            'other_diagnosis_code_1',
            'other_diagnosis_code_2',
            'other_diagnosis_code_3',
            'other_diagnosis_code_4',
            'other_diagnosis_code_5',
            'other_diagnosis_code_6',
            'other_diagnosis_code_7',
            'other_icd_proc_code_1',
            'other_icd_proc_code_2',
            'other_icd_proc_code_3',
            'other_icd_proc_code_4',
            'other_icd_proc_code_5',
            'column_78',
            'claim_filing_indicator_cd',
            'column_80',
            'accident_related_ind',
            'column_82',
            'column_83',
            'column_84',
            'column_85',
            'column_86',
            'column_87',
            'column_88',
            'column_89',
            'column_90',
            'column_91',
            'admission_date',
            'column_93',
            'admission_type_code',
            'admission_source_code',
            'column_96',
            'patient_status_code',
            'column_98',
            'other_icd_proc_code_6',
            'other_icd_proc_code_7',
            'other_icd_proc_code_8',
            'other_icd_proc_code_9',
            'billing_pr_taxonomy',
            'billing_pr_state_license',
            'column_105',
            'billing_pr_ssn',
            'rendering_attending_prov_taxonomy',
            'rendering_attending_prov_state_license',
            'column_109',
            'facility_npi',
            'facility_state_license'
        ]
    )
}
