"""
transactional schema
"""
from spark.helpers.source_table import SourceTable

TABLE_CONF = {
    'claim': SourceTable(
        'csv',
        separator='|',
        columns=[
            'claim_number',
            'record_type',
            'payer_id',
            'coding_type',
            'received_date',
            'claim_type_code',
            'contract_allow_ind',
            'payer_name',
            'sub_client_id',
            'group_name',
            'member_id',
            'member_fname',
            'member_lname',
            'member_gender',
            'member_yob',
            'member_adr_line1',
            'member_adr_line2',
            'member_adr_city',
            'member_adr_state',
            'member_adr_zip',
            'patient_id',
            'patient_relation',
            'patient_fname',
            'patient_lname',
            'patient_gender',
            'patient_yob',
            'patient_age',
            'billing_pr_id',
            'billing_pr_npi',
            'billing_name1',
            'billing_name2',
            'billing_adr_line1',
            'billing_adr_line2',
            'billing_adr_city',
            'billing_adr_state',
            'billing_adr_zip',
            'referring_pr_id',
            'referring_pr_npi',
            'referring_name1',
            'referring_name2',
            'attending_pr_id',
            'attending_pr_npi',
            'attending_name1',
            'attending_name2',
            'facility_id',
            'facility_name1',
            'facility_name2',
            'facility_adr_line1',
            'facility_adr_line2',
            'facility_adr_city',
            'facility_adr_state',
            'facility_adr_zip',
            'statement_from',
            'statement_to',
            'total_charge',
            'total_allowed',
            'drg_code',
            'patient_control',
            'type_bill',
            'release_sign',
            'assignment_sign',
            'in_out_network',
            'principal_procedure',
            'admit_diagnosis',
            'primary_diagnosis',
            'diagnosis_code_2',
            'diagnosis_code_3',
            'diagnosis_code_4',
            'diagnosis_code_5',
            'diagnosis_code_6',
            'diagnosis_code_7',
            'diagnosis_code_8',
            'other_proc_code_2',
            'other_proc_code_3',
            'other_proc_code_4',
            'other_proc_code_5',
            'other_proc_code_6',
            'prov_specialty',
            'type_coverage',
            'explanation_code',
            'accident_related',
            'esrd_patient',
            'hosp_admis_or_er',
            'amb_nurse_to_hosp',
            'not_covrd_specialt',
            'electronic_claim',
            'dialysis_related',
            'new_patient',
            'initial_procedure',
            'amb_nurse_to_diag',
            'amb_hosp_to_hosp',
            'admission_date',
            'admission_hour',
            'admit_type_code',
            'admit_src_code',
            'discharge_hour',
            'patient_status_cd',
            'tooth_number',
            'other_proc_code_7',
            'other_proc_code_8',
            'other_proc_code_9',
            'other_proc_code_10',
            'billing_taxonomy',
            'billing_state_lic',
            'billing_upin',
            'billing_ssn',
            'rendering_taxonomy',
            'rendering_state_lic',
            'rendering_upin',
            'facility_npi',
            'facility_state_lic',
            'hvjoinkey'
        ]
    ),
    'lines': SourceTable(
        'csv',
        separator='|',
        columns=[
            'claim_number',
            'record_type',
            'line_number',
            'service_from',
            'service_to',
            'place_service',
            'procedure',
            'procedure_qual',
            'procedure_modifier_1',
            'procedure_modifier_2',
            'procedure_modifier_3',
            'procedure_modifier_4',
            'line_charge',
            'line_allowed',
            'units',
            'revenue_code',
            'diagnosis_pointer_1',
            'diagnosis_pointer_2',
            'diagnosis_pointer_3',
            'diagnosis_pointer_4',
            'diagnosis_pointer_5',
            'diagnosis_pointer_6',
            'diagnosis_pointer_7',
            'diagnosis_pointer_8',
            'ndc',
            'ambulance_to_hosp',
            'emergency',
            'tooth_surface',
            'oral_cavity',
            'type_service',
            'copay',
            'paid_amount',
            'date_paid',
            'bene_not_entitled',
            'patient_reach_max',
            'svc_during_postop',
            'adjudicated_procedure',
            'adjudicated_procedure_qual',
            'adjudicated_proc_modifier_1',
            'adjudicated_proc_modifier_2',
            'adjudicated_proc_modifier_3',
            'adjudicated_proc_modifier_4'
        ]
    )
}
