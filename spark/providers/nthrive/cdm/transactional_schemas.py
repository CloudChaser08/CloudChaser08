# pylint: disable=duplicate-code
from spark.helpers.source_table import SourceTable

TABLE_CONF = {

    'nthrive_patient': SourceTable(
        'csv',
        separator='|',
        columns=[
            'record_id',
            'unique_patient_id',
            'provider_id',
            'npi',
            'patienttype',
            'patientfirstname',
            'patientlastname',
            'patientaddress',
            'patientcity',
            'patientstate',
            'patientzipcode',
            'patientdob',
            'facilityzip',
            'hvjoinkey'
        ]
    ),
    'nthrive_master_drg': SourceTable(
        'csv',
        separator='|',
        columns=[
            'drg_id',
            'drg_code',
            'drg_desc',
            'drg_mdc_code',
            'drg_mdc_desc',
            'effective_date',
            'expiration_date',
            'extra_col'
        ]
    ),
    'nthrive_master_ms_drg': SourceTable(
        'csv',
        separator='|',
        columns=[
            'ms_drg_id',
            'ms_drg_code',
            'ms_drg_desc',
            'ms_drg_mdc_code',
            'ms_drg_mdc_desc',
            'effective_date',
            'expiration_date',
            'extra_col'
        ]
    ),
    #    'cpt': SourceTable(
    #        'csv',
    #        separator='|',
    #        columns=[
    #            'cpt_code',
    #           'cpt_desc'
    #        ]
    #    ),
    #    'mod': SourceTable(
    #        'csv',
    #        separator='|',
    #        columns=[
    #            'cpt_modifier_code',
    #            'cpt_modifier_desc'
    #        ]
    #    ),
    'nthrive_chargemaster': SourceTable(
        'csv',
        separator='|',
        columns=[
            'charge_id',
            'cdm_version',
            'charge_code',
            'charge_desc',
            'cdm_dept_code',
            'cdm_std_id',
            'cdm_to_std_conv_factor',
            'chg_in_time',
            'effective_date',
            'end_date',
            'extra_col'
        ]
    ),
    'nthrive_standard_chargemaster': SourceTable(
        'csv',
        separator='|',
        columns=[
            'cdm_std_id',
            'cdm_std_code',
            'cdm_std_desc',
            'cdm_std_short_desc',
            'cpt_code',
            'hcpcs_code',
            'hcpcs_modifier',
            'cpm_code',
            'cpm_desc',
            'std_to_cpm_conv_factor',
            'std_dept_code',
            'std_dept_desc',
            'manuf_name',
            'manuf_cat_num',
            'manuf_descr',
            'extra_col'
        ]
    ),
    'nthrive_providers': SourceTable(
        'csv',
        separator='|',
        columns=[
            'provider_id',
            'provider_npi',
            'bed_grp',
            'bed_grp_desc',
            'teaching',
            'urban_rural',
            'state',
            'region',
            'region_desc',
            'geo_division',
            'geo_division_desc',
            'rcc',
            'extra_col'
        ]
    ),
    'nthrive_provider_county': SourceTable(
        'csv',
        separator='|',
        columns=[
            'provider_id',
            'county',
            'county_res_ratio'
        ]
    ),
    'nthrive_provider_cbsa': SourceTable(
        'csv',
        separator='|',
        columns=[
            'provider_id',
            'cbsa',
            'cbsa_res_ratio'
        ]
    ),
    'nthrive_episodes': SourceTable(
        'csv',
        separator='|',
        columns=[
            'record_id',
            'provider_id',
            'unique_patient_id',
            'drg_id',
            'ms_drg_id',
            'bill_type',
            'patient_type',
            'gender',
            'age',
            'age_days',
            'pat_mar_stat',
            'admit_dt',
            'discharge_dt',
            'admission_source',
            'admit_source_desc',
            'admission_type',
            'admit_type_desc',
            'discharge_status',
            'discharge_stat_desc',
            'zip_code',
            'los',
            'los_adm_pri_proc',
            'los_pri_proc_disch',
            'los_in_hrs',
            'mortality',
            'total_charges',
            'payment_actual',
            'payment_expected',
            'discount_amount',
            'pat_liability_amount',
            'pat_payment',
            'account_balance',
            'eob_writeoff',
            'contractual_adjustment',
            'other_adjustment',
            'other_payment',
            'amount_denied',
            'amount_copay',
            'amount_deductible',
            'sec_insurance_payment',
            'days_to_readmit',
            'readmit_enctr_key',
            'total_cost',
            'visit_order',
            'days_to_revisit',
            'revisit_enctr_key',
            'time_or',
            'icu_ccu_days',
            'attendingphysiciannpi',
            'operatingphysiciannpi',
            'referringphysiciannpi',
            'admittingphysiciannpi',
            'servingphysiciannpi'
        ]
    ),
    'nthrive_patient_payer': SourceTable(
        'csv',
        separator='|',
        columns=[
            'record_id',
            'payer_order',
            'emp_st_code',
            'pat_rel_code',
            'payer_desc',
            'ins_plan_desc',
            'payer_cat',
            'extra_col'
        ]
    ),
    'nthrive_patient_charges': SourceTable(
        'csv',
        separator='|',
        columns=[
            'record_id',
            'charge_id',
            'service_day',
            'cpt_code',
            'cpt_modifier',
            'units',
            'charge',
            'extra_col'
        ]
    ),
    'nthrive_patient_diagnosis': SourceTable(
        'csv',
        separator='|',
        columns=[
            'record_id',
            'icd_diagnosis_code',
            'icd_diagnosis_desc',
            'code_type',
            'diagnosis_order',
            'present_on_admit',
            'icd_diag_cc',
            'icd_diag_mcc',
            'chronic_condition_ind',
            'chronic_condition_ind_desc',
            'body_system',
            'body_system_desc',
            'ahrq_version'
        ]
    ),
    'patient_procedure': SourceTable(
        'csv',
        separator='|',
        columns=[
            'record_id',
            'icd_procedure_code',
            'icd_procedure_desc',
            'proc_class',
            'proc_class_desc',
            'ahrq_version',
            'procedure_day',
            'procedure_order',
            'extra_col'
        ]
    ),
    'nthrive_patient_cpt': SourceTable(
        'csv',
        separator='|',
        columns=[
            'record_id',
            'cpt_code',
            'cpt_modifier1',
            'cpt_modifier2',
            'cpt_modifier3',
            'procedure_day',
            'procedure_order',
            'extra_col'
        ]
    ),
    'nthrive_weights': SourceTable(
        'csv',
        separator='|',
        columns=[
            'provider_id',
            'patient_type',
            'discharge_month',
            'weight'
        ]
    )
}
