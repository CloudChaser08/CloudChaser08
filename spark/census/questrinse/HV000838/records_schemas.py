"""records schema questrince hv000838"""
from spark.helpers.source_table import SourceTable

TABLE_CONF = {
    'diagnosis': SourceTable(
        'parquet',
        columns=[
            'accn_id',
            'date_of_service',
            'lab_code',
            'unique_accession_id',
            'acct_id',
            'acct_number',
            's_icd_codeset_ind',
            's_diag_code',
            'diag_code',
            'icd_codeset_ind',
            'dos_yyyymm'
        ]
    ),
    'order_result': SourceTable(
        'parquet',
        columns=[
            'accn_id',
            'date_of_service',
            'lab_code',
            'unique_accession_id',
            'phy_id',
            'accession_number',
            'accn_dom_id',
            'requisition_number',
            'cmdm_spclty_cd',
            'acct_name',
            'market_type',
            'cmdm_licstate',
            'cmdm_licnum',
            'billing_client_number',
            'acct_address_1',
            'acct_address_2',
            'acct_city',
            'acct_state',
            'acct_zip',
            'fasting_ind',
            'fasting_hours',
            'qbs_ordering_client_num',
            'date_of_collection',
            'date_final_report',
            'date_order_entry',
            'informed_consent_flag',
            'long_description',
            'legal_entity',
            'specimen_type',
            'ordering_site_code',
            'canceled_accn_ind',
            'pm_ethnic_grp',
            'relationship_to_insured',
            'copy_to_clns',
            'non_physician_name',
            'non_physician_id',
            'phy_name',
            'phy_first_name',
            'phy_middle_name',
            'phy_last_name',
            'upin',
            'client_specialty',
            'suffix',
            'degree',
            'npi',
            'obs_id',
            'local_profile_code',
            'standard_profile_code',
            'profile_name',
            'standard_order_code',
            'order_name',
            'local_result_code',
            'idw_analyte_code',
            'loinc_code',
            'result_name',
            'ref_range_alpha',
            'ref_range_low',
            'ref_range_high',
            'units',
            'qls_service_code',
            'reportable_results_ind',
            'ord_seq',
            'res_seq',
            'perf_lab_code',
            'abnormal_ind',
            'cpt_code',
            'result_type',
            'result_value',
            'amended_report_ind',
            'alpha_normal_flag',
            'date_reported',
            'instrument_id',
            'result_release_date',
            'enterprise_ntc_code',
            'idw_local_order_code',
            'derived_profile_code',
            'qtim_as_ordered_code',
            'qtim_profile_ind',
            'dos_yyyymm',
            'sister_lab',
            'bill_type_cd',
            'idw_report_change_status',
            'lab_id',
            'ins_id',
            'ins_seq',
            'company',
            'active_ind',
            'bill_code',
            'insurance_billing_type',
            'policy_number',
            'medicaid_number',
            'medicare_number',
            'group_number',
            'qbs_payor_cd',
            'dos_id',
            'lab_name',
            'lab_lis_type',
            'confidential_order_ind',
            'daard_client_flag'
        ]
    ),
    'transactions': SourceTable(
        'csv',
        separator='|',
        columns=[
            'accession_number',
            'date_of_service',
            'lab_code',
            'unique_accession_id',
            'accn_enterprise_id',
            'pat_first_name',
            'pat_middle_name',
            'pat_last_name',
            'social_security_number',
            'date_of_birth',
            'numeric_age',
            'age_code',
            'gender',
            'species',
            'pat_address_1',
            'pat_address_2',
            'pat_city',
            'pat_state',
            'pat_zip',
            'pat_country',
            'pat_phone',
            'external_patient_id',
            'pat_master_id',
            'lab_reference_number',
            'room_number',
            'bed_number',
            'hospital_location',
            'ward',
            'medical_records_number',
            'chart_id',
            'admission_date',
            'health_id',
            'pm_eid',
            'idw_pm_email_address',
            'hvjoinkey'
        ]
    )
}
