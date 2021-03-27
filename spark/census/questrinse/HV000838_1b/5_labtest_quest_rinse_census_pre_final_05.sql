SELECT
    norm_pre04.HV_claim_id             ,
    norm_pre04.hvid                    ,
    norm_pre04.created                 ,
    norm_pre04.model_version           ,
    norm_pre04.data_set                ,
    norm_pre04.data_feed               ,
    norm_pre04.data_vendor             ,
    norm_pre04.HV_patient_gender       ,
    norm_pre04.HV_patient_age          ,
    norm_pre04.HV_patient_year_of_birth,
    norm_pre04.HV_patient_zip3         ,
    norm_pre04.HV_patient_state        ,
    norm_pre04.date_service            ,
    norm_pre04.date_specimen           ,
    norm_pre04.HV_date_report          ,
    norm_pre04.loinc_code              ,
    norm_pre04.hv_loinc_code           ,
    norm_pre04.lab_id                  ,
    norm_pre04.test_id                 ,
    norm_pre04.HV_test_number          ,
    norm_pre04.test_battery_local_id   ,
    norm_pre04.test_battery_std_id     ,
    norm_pre04.test_battery_name       ,
    norm_pre04.test_ordered_local_id   ,    
    norm_pre04.test_ordered_std_id     ,
    norm_pre04.test_ordered_name       ,
    norm_pre04.result_id               ,
    norm_pre04.result                  ,
    norm_pre04.result_name             ,
    norm_pre04.result_unit_of_measure  ,
    norm_pre04.result_desc             ,
    norm_pre04.HV_ref_range_alpha      ,
    norm_pre04.HV_fasting_status       ,
    norm_pre04.HV_procedure_code       ,
    norm_pre04.HV_procedure_code_qual  ,
    norm_pre04.HV_ordering_npi         ,
    norm_pre04.payer_id                ,
    norm_pre04.payer_name              ,
    norm_pre04.lab_other_id            ,
    norm_pre04.HV_lab_other_qual       ,
    norm_pre04.ordering_market_type    ,
    norm_pre04.ordering_specialty      ,
    norm_pre04.ordering_state_license  ,
    norm_pre04.ordering_upin           ,
    norm_pre04.ordering_address_1      ,
    norm_pre04.ordering_address_2      ,
    norm_pre04.ordering_city           ,
    norm_pre04.HV_ordering_state       ,
    norm_pre04.ordering_zip            ,
    norm_pre04.part_provider           ,
    norm_pre04.HV_part_best_date       ,
    norm_pre04.unique_accession_id     ,
    norm_pre04.diag_accn_id            ,
    norm_pre04.diag_date_of_service    ,
    norm_pre04.diag_lab_code           ,
    norm_pre04.acct_id                 ,
    norm_pre04.acct_number             ,
    norm_pre04.diag_dos_yyyymm         ,
    norm_pre04.rslt_accn_id            ,
    norm_pre04.lab_code                ,
    norm_pre04.phy_id                  ,
    norm_pre04.rslt_accession_number   ,
    norm_pre04.accn_dom_id             ,
    norm_pre04.cmdm_spclty_cd          ,
    norm_pre04.acct_name               ,
    norm_pre04.cmdm_licstate           ,
    norm_pre04.billing_client_number   ,
    norm_pre04.fasting_hours           ,
    norm_pre04.qbs_ordering_client_num ,
    norm_pre04.date_order_entry        ,
    norm_pre04.informed_consent_flag   ,
    norm_pre04.legal_entity            ,
    norm_pre04.specimen_type           ,
    norm_pre04.ordering_site_code      ,
    norm_pre04.canceled_accn_ind       ,
    norm_pre04.copy_to_clns            ,
    norm_pre04.non_physician_name      ,
    norm_pre04.non_physician_id        ,
    norm_pre04.long_description        ,
    norm_pre04.phy_name                ,    
    norm_pre04.suffix                  ,
    norm_pre04.degree                  ,
    norm_pre04.idw_analyte_code        ,
    norm_pre04.qls_service_code        ,
    norm_pre04.reportable_results_ind  ,
    norm_pre04.ord_seq                 ,
    norm_pre04.res_seq                 ,
    norm_pre04.abnormal_ind            ,
    norm_pre04.amended_report_ind      ,
    norm_pre04.alpha_normal_flag       ,
    norm_pre04.instrument_id           ,
    norm_pre04.result_release_date     ,
    norm_pre04.enterprise_ntc_code     ,
    norm_pre04.derived_profile_code    ,
    norm_pre04.qtim_as_ordered_code    ,
    norm_pre04.qtim_profile_ind        ,
    norm_pre04.rslt_dos_yyyymm         ,
    norm_pre04.sister_lab              ,
    norm_pre04.bill_type_cd            ,
    norm_pre04.idw_report_change_status,
    norm_pre04.ins_seq                 ,
    norm_pre04.active_ind              ,
    norm_pre04.bill_code               ,
    norm_pre04.insurance_billing_type  ,
    norm_pre04.qbs_payor_cd            ,
    norm_pre04.dos_id                  ,
    norm_pre04.lab_name                ,
    norm_pre04.lab_lis_type            ,
    norm_pre04.confidential_order_ind  ,
    norm_pre04.daard_client_flag       ,
    norm_pre04.ptnt_accession_number   ,
    norm_pre04.ptnt_date_of_service    ,
    norm_pre04.ptnt_lab_code           ,
    norm_pre04.accn_enterprise_id      ,
    norm_pre04.age_code                ,
    norm_pre04.species                 ,
    norm_pre04.pat_country             ,
    norm_pre04.external_patient_id     ,
    norm_pre04.pat_master_id           ,
    norm_pre04.lab_reference_number    ,
    norm_pre04.room_number             ,
    norm_pre04.bed_number              ,
    norm_pre04.hospital_location       ,
    norm_pre04.ward                    ,
    norm_pre04.admission_date          ,
    norm_pre04.health_id               ,
    norm_pre04.pm_eid                  ,
    norm_pre04.idw_pm_email_address    ,
    norm_pre04.date_reported           ,
    norm_pre04.ref_range_low           ,
    norm_pre04.ref_range_high          ,
    norm_pre04.ref_range_alpha         ,    
    norm_pre04.requisition_number      ,
    norm_pre04.fasting_ind             ,
    norm_pre04.cpt_code                ,
    norm_pre04.npi                     ,
    norm_pre04.phy_last_name           ,
    norm_pre04.phy_first_name          ,
    norm_pre04.phy_middle_name         ,
    norm_pre04.acct_state              ,
    CASE
        WHEN LENGTH(TRIM(COALESCE(norm_pre04.HV_result_value_operator,''))) = 0 THEN NULL
    ELSE norm_pre04.HV_result_value_operator
    END                                 AS HV_result_value_operator,
    CASE
        WHEN LENGTH(TRIM(COALESCE(norm_pre04.HV_result_value_numeric,''))) = 0 THEN NULL
        
    ELSE norm_pre04.HV_result_value_numeric
    END                                 AS HV_result_value_numeric,

    CASE
        WHEN LENGTH(TRIM(COALESCE(norm_pre04.HV_result_value_alpha,''))) = 0 THEN NULL
    ELSE norm_pre04.HV_result_value_alpha
    END                                 AS HV_result_value_alpha,
-------------------------------------------------------------------------------------------------
---------- New fields added per request to populate outlier result (2020-12-14) included in 3.5 module
-------------------------------------------------------------------------------------------------
    CASE
        WHEN ( norm_pre04.HV_result_value_operator IS NULL OR LENGTH(TRIM(COALESCE(norm_pre04.HV_result_value_operator,''))) = 0)
         AND ( norm_pre04.HV_result_value_numeric IS NUll  OR LENGTH(TRIM(COALESCE(norm_pre04.HV_result_value_numeric,''))) = 0)
         AND ( norm_pre04.HV_result_value_alpha IS NUll  OR LENGTH(TRIM(COALESCE(norm_pre04.HV_result_value_alpha,''))) = 0)
                    THEN norm_pre04.result
    ELSE NULL
    END   AS HV_result_value, 
    
    norm_pre04.date_final_report       ,
    norm_pre04.profile_name_qtim       ,
    norm_pre04.order_name_qtim         ,
    norm_pre04.specimen_type_desc_qtim ,
    norm_pre04.methodology_qtim        ,
    norm_pre04.result_name_qtim        ,
    norm_pre04.unit_of_measure_qtim    ,
    norm_pre04.loinc_number_qtim       ,
    
    norm_pre04.s_diag_code_codeset_ind ,
    norm_pre04.HV_s_diag_code_codeset_ind
    
FROM labtest_quest_rinse_census_pre_final_04 norm_pre04
