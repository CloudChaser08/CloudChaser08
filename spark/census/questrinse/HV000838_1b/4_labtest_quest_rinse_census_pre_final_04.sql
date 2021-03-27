SELECT
    norm_pre03.HV_claim_id             ,
    norm_pre03.hvid                    ,
    norm_pre03.created                 ,
    norm_pre03.model_version           ,
    norm_pre03.data_set                ,
    norm_pre03.data_feed               ,
    norm_pre03.data_vendor             ,
    norm_pre03.HV_patient_gender       ,
    norm_pre03.HV_patient_age          ,
    norm_pre03.HV_patient_year_of_birth,
    norm_pre03.HV_patient_zip3         ,
    norm_pre03.HV_patient_state        ,
    norm_pre03.date_service            ,
    norm_pre03.date_specimen           ,
    norm_pre03.HV_date_report          ,
    norm_pre03.loinc_code              ,
    norm_pre03.hv_loinc_code           ,
    norm_pre03.lab_id                  ,
    norm_pre03.test_id                 ,
    norm_pre03.HV_test_number          ,
    norm_pre03.test_battery_local_id   ,
    norm_pre03.test_battery_std_id     ,
    norm_pre03.test_battery_name       ,
    norm_pre03.test_ordered_local_id   ,    
    norm_pre03.test_ordered_std_id     ,
    norm_pre03.test_ordered_name       ,
    norm_pre03.result_id               ,
    norm_pre03.result                  ,
    norm_pre03.result_name             ,
    norm_pre03.result_unit_of_measure  ,
    norm_pre03.result_desc             ,
    norm_pre03.HV_ref_range_alpha      ,
    norm_pre03.HV_fasting_status       ,
    norm_pre03.HV_procedure_code       ,
    norm_pre03.HV_procedure_code_qual  ,
    norm_pre03.HV_ordering_npi         ,
    norm_pre03.payer_id                ,
    norm_pre03.payer_name              ,
    norm_pre03.lab_other_id            ,
    norm_pre03.HV_lab_other_qual       ,
    norm_pre03.ordering_market_type    ,
    norm_pre03.ordering_specialty      ,
    norm_pre03.ordering_state_license  ,
    norm_pre03.ordering_upin           ,
    norm_pre03.ordering_address_1      ,
    norm_pre03.ordering_address_2      ,
    norm_pre03.ordering_city           ,
    norm_pre03.HV_ordering_state       ,
    norm_pre03.ordering_zip            ,
    norm_pre03.part_provider           ,
    norm_pre03.HV_part_best_date       ,
    norm_pre03.unique_accession_id     ,
    norm_pre03.diag_accn_id            ,
    norm_pre03.diag_date_of_service    ,
    norm_pre03.diag_lab_code           ,
    norm_pre03.acct_id                 ,
    norm_pre03.acct_number             ,
    norm_pre03.diag_dos_yyyymm         ,
    norm_pre03.rslt_accn_id            ,
    norm_pre03.lab_code                ,
    norm_pre03.phy_id                  ,
    norm_pre03.rslt_accession_number   ,
    norm_pre03.accn_dom_id             ,
    norm_pre03.cmdm_spclty_cd          ,
    norm_pre03.acct_name               ,
    norm_pre03.cmdm_licstate           ,
    norm_pre03.billing_client_number   ,
    norm_pre03.fasting_hours           ,
    norm_pre03.qbs_ordering_client_num ,
    norm_pre03.date_order_entry        ,
    norm_pre03.informed_consent_flag   ,
    norm_pre03.legal_entity            ,
    norm_pre03.specimen_type           ,
    norm_pre03.ordering_site_code      ,
    norm_pre03.canceled_accn_ind       ,
    norm_pre03.copy_to_clns            ,
    norm_pre03.non_physician_name      ,
    norm_pre03.non_physician_id        ,
    norm_pre03.long_description        ,
    norm_pre03.phy_name                ,    
    norm_pre03.suffix                  ,
    norm_pre03.degree                  ,
    norm_pre03.idw_analyte_code        ,
    norm_pre03.qls_service_code        ,
    norm_pre03.reportable_results_ind  ,
    norm_pre03.ord_seq                 ,
    norm_pre03.res_seq                 ,
    norm_pre03.abnormal_ind            ,
    norm_pre03.amended_report_ind      ,
    norm_pre03.alpha_normal_flag       ,
    norm_pre03.instrument_id           ,
    norm_pre03.result_release_date     ,
    norm_pre03.enterprise_ntc_code     ,
    norm_pre03.derived_profile_code    ,
    norm_pre03.qtim_as_ordered_code    ,
    norm_pre03.qtim_profile_ind        ,
    norm_pre03.rslt_dos_yyyymm         ,
    norm_pre03.sister_lab              ,
    norm_pre03.bill_type_cd            ,
    norm_pre03.idw_report_change_status,
    norm_pre03.ins_seq                 ,
    norm_pre03.active_ind              ,
    norm_pre03.bill_code               ,
    norm_pre03.insurance_billing_type  ,
    norm_pre03.qbs_payor_cd            ,
    norm_pre03.dos_id                  ,
    norm_pre03.lab_name                ,
    norm_pre03.lab_lis_type            ,
    norm_pre03.confidential_order_ind  ,
    norm_pre03.daard_client_flag       ,
    norm_pre03.ptnt_accession_number   ,
    norm_pre03.ptnt_date_of_service    ,
    norm_pre03.ptnt_lab_code           ,
    norm_pre03.accn_enterprise_id      ,
    norm_pre03.age_code                ,
    norm_pre03.species                 ,
    norm_pre03.pat_country             ,
    norm_pre03.external_patient_id     ,
    norm_pre03.pat_master_id           ,
    norm_pre03.lab_reference_number    ,
    norm_pre03.room_number             ,
    norm_pre03.bed_number              ,
    norm_pre03.hospital_location       ,
    norm_pre03.ward                    ,
    norm_pre03.admission_date          ,
    norm_pre03.health_id               ,
    norm_pre03.pm_eid                  ,
    norm_pre03.idw_pm_email_address    ,
    norm_pre03.date_reported           ,
    norm_pre03.ref_range_low           ,
    norm_pre03.ref_range_high          ,
    norm_pre03.ref_range_alpha         ,    
    norm_pre03.requisition_number      ,
    norm_pre03.fasting_ind             ,
    norm_pre03.cpt_code                ,
    norm_pre03.npi                     ,
    norm_pre03.phy_last_name           ,
    norm_pre03.phy_first_name          ,
    norm_pre03.phy_middle_name         ,
    norm_pre03.acct_state              ,
    norm_pre03.HV_result_value_operator,
    norm_pre03.HV_result_value_numeric ,
    ----------------- Special exceptions (When there is number in the result but numeric field is NULL and opearator is NULL then make the alpha NULL)
    CASE
        WHEN CAST(REPLACE(SPLIT(norm_pre03.result,' ')[0],',','') AS FLOAT) IS NOT NULL 
         AND norm_pre03.HV_result_value_operator IS NULL
         AND norm_pre03.HV_result_value_numeric IS NULL         THEN NULL
        WHEN SUBSTR(norm_pre03.HV_result_value_alpha,1,1) = '/' THEN REPLACE(norm_pre03.HV_result_value_alpha, '/', '')        
  
    ELSE
        norm_pre03.HV_result_value_alpha
    END AS HV_result_value_alpha       ,
    -----------------
    norm_pre03.date_final_report       ,
    norm_pre03.profile_name_qtim       ,
    norm_pre03.order_name_qtim         ,
    norm_pre03.specimen_type_desc_qtim ,
    norm_pre03.methodology_qtim        ,
    norm_pre03.result_name_qtim        ,
    norm_pre03.unit_of_measure_qtim    ,
    norm_pre03.loinc_number_qtim       ,
    norm_pre03.s_diag_code_codeset_ind ,
    norm_pre03.HV_s_diag_code_codeset_ind
    
FROM labtest_quest_rinse_census_pre_final_03 norm_pre03
