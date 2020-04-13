SELECT /*+ BROADCAST(diag), BROADCAST(ptnt) */
    MONOTONICALLY_INCREASING_ID()                                                           AS record_id,
    rslt.unique_accession_id                                                                AS claim_id,
    ptnt.hvid,
    CURRENT_DATE()                                                                          AS created,
	'09'                                                                                    AS model_version,
    SPLIT(rslt.input_file_name, '/')[SIZE(SPLIT(rslt.input_file_name, '/')) - 1]            AS data_set,
	'187'                                                                                   AS data_feed,
	'7'                                                                                     AS data_vendor,
	/* patient_gender */
	ptnt.patient_gender,
	/* patient_age - (Notes for me - Per our transformation - if the ptnt.age is NULL the patient_age becomes NULL)  
	   CASE is added to increase the performance UDF cap_year_of_birth(age, date_service, year_of_birth) */
	CASE
	    WHEN ptnt.age IS NULL THEN NULL
	    ELSE 
	    VALIDATE_AGE(
	        ptnt.age,
	        CAST(rslt.date_of_service AS DATE),
	        ptnt.yearofbirth)
	END                                                                                     AS patient_age,
	CAP_YEAR_OF_BIRTH(
        ptnt.age,
        CAST(rslt.date_of_service AS DATE),
        ptnt.yearofbirth)                                                                    AS patient_year_of_birth,
	ptnt.patient_zip3,
    ptnt.patient_state,
    CASE
        WHEN CAST(rslt.date_of_service AS DATE) < CAST('{EARLIEST_SERVICE_DATE}' AS DATE)
          OR CAST(rslt.date_of_service AS DATE) > CAST('{VDR_FILE_DT}'                 AS DATE) THEN NULL
        ELSE CAST(rslt.date_of_service AS DATE)
    END                                                                                      AS date_service,
    CASE
        WHEN CAST(rslt.date_of_collection AS DATE) < CAST('{EARLIEST_SERVICE_DATE}' AS DATE)
          OR CAST(rslt.date_of_collection AS DATE) > CAST('{VDR_FILE_DT}'                 AS DATE) THEN NULL
        ELSE CAST(rslt.date_of_collection AS DATE)
    END                                                                                      AS date_specimen,
    CASE
        WHEN CAST(COALESCE(rslt.date_final_report,rslt.date_reported) AS DATE) < CAST('{EARLIEST_SERVICE_DATE}' AS DATE)
          OR CAST(COALESCE(rslt.date_final_report,rslt.date_reported) AS DATE) > CAST('{VDR_FILE_DT}'                 AS DATE) THEN NULL
        ELSE CAST(COALESCE(rslt.date_final_report,rslt.date_reported) AS DATE)
    END                                                                                      AS date_report,
    rslt.date_final_report                                                                   AS time_report,
    CLEAN_UP_NUMERIC_CODE(rslt.loinc_code)	                                                 AS loinc_code,
    rslt.lab_id                                                                              AS lab_id,
    rslt.obs_id                                                                              AS test_id,
    CASE
        WHEN UPPER(rslt.requisition_number) = 'NONE' THEN NULL
        ELSE rslt.requisition_number
    END                                                                                      AS test_number,
    rslt.standard_profile_code                                                               AS test_battery_std_id,
    rslt.profile_name                                                                        AS test_battery_name,
    rslt.standard_order_code                                                                 AS test_ordered_std_id,
    rslt.order_name                                                                          AS test_ordered_name,
    rslt.local_result_code                                                                   AS result_id,
    rslt.result_value                                                                        AS result,
    rslt.result_name                                                                         AS result_name,
    rslt.units                                                                               AS result_unit_of_measure,
    -----------------
    CASE
        WHEN ref_range_alpha IS NOT NULL
            THEN ref_range_alpha
        WHEN ref_range_alpha IS NULL     AND rslt.ref_range_low IS NOT NULL AND rslt.ref_range_high IS NOT NULL
            THEN CONCAT(rslt.ref_range_low,' - ', rslt.ref_range_high)
        WHEN ref_range_alpha IS NULL     AND rslt.ref_range_low IS     NULL AND rslt.ref_range_high IS NOT NULL
            THEN  rslt.ref_range_high
        WHEN ref_range_alpha IS NULL     AND rslt.ref_range_low IS NOT NULL AND rslt.ref_range_high IS     NULL
            THEN  rslt.ref_range_low
    END                                                                                      AS ref_range_alpha,
    -----------------
    CASE
        WHEN rslt.fasting_ind IN ('Y', 'N', 'U') THEN rslt.fasting_ind
        ELSE NULL
    END	                                                                                     AS fasting_status,
    CLEAN_UP_DIAGNOSIS_CODE
        (
        diag.s_diag_code,
        '02',
        CAST(rslt.date_of_service AS DATE)
        )                                                                                    AS diagnosis_code,
    '02'                                                                                     AS diagnosis_code_qual,
    CLEAN_UP_PROCEDURE_CODE(rslt.cpt_code)                                                   AS procedure_code,
    'CPT'                                                                                    AS procedure_code_qual,
    CLEAN_UP_NPI_CODE(rslt.npi)                                                              AS ordering_npi,
    rslt.ins_id                                                                              AS payer_id,
    rslt.company                                                                             AS payer_name,
    rslt.perf_lab_code                                                                       AS lab_other_id,
	'PERFORMINGLAB_PERFORMED'                                                                AS lab_other_qual,
	------------- NAME
	CONCAT
    (
        COALESCE(CONCAT(      rslt.phy_last_name), ''),
        COALESCE(CONCAT(', ', rslt.phy_first_name), ''),
        COALESCE(CONCAT(', ', rslt.phy_middle_name), '')
    )                                                                                        AS ordering_name,
    rslt.market_type                                                                         AS ordering_market_type,
    rslt.client_specialty                                                                    AS ordering_specialty,
    rslt.cmdm_licnum                                                                         AS ordering_state_license,
    rslt.upin                                                                                AS ordering_upin,
    rslt.acct_address_1                                                                      AS ordering_address_1,
    rslt.acct_address_2                                                                      AS ordering_address_2,
    rslt.acct_city                                                                           AS ordering_city,
    VALIDATE_STATE_CODE(UPPER(COALESCE(rslt.acct_state,ptnt.state)))                         AS ordering_state,
    rslt.acct_zip                                                                            AS ordering_zip,
    'quest_rinse'                                                                            AS part_provider,
    /* part_best_date */
	CASE
	    WHEN 0 = LENGTH
	        (COALESCE
	            (
	           CAP_DATE
                (
                  CAST(rslt.date_of_service                               AS DATE),
                  CAST(COALESCE('{AVAILABLE_START_DATE}', '{EARLIEST_SERVICE_DATE}') AS DATE), 
                  CAST('{VDR_FILE_DT}'                                                            AS DATE)
                ), 
                ''
                )
            )
	        THEN '0_PREDATES_HVM_HISTORY'
	    ELSE CONCAT
	            (
                    SUBSTR(rslt.date_of_service, 1, 4), '-',
                    SUBSTR(rslt.date_of_service, 6, 2), '-01'
                )
	END                                                                                     AS part_best_date,        
-------------------------------------------------------------------------------------------------
---------- Census Fields not captured by our Model
-------------------------------------------------------------------------------------------------
    diag.accn_id                      AS  diag_accn_id            ,
    diag.date_of_service              AS  date_of_service         ,
    diag.lab_code                     AS  diag_lab_code           ,
    diag.acct_id                      AS  acct_id                 ,
    diag.acct_number                  AS  acct_number             ,
    diag.s_icd_codeset_ind            AS  s_icd_codeset_ind       ,
    diag.diag_code                    AS  diag_code               ,
    diag.icd_codeset_ind              AS  icd_codeset_ind         ,
    diag.dos_yyyymm                   AS  diag_dos_yyyymm         ,
    rslt.accn_id                      AS  rslt_accn_id            ,
    rslt.lab_code                     AS  lab_code                ,
    rslt.phy_id                       AS  phy_id                  ,
    rslt.accession_number             AS  rslt_accession_number   ,
    rslt.accn_dom_id                  AS  accn_dom_id             ,
    rslt.cmdm_spclty_cd               AS  cmdm_spclty_cd          ,
    rslt.acct_name                    AS  acct_name               ,
    rslt.cmdm_licstate                AS  cmdm_licstate           ,
    rslt.billing_client_number        AS  billing_client_number   ,
    rslt.fasting_hours                AS  fasting_hours           ,
    rslt.qbs_ordering_client_num      AS  qbs_ordering_client_num ,
    rslt.date_order_entry             AS  date_order_entry        ,
    rslt.informed_consent_flag        AS  informed_consent_flag   ,
    rslt.legal_entity                 AS  legal_entity            ,
    rslt.specimen_type                AS  specimen_type           ,
    rslt.ordering_site_code           AS  ordering_site_code      ,
    rslt.canceled_accn_ind            AS  canceled_accn_ind       ,
    rslt.copy_to_clns                 AS  copy_to_clns            ,
    rslt.non_physician_name           AS  non_physician_name      ,
    rslt.non_physician_id             AS  non_physician_id        ,
    rslt.long_description             AS  long_description        ,
    rslt.phy_name                     AS  phy_first_name          ,
    rslt.suffix                       AS  suffix                  ,
    rslt.degree                       AS  degree                  ,
    rslt.local_profile_code           AS  local_profile_code      ,
    rslt.idw_analyte_code             AS  idw_analyte_code        ,
    rslt.qls_service_code             AS  qls_service_code        ,
    rslt.reportable_results_ind       AS  reportable_results_ind  ,
    rslt.ord_seq                      AS  ord_seq                 ,
    rslt.res_seq                      AS  res_seq                 ,
    rslt.abnormal_ind                 AS  abnormal_ind            ,
    rslt.result_type                  AS  result_type             ,
    rslt.amended_report_ind           AS  amended_report_ind      ,
    rslt.alpha_normal_flag            AS  alpha_normal_flag       ,
    rslt.instrument_id                AS  instrument_id           ,
    rslt.result_release_date          AS  result_release_date     ,
    rslt.enterprise_ntc_code          AS  enterprise_ntc_code     ,
    rslt.idw_local_order_code         AS  idw_local_order_code    ,
    rslt.derived_profile_code         AS  derived_profile_code    ,
    rslt.qtim_as_ordered_code         AS  qtim_as_ordered_code    ,
    rslt.qtim_profile_ind             AS  qtim_profile_ind        ,
    rslt.dos_yyyymm                   AS  rslt_dos_yyyymm         ,
    rslt.sister_lab                   AS  sister_lab              ,
    rslt.bill_type_cd                 AS  bill_type_cd            ,
    rslt.idw_report_change_status     AS  idw_report_change_status,
    rslt.ins_seq                      AS  ins_seq                 ,
    rslt.active_ind                   AS  active_ind              ,
    rslt.bill_code                    AS  bill_code               ,
    rslt.insurance_billing_type       AS  insurance_billing_type  ,
    rslt.qbs_payor_cd                 AS  qbs_payor_cd            ,
    rslt.dos_id                       AS  dos_id                  ,
    rslt.lab_name                     AS  lab_name                ,
    rslt.lab_lis_type                 AS  lab_lis_type            ,
    rslt.confidential_order_ind       AS  confidential_order_ind  ,
    rslt.daard_client_flag            AS  daard_client_flag       ,
    ptnt.ptnt_accession_number   ,
    ptnt.ptnt_date_of_service    ,
    ptnt.ptnt_lab_code           ,
    ptnt.accn_enterprise_id      ,
    ptnt.age_code                ,
    ptnt.species                 ,
    ptnt.pat_country             ,
    ptnt.external_patient_id     ,
    ptnt.pat_master_id           ,
    ptnt.lab_reference_number    ,
    ptnt.room_number             ,
    ptnt.bed_number              ,
    ptnt.hospital_location       ,
    ptnt.ward                    ,
    ptnt.admission_date          ,
    ptnt.health_id               ,
    ptnt.pm_eid                  ,
    ptnt.idw_pm_email_address
	

FROM order_result rslt
JOIN labtest_quest_rinse_setup ptnt ON rslt.unique_accession_id = ptnt.unique_accession_id
LEFT OUTER JOIN diagnosis diag ON rslt.unique_accession_id = diag.unique_accession_id 


WHERE  LOWER(COALESCE(rslt.unique_accession_id, '')) <> 'unique_accession_id'

