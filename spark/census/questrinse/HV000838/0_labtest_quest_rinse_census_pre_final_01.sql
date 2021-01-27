SELECT
    CONCAT
        (
        rslt.unique_accession_id,
        COALESCE(CONCAT('_',    rslt.accn_id),''),
        COALESCE(CONCAT('_',    rslt.ord_seq),''),
        COALESCE(CONCAT('_',    rslt.res_seq),''),
        COALESCE(CONCAT('_',    rslt.ins_seq),'')
        )                                                                               AS HV_claim_id,
    CASE
        WHEN pay.hvid           IS NOT NULL THEN obfuscate_hvid(pay.hvid, 'questrinse')
        ELSE NULL
    END                                                                                     AS hvid,

    CAST(CURRENT_DATE() AS STRING)                                                          AS created,
	'09'                                                                                    AS model_version,
    ---------------------------------------------------------------------------------------------------------------------
    ------- This is a boiler plate code needed for every census feed (added after consultation with Ilia JKS 2020-09-08 )
    ---------------------------------------------------------------------------------------------------------------------
    REPLACE
    (
        SPLIT(rslt.input_file_name, '/')[SIZE(SPLIT(rslt.input_file_name, '/')) - 1],
        'order_result_',
        ''
    )                                                                                       AS data_set,
	'187'                                                                                   AS data_feed,
	'7'                                                                                     AS data_vendor,
	/* patient_gender */
    	(
        	CASE
        	    WHEN SUBSTR(UPPER(ptnt.gender), 1, 1) IN ('F', 'M') THEN SUBSTR(UPPER(ptnt.gender), 1, 1)
        	    WHEN SUBSTR(UPPER(pay.gender ), 1, 1) IN ('F', 'M') THEN SUBSTR(UPPER(pay.gender ), 1, 1)
        	    ELSE 'U'
        	END
	    )                                                                                   AS HV_patient_gender,
	/* patient_age - (Notes for me - Per our transformation - if the pay.age is NULL the patient_age becomes NULL)
	   CASE is added to increase the performance UDF cap_year_of_birth(age, date_service, year_of_birth)  EXTRACT_DATE('01/31/2017', '%Y-%m-%d') */
	CASE
	    WHEN ptnt.numeric_age IS NULL  AND pay.age IS NULL THEN NULL
	    ELSE
	    CAP_AGE(COALESCE(ptnt.numeric_age, pay.age))
	END                                                                                     AS HV_patient_age,
	CAP_YEAR_OF_BIRTH
	(
	    COALESCE(ptnt.numeric_age, pay.age),
    	CAST(TO_DATE(rslt.date_of_service, 'yyyy-MM-dd') AS DATE),
    	pay.yearofbirth
	)                                                                                       AS HV_patient_year_of_birth,
	MASK_ZIP_CODE(SUBSTR(pay.threedigitzip, 1, 3))                                          AS HV_patient_zip3,
    VALIDATE_STATE_CODE(UPPER(COALESCE(ptnt.pat_state, pay.state)))                         AS HV_patient_state,
    ---------------------------------------------------------------------------------------------------------
    ------------------- Removing all the transformation for  (2020-05-06)
    ------------------- date_service, date_specimen, date_report, loinc_code, diagnosis_code
    ---------------------------------------------------------------------------------------------------------
    rslt.date_of_service                                                                     AS date_service,
    rslt.date_of_collection                                                                  AS date_specimen,
    COALESCE(rslt.date_final_report,rslt.date_reported)                                      AS HV_date_report,
    rslt.loinc_code	                                                                         AS loinc_code,
    ----------------------------------------------------------------------------------------------------------------> LOINC From HV
    --When the rslt.loinc_code is NULL or starts with the letter L, or when the lab_id is liste code
    --then populate target with hv_loinc.loinc_code where available. Else populate rslt.loinc_code
     ----------------------------------------------------------------------------------------------------------------> LOINC From HV
    CASE
        WHEN (
               LPAD(rslt.lab_id , 2, '0') IN ('01', '03', '04',	'08', '09', '10', '11', '13', '14', '35', '37', '42', '43', '47', '48', '53', '54' )
                        OR SUBSTR(UPPER(rslt.loinc_code),1, 1) ='L' OR rslt.loinc_code IS NULL
             ) THEN
                    CASE
                        WHEN loinc_delta.loinc_code IS NOT NULL THEN loinc_delta.loinc_code
                        WHEN loinc.loinc_code IS NOT NULL THEN loinc.loinc_code
                    ELSE rslt.loinc_code
                    END
        ELSE rslt.loinc_code
    END                                                                                      AS hv_loinc_code,
    ---------------------------------------------------------------------------------------------------------------->
    rslt.lab_id                                                                              AS lab_id                ,
    rslt.obs_id                                                                              AS test_id               ,
    CASE
        WHEN UPPER(rslt.requisition_number) = 'NONE' THEN NULL
        ELSE rslt.requisition_number
    END                                                                                      AS HV_test_number        ,
    rslt.local_profile_code                                                                  AS test_battery_local_id ,
    rslt.standard_profile_code                                                               AS test_battery_std_id   ,
    rslt.profile_name                                                                        AS test_battery_name     ,
    rslt.idw_local_order_code                                                                AS test_ordered_local_id ,
    rslt.standard_order_code                                                                 AS test_ordered_std_id   ,
    rslt.order_name                                                                          AS test_ordered_name     ,
    rslt.local_result_code                                                                   AS result_id             ,
    rslt.result_value                                                                        AS result                ,
    rslt.result_name                                                                         AS result_name           ,
    rslt.units                                                                               AS result_unit_of_measure,
    rslt.result_type                                                                         AS  result_desc          ,
    --------------------------------------------------------------------------------------------------------------
    ------------------- New logic from QUEST JKS 2020-06-17
    --------------------------------------------------------------------------------------------------------------
    CASE
        WHEN rslt.ref_range_alpha IS NOT NULL                                                                       THEN rslt.ref_range_alpha
        WHEN rslt.ref_range_alpha IS NULL   AND rslt.ref_range_low IS NOT NULL AND rslt.ref_range_high IS NOT NULL  THEN CONCAT(rslt.ref_range_low,' - ', rslt.ref_range_high)
        WHEN rslt.ref_range_alpha IS NULL   AND rslt.ref_range_low IS     NULL AND rslt.ref_range_high IS     NULL  THEN  NULL
        WHEN rslt.ref_range_alpha IS NULL   AND rslt.ref_range_low IS NOT NULL AND rslt.ref_range_high IS NULL      THEN  CONCAT('>= ', rslt.ref_range_low)
        WHEN rslt.ref_range_low   IS NULL   AND rslt.ref_range_high IS NOT NULL                                     THEN CONCAT('<= ', rslt.ref_range_high)
    ELSE NULL
    END                                                                                    AS HV_ref_range_alpha,
    --------------------------------------------------------------------------------------------------------------
    ------------------- Add COALESCE U (2020-05-28) This is request from QUEST via Will V
    --------------------------------------------------------------------------------------------------------------
    CASE
        WHEN COALESCE(rslt.fasting_ind,'') IN ('Y', 'N', 'U') THEN rslt.fasting_ind
        ELSE 'U'
    END	                                                                                     AS HV_fasting_status,

    -- --------------------------------------------------------------------------------------------------------------
    -- ------------------- Changed on 2020-07-24 "clean_up_diagnosis_code(diagnosis_code, diagnosis_code_qual, date_service)"
    -- --------------------------------------------------------------------------------------------------------------
    CASE
        WHEN diag.s_diag_code IS NULL THEN NULL
        WHEN CLEAN_UP_DIAGNOSIS_CODE(diag.s_diag_code,
            CASE
                WHEN diag.s_icd_codeset_ind = '9'  THEN '01'
                WHEN diag.s_icd_codeset_ind = '10' THEN '02'
            ELSE NULL
            END, CAST(TO_DATE(rslt.date_of_service, 'yyyy-MM-dd') AS DATE)) IS NULL THEN NULL
    ELSE
        CONCAT(diag.s_icd_codeset_ind, '^', UPPER(diag.s_diag_code))
    END                                                                                      AS HV_ONE_diagnosis_code ,
    CLEAN_UP_PROCEDURE_CODE(rslt.cpt_code)                                                   AS HV_procedure_code     ,
    CASE
        WHEN CLEAN_UP_PROCEDURE_CODE(rslt.cpt_code)  IS NOT NULL THEN 'CPT'
    ELSE NULL
    END                                                                                      AS HV_procedure_code_qual,
    CLEAN_UP_NPI_CODE(rslt.npi)                                                              AS HV_ordering_npi       ,
    rslt.ins_id                                                                              AS payer_id              ,
    rslt.company                                                                             AS payer_name            ,
    rslt.perf_lab_code                                                                       AS lab_other_id          ,
	'PERFORMINGLAB_PERFORMED'                                                                AS HV_lab_other_qual     ,
    rslt.market_type                                                                         AS ordering_market_type  ,
    rslt.client_specialty                                                                    AS ordering_specialty    ,
    rslt.cmdm_licnum                                                                         AS ordering_state_license,
    rslt.upin                                                                                AS ordering_upin         ,
    rslt.acct_address_1                                                                      AS ordering_address_1    ,
    rslt.acct_address_2                                                                      AS ordering_address_2    ,
    rslt.acct_city                                                                           AS ordering_city         ,

    VALIDATE_STATE_CODE(UPPER(rslt.acct_state))                                              AS HV_ordering_state     ,

    rslt.acct_zip                                                                            AS ordering_zip          ,
    'quest_rinse'                                                                            AS part_provider         ,
    /* part_best_date */
	CONCAT
	    (
                    SUBSTR(rslt.date_of_service, 1, 4), '-',
                    SUBSTR(rslt.date_of_service, 6, 2), '-01'
        )
	                                                                                         AS HV_part_best_date     ,
-------------------------------------------------------------------------------------------------
---------- Census Fields not captured by our Model
-------------------------------------------------------------------------------------------------
    rslt.unique_accession_id          AS  unique_accession_id     ,
    diag.accn_id                      AS  diag_accn_id            ,
    diag.date_of_service              AS  diag_date_of_service    ,
    diag.lab_code                     AS  diag_lab_code           ,
    diag.acct_id                      AS  acct_id                 ,
    diag.acct_number                  AS  acct_number             ,
    diag.dos_yyyymm                   AS  diag_dos_yyyymm         ,
    rslt.accn_id                      AS  rslt_accn_id            ,
    rslt.lab_code                     AS  lab_code                ,
    rslt.phy_id                       AS  phy_id                  ,
    rslt.accession_number             AS  rslt_accession_number   ,
    CAST(rslt.accn_dom_id AS BIGINT)  AS  accn_dom_id             ,
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
    rslt.phy_name                     AS  phy_name                , --- JKS 2020-06-08
    rslt.suffix                       AS  suffix                  ,
    rslt.degree                       AS  degree                  ,
    rslt.idw_analyte_code             AS  idw_analyte_code        ,
    rslt.qls_service_code             AS  qls_service_code        ,
    rslt.reportable_results_ind       AS  reportable_results_ind  ,
    rslt.ord_seq                      AS  ord_seq                 ,
    rslt.res_seq                      AS  res_seq                 ,
    rslt.abnormal_ind                 AS  abnormal_ind            ,
    rslt.amended_report_ind           AS  amended_report_ind      ,
    rslt.alpha_normal_flag            AS  alpha_normal_flag       ,
    rslt.instrument_id                AS  instrument_id           ,
    rslt.result_release_date          AS  result_release_date     ,
    rslt.enterprise_ntc_code          AS  enterprise_ntc_code     ,
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
    ptnt.accession_number             AS  ptnt_accession_number   ,
    ptnt.date_of_service              AS  ptnt_date_of_service    ,
    ptnt.lab_code                     AS  ptnt_lab_code           ,
    ptnt.accn_enterprise_id           AS  accn_enterprise_id      ,
    ptnt.age_code                     AS  age_code                ,
    ptnt.species                      AS  species                 ,
    ptnt.pat_country                  AS  pat_country             ,
    ptnt.external_patient_id          AS  external_patient_id     ,
    ptnt.pat_master_id                AS  pat_master_id           ,
    ptnt.lab_reference_number         AS  lab_reference_number    ,
    ptnt.room_number                  AS  room_number             ,
    ptnt.bed_number                   AS  bed_number              ,
    ptnt.hospital_location            AS  hospital_location       ,
    ptnt.ward                         AS  ward                    ,
    ptnt.admission_date               AS  admission_date          ,
    ptnt.health_id                    AS  health_id               ,
    ptnt.pm_eid                       AS  pm_eid                  ,
    ptnt.idw_pm_email_address         AS  idw_pm_email_address    ,
-------------------------------------------------------------------------------------------------
---------- New fields added per request from QUEST 2020-05-21
-------------------------------------------------------------------------------------------------
    rslt.date_reported                AS date_reported            ,
    rslt.ref_range_low                AS ref_range_low            ,
    rslt.ref_range_high               AS ref_range_high           ,
    rslt.ref_range_alpha              As ref_range_alpha          ,
-------------------------------------------------------------------------------------------------
---------- New fields added per request from QUEST 2020-06-08
-------------------------------------------------------------------------------------------------
    rslt.requisition_number           AS requisition_number       ,
    rslt.fasting_ind                  AS fasting_ind              ,
--  diag.s_diag_code                  AS s_diag_code              , ---- > removed on 2020-07-08
    rslt.cpt_code                     AS cpt_code                 ,
    rslt.npi                          AS npi                      ,
    rslt.phy_last_name                AS phy_last_name            ,
    rslt.phy_first_name               AS phy_first_name           ,
    rslt.phy_middle_name              AS phy_middle_name          ,
    rslt.acct_state                   AS acct_state               ,
-------------------------------------------------------------------------------------------------
---------- New fields added per request from QUEST 2020-06-17
-------------------------------------------------------------------------------------------------
    rslt.date_final_report            AS date_final_report        ,
    CASE
        WHEN diag.s_diag_code IS NULL THEN NULL
    ELSE CONCAT(diag.s_icd_codeset_ind, '^', UPPER(diag.s_diag_code))
    END                              AS s_diag_code_codeset_ind

FROM order_result rslt
LEFT OUTER JOIN diagnosis diag ON rslt.unique_accession_id = diag.unique_accession_id
LEFT OUTER JOIN transactions  ptnt ON rslt.unique_accession_id = ptnt.unique_accession_id
LEFT OUTER JOIN matching_payload  pay  ON ptnt.hvjoinkey           = pay.hvJoinKey
LEFT OUTER JOIN loinc_delta   ON CAST(TO_DATE(rslt.date_of_service, 'yyyy-MM-dd') AS DATE)  = CAST(TO_DATE(loinc_delta.date_of_service, 'yyyy-MM-dd') AS DATE)
                                       AND UPPER(rslt.result_name) = UPPER(loinc_delta.upper_result_name)
                                       AND rslt.local_result_code = loinc_delta.local_result_code
                                       AND rslt.units = loinc_delta.units
LEFT OUTER JOIN loinc           ON CAST(TO_DATE(rslt.date_of_service, 'yyyy-MM-dd') AS DATE)  = CAST(TO_DATE(loinc.date_of_service, 'yyyy-MM-dd') AS DATE)
                                       AND UPPER(rslt.result_name) = UPPER(loinc.upper_result_name)
                                       AND rslt.local_result_code = loinc.local_result_code
                                       AND rslt.units             = loinc.units
WHERE EXISTS
/* Select only valid U.S. states and territories. Added PA as default state if there is no state found*/
    (
        SELECT 1
        FROM ref_geo_state sts
        WHERE UPPER(COALESCE(ptnt.pat_state, pay.state, 'PA')) = sts.geo_state_pstl_cd
    )
/* Eliminate column heade rows */
AND LOWER(COALESCE(rslt.unique_accession_id, '')) <> 'unique_accession_id'
------- Eliminate Non US patient
AND UPPER(COALESCE(SUBSTR(ptnt.pat_country,1,2),'US')) = 'US'
------- Eliminate Non US patient from result
AND EXISTS
    (
        SELECT 1
        FROM ref_geo_state sts
        WHERE UPPER(COALESCE(rslt.acct_state, 'PA')) = sts.geo_state_pstl_cd
    )
