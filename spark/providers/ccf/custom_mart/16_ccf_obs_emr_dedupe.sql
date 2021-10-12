SELECT
    CURRENT_DATE()                                                             AS crt_dt,
    ----------------------------------------------------------------------------------------------------------------------
    -- data_set_nm
    ----------------------------------------------------------------------------------------------------------------------
    SPLIT(obs_emr.input_file_name, '/')[SIZE(SPLIT(obs_emr.input_file_name, '/')) - 1] AS data_set_nm,    
    ----------------------------------------------------------------------------------------------------------------------
    pay.hvid 																   AS hvid,
    ----------------------------------------------------------------------------------------------------------------------
    -- ptnt_birth_yr
    ----------------------------------------------------------------------------------------------------------------------
	CAST(
    	CAP_YEAR_OF_BIRTH
    	    (
    	        CAST(COALESCE(pay.age,  mptnt.current_age) AS INT),
    	        CASE WHEN LENGTH(COALESCE(obs_emr.obs_test_result_date,'')) =  11  
                     THEN TO_DATE(obs_emr.obs_test_result_date,  'dd-MMM-yyyy')
                     WHEN LENGTH(COALESCE(obs_emr.obs_test_result_date,'')) =  9
                     THEN TO_DATE(obs_emr.obs_test_result_date,  'dd-MMM-yy')
                     ELSE NULL                                           
                END,
    	        CAST(COALESCE(pay.yearofbirth, mptnt.birth_year) AS INT)
    	    )																					
	    AS INT)                                                                AS ptnt_birth_yr,
    ----------------------------------------------------------------------------------------------------------------------
    -- ptnt_age_num
    ----------------------------------------------------------------------------------------------------------------------
    CAP_AGE(
        VALIDATE_AGE
            (
                CAST(COALESCE(pay.age,  mptnt.current_age) AS INT),
    	        CASE WHEN LENGTH(COALESCE(obs_emr.obs_test_result_date,'')) =  11  
                     THEN TO_DATE(obs_emr.obs_test_result_date,  'dd-MMM-yyyy')
                     WHEN LENGTH(COALESCE(obs_emr.obs_test_result_date,'')) =  9
                     THEN TO_DATE(obs_emr.obs_test_result_date,  'dd-MMM-yy')
                     ELSE NULL                                           
                END,
    	        CAST(COALESCE(pay.yearofbirth, mptnt.birth_year) AS INT)
            )
          )                                                                  AS ptnt_age_num,
    ----------------------------------------------------------------------------------------------------------------------
    -- ptnt_gender_cd
    ----------------------------------------------------------------------------------------------------------------------
    CASE
    	WHEN pay.gender IS NULL AND mptnt.gender IS NULL               THEN NULL
    	WHEN SUBSTR(UPPER(pay.gender ),   1, 1) IN ('F', 'M', 'U')       THEN SUBSTR(UPPER(pay.gender ), 1, 1)
    	WHEN SUBSTR(UPPER(mptnt.gender ), 1, 1) IN ('F', 'M', 'U')     THEN SUBSTR(UPPER(mptnt.gender ), 1, 1)
        ELSE 'U' 
    END                                                                    AS ptnt_gender_cd,
    ----------------------------------------------------------------------------------------------------------------------
    -- ptnt_state_cd
    ----------------------------------------------------------------------------------------------------------------------
    VALIDATE_STATE_CODE(UPPER(pay.state))	                               AS ptnt_state_cd,
    ----------------------------------------------------------------------------------------------------------------------
    -- ptnt_zip3_cd
    ----------------------------------------------------------------------------------------------------------------------
	MASK_ZIP_CODE(pay.threedigitzip)                                       AS ptnt_zip3_cd,
    ----------------------------------------------------------------------------------------------------------------------
	obs_emr.deidentified_master_patient_id                                 AS deidentified_master_patient_id,
	obs_emr.deidentified_patient_id                                        AS deidentified_patient_id,
    obs_emr.data_source                                                    AS data_source,
    obs_emr.obs_id                                                         AS obs_id,
    obs_emr.visit_encounter_id                                             AS visit_encounter_id,
    
    obs_emr.obs_test_concept_name                               AS obs_test_concept_name,
    obs_emr.obs_test_concept_code                               AS obs_test_concept_code,
    obs_emr.obs_test_system_name                                AS obs_test_system_name,

    obs_emr.src_obs_test_concept_code                        AS src_obs_test_concept_code,
    obs_emr.src_obs_test_system_name                         AS src_obs_test_system_name,
    obs_emr.src_obs_test_system_desc                         AS src_obs_test_system_desc,
    
    obs_emr.test_unit                                        AS test_unit,
    obs_emr.test_result_numeric                              AS test_result_numeric,
    ----------------------------------------------------------------------------------------------------------------------
    --  obs_test_result_date
    ----------------------------------------------------------------------------------------------------------------------
    CAST(
            CASE WHEN LENGTH(COALESCE(obs_emr.obs_test_result_date,'')) =  11  
                 THEN TO_DATE(obs_emr.obs_test_result_date,  'dd-MMM-yyyy')
                 WHEN LENGTH(COALESCE(obs_emr.obs_test_result_date,'')) =  9
                 THEN TO_DATE(obs_emr.obs_test_result_date,  'dd-MMM-yy')
                 WHEN LENGTH(COALESCE(obs_emr.obs_test_result_date,'')) =  4 
                 THEN obs_emr.obs_test_result_date 
                 ELSE NULL                                           
            END  AS STRING
        )                                                   AS obs_test_result_date,
   ----------------------------------------------------------------------------------------------------------------------
    obs_emr.descriptive_symp_test_results                  AS descriptive_symp_test_results,
    
    obs_emr.obs_test_ty_concept_code                        AS obs_test_ty_concept_code,
    obs_emr.obs_test_ty_concept_name                        AS obs_test_ty_concept_name,
    
    'observations_emr'                                                          AS prmy_src_tbl_nm,
    'ccf'                                                                     AS part_provider,
    DATE_FORMAT(current_date, 'yyyy-MM-dd')                                     AS part_mth
FROM  observations_emr obs_emr
LEFT OUTER JOIN master_patient mptnt
ON  LOWER(COALESCE(obs_emr.deidentified_master_patient_id, 'NA')) = LOWER(COALESCE(mptnt.deidentified_master_patient_id, 'empty')) 
LEFT OUTER JOIN matching_payload pay 
ON LOWER(COALESCE(mptnt.deidentified_master_patient_id, 'NA')) = LOWER(COALESCE(pay.claimid, 'empty')) 
WHERE obs_emr.obs_id <> 'OBS_ID'
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18,19,20,21,22,23,24,25,26,27
--limit 10
