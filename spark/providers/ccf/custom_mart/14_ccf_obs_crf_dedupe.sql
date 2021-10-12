SELECT
    CURRENT_DATE()                                                             AS crt_dt,
    ----------------------------------------------------------------------------------------------------------------------
    -- data_set_nm
    ----------------------------------------------------------------------------------------------------------------------
    SPLIT(obs_crf.input_file_name, '/')[SIZE(SPLIT(obs_crf.input_file_name, '/')) - 1] AS data_set_nm,    
    ----------------------------------------------------------------------------------------------------------------------
    pay.hvid 																   AS hvid,
    ----------------------------------------------------------------------------------------------------------------------
    -- ptnt_birth_yr
    ----------------------------------------------------------------------------------------------------------------------
	CAST(
    	CAP_YEAR_OF_BIRTH
    	    (
    	        CAST(COALESCE(pay.age,  mptnt.current_age) AS INT),
    	        COALESCE(
    	                    CASE WHEN LENGTH(COALESCE(obs_crf.obs_test_result_date,'')) =  11  
                                 THEN TO_DATE(obs_crf.obs_test_result_date,  'dd-MMM-yyyy')
                                 WHEN LENGTH(COALESCE(obs_crf.obs_test_result_date,'')) =  9
                                 THEN TO_DATE(obs_crf.obs_test_result_date,  'dd-MMM-yy')
                                 ELSE NULL                                           
                            END,
    	                    CASE WHEN LENGTH(COALESCE(obs_crf.obs_event_start_date,'')) =  11  
                                 THEN TO_DATE(obs_crf.obs_event_start_date,  'dd-MMM-yyyy')
                                 WHEN LENGTH(COALESCE(obs_crf.obs_event_start_date,'')) =  9
                                 THEN TO_DATE(obs_crf.obs_event_start_date,  'dd-MMM-yy')
                                 ELSE NULL                                           
                            END,
    	                    CASE WHEN LENGTH(COALESCE(obs_crf.obs_event_end_date,'')) =  11  
                                 THEN TO_DATE(obs_crf.obs_event_end_date,  'dd-MMM-yyyy')
                                 WHEN LENGTH(COALESCE(obs_crf.obs_event_end_date,'')) =  9
                                 THEN TO_DATE(obs_crf.obs_event_end_date,  'dd-MMM-yy')
                                 ELSE NULL                                           
                            END
    	                ),
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
   	            COALESCE(
    	                    CASE WHEN LENGTH(COALESCE(obs_crf.obs_test_result_date,'')) =  11  
                                 THEN TO_DATE(obs_crf.obs_test_result_date,  'dd-MMM-yyyy')
                                 WHEN LENGTH(COALESCE(obs_crf.obs_test_result_date,'')) =  9
                                 THEN TO_DATE(obs_crf.obs_test_result_date,  'dd-MMM-yy')
                                 ELSE NULL                                           
                            END,
    	                    CASE WHEN LENGTH(COALESCE(obs_crf.obs_event_start_date,'')) =  11  
                                 THEN TO_DATE(obs_crf.obs_event_start_date,  'dd-MMM-yyyy')
                                 WHEN LENGTH(COALESCE(obs_crf.obs_event_start_date,'')) =  9
                                 THEN TO_DATE(obs_crf.obs_event_start_date,  'dd-MMM-yy')
                                 ELSE NULL                                           
                            END,
    	                    CASE WHEN LENGTH(COALESCE(obs_crf.obs_event_end_date,'')) =  11  
                                 THEN TO_DATE(obs_crf.obs_event_end_date,  'dd-MMM-yyyy')
                                 WHEN LENGTH(COALESCE(obs_crf.obs_event_end_date,'')) =  9
                                 THEN TO_DATE(obs_crf.obs_event_end_date,  'dd-MMM-yy')
                                 ELSE NULL                                           
                            END
    	                ),
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
	obs_crf.deidentified_master_patient_id                                 AS deidentified_master_patient_id,
	obs_crf.deidentified_patient_id                                        AS deidentified_patient_id,
    obs_crf.data_source                                                    AS data_source,
    obs_crf.obs_id                                                         AS obs_id,
    obs_crf.visit_encounter_id                                             AS visit_encounter_id,
    
    obs_crf.obs_test_concept_name                               AS obs_test_concept_name,
    ----------------------------------------------------------------------------------------------------------------------
    --  obs_test_concept_code
    ----------------------------------------------------------------------------------------------------------------------
    CASE WHEN SUBSTR(UPPER(obs_crf.obs_test_system_name), 1,3) = 'ICD'  
         THEN
            CLEAN_UP_DIAGNOSIS_CODE
            (
                obs_crf.obs_test_concept_code,
                -- ICD9 or ICD10
                CASE WHEN SUBSTR(REPLACE(REPLACE(REPLACE (obs_crf.obs_test_system_name, ' ', ''), '-', ''), '*', ''), 4,1) = '9'
                     THEN '01'
                     ELSE '02'
                END,
                COALESCE(
    	                    CASE WHEN LENGTH(COALESCE(obs_crf.obs_test_result_date,'')) =  11  
                                 THEN TO_DATE(obs_crf.obs_test_result_date,  'dd-MMM-yyyy')
                                 WHEN LENGTH(COALESCE(obs_crf.obs_test_result_date,'')) =  9
                                 THEN TO_DATE(obs_crf.obs_test_result_date,  'dd-MMM-yy')
                                 ELSE NULL                                           
                            END,
    	                    CASE WHEN LENGTH(COALESCE(obs_crf.obs_event_start_date,'')) =  11  
                                 THEN TO_DATE(obs_crf.obs_event_start_date,  'dd-MMM-yyyy')
                                 WHEN LENGTH(COALESCE(obs_crf.obs_event_start_date,'')) =  9
                                 THEN TO_DATE(obs_crf.obs_event_start_date,  'dd-MMM-yy')
                                 ELSE NULL                                           
                            END,
    	                    CASE WHEN LENGTH(COALESCE(obs_crf.obs_event_end_date,'')) =  11  
                                 THEN TO_DATE(obs_crf.obs_event_end_date,  'dd-MMM-yyyy')
                                 WHEN LENGTH(COALESCE(obs_crf.obs_event_end_date,'')) =  9
                                 THEN TO_DATE(obs_crf.obs_event_end_date,  'dd-MMM-yy')
                                 ELSE NULL                                           
                            END
    	                )
            )       ---If date is yyyy, it will be null and the UDF should still work fine                                 
        ELSE
            obs_crf.obs_test_concept_code
    END                                                      AS obs_test_concept_code,
    ----------------------------------------------------------------------------------------------------------------------
    obs_crf.obs_test_system_name                             AS obs_test_system_name,

    obs_crf.src_obs_test_concept_code                        AS src_obs_test_concept_code,
    obs_crf.src_obs_test_system_name                         AS src_obs_test_system_name,
    obs_crf.src_obs_test_system_desc                         AS src_obs_test_system_desc,

    obs_crf.test_result_numeric                              AS test_result_numeric,
    ----------------------------------------------------------------------------------------------------------------------
    --  obs_test_result_date
    ----------------------------------------------------------------------------------------------------------------------
    CAST(
            CASE WHEN LENGTH(COALESCE(obs_crf.obs_test_result_date,'')) =  11  
                 THEN TO_DATE(obs_crf.obs_test_result_date,  'dd-MMM-yyyy')
                 WHEN LENGTH(COALESCE(obs_crf.obs_test_result_date,'')) =  9
                 THEN TO_DATE(obs_crf.obs_test_result_date,  'dd-MMM-yy')
                 WHEN LENGTH(COALESCE(obs_crf.obs_test_result_date,'')) =  4 
                 THEN obs_crf.obs_test_result_date 
                 ELSE NULL                                           
            END  AS STRING
        )                                                   AS obs_test_result_date,
    ----------------------------------------------------------------------------------------------------------------------
    --  descriptive_symp_test_results
    ----------------------------------------------------------------------------------------------------------------------
    CASE WHEN obs_crf.obs_test_concept_name is NOT NULL AND obs_crf.obs_test_result_date IS NOT NULL
         THEN obs_crf.descriptive_symp_test_results 
         ELSE NULL
    END                                                     AS descriptive_symp_test_results,
    ----------------------------------------------------------------------------------------------------------------------
    obs_crf.obs_test_ty_concept_code                        AS obs_test_ty_concept_code,
    obs_crf.obs_test_ty_concept_name                        AS  obs_test_ty_concept_name,
    
    obs_crf.ana_site_concept_name                           AS  ana_site_concept_name,
    obs_crf.ana_site_concept_code                           AS  ana_site_concept_code,
    ----------------------------------------------------------------------------------------------------------------------
    --  obs_event_start_date
    ----------------------------------------------------------------------------------------------------------------------
    CAST(
            CASE WHEN LENGTH(COALESCE(obs_crf.obs_event_start_date,'')) =  11  
                 THEN TO_DATE(obs_crf.obs_event_start_date,  'dd-MMM-yyyy')
                 WHEN LENGTH(COALESCE(obs_crf.obs_event_start_date,'')) =  9
                 THEN TO_DATE(obs_crf.obs_event_start_date,  'dd-MMM-yy')
                 WHEN LENGTH(COALESCE(obs_crf.obs_event_start_date,'')) =  4 
                 THEN obs_crf.obs_event_start_date 
                 ELSE NULL                                           
            END  AS STRING
        )                                                   AS obs_event_start_date,
    ----------------------------------------------------------------------------------------------------------------------
    --  obs_event_end_date
    ----------------------------------------------------------------------------------------------------------------------
    CAST(
            CASE WHEN LENGTH(COALESCE(obs_crf.obs_event_end_date,'')) =  11  
                 THEN TO_DATE(obs_crf.obs_event_end_date,  'dd-MMM-yyyy')
                 WHEN LENGTH(COALESCE(obs_crf.obs_event_end_date,'')) =  9
                 THEN TO_DATE(obs_crf.obs_event_end_date,  'dd-MMM-yy')
                 WHEN LENGTH(COALESCE(obs_crf.obs_event_end_date,'')) =  4 
                 THEN obs_crf.obs_event_end_date 
                 ELSE NULL                                           
            END  AS STRING
        )                                                  AS obs_event_end_date,
    'observations_crf'                                                                 AS prmy_src_tbl_nm,
    'ccf'                                                                     AS part_provider,
    DATE_FORMAT(current_date, 'yyyy-MM-dd')                                     AS part_mth
FROM  observations_crf obs_crf
LEFT OUTER JOIN master_patient mptnt
ON  LOWER(COALESCE(obs_crf.deidentified_master_patient_id, 'NA')) = LOWER(COALESCE(mptnt.deidentified_master_patient_id, 'empty')) 
LEFT OUTER JOIN matching_payload pay 
ON LOWER(COALESCE(mptnt.deidentified_master_patient_id, 'NA')) = LOWER(COALESCE(pay.claimid, 'empty')) 
WHERE obs_crf.obs_id <> 'OBS_ID'
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18,19,20,21,22,23,24,25,26,27,28,29,30
--limit 10
