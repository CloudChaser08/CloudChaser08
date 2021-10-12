SELECT
    CURRENT_DATE()                                                             AS crt_dt,
    ----------------------------------------------------------------------------------------------------------------------
    -- data_set_nm
    ----------------------------------------------------------------------------------------------------------------------
    SPLIT(ptnt_prblm.input_file_name, '/')[SIZE(SPLIT(ptnt_prblm.input_file_name, '/')) - 1] AS data_set_nm,    
    ----------------------------------------------------------------------------------------------------------------------
    pay.hvid 																   AS hvid,
    ----------------------------------------------------------------------------------------------------------------------
    -- ptnt_birth_yr
    ----------------------------------------------------------------------------------------------------------------------
	CAST(
    	CAP_YEAR_OF_BIRTH
    	    (
    	        CAST(COALESCE(pay.age,  mptnt.current_age) AS INT),
                COALESCE(    CASE WHEN LENGTH(COALESCE(ptnt_prblm.problem_start_date,'')) =  11  
                                  THEN TO_DATE(ptnt_prblm.problem_start_date,  'dd-MMM-yyyy')
                                  WHEN LENGTH(COALESCE(ptnt_prblm.problem_start_date,'')) =  9
                                  THEN TO_DATE(ptnt_prblm.problem_start_date,  'dd-MMM-yy')
                                  ELSE NULL                                           
                             END,
                             CASE WHEN LENGTH(COALESCE(ptnt_prblm.problem_end_date,'')) =  11  
                                  THEN TO_DATE(ptnt_prblm.problem_end_date,  'dd-MMM-yyyy')
                                  WHEN LENGTH(COALESCE(ptnt_prblm.problem_end_date,'')) =  9
                                  THEN TO_DATE(ptnt_prblm.problem_end_date,  'dd-MMM-yy')
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
    	        COALESCE(    CASE WHEN LENGTH(COALESCE(ptnt_prblm.problem_start_date,'')) =  11  
                                  THEN TO_DATE(ptnt_prblm.problem_start_date,  'dd-MMM-yyyy')
                                  WHEN LENGTH(COALESCE(ptnt_prblm.problem_start_date,'')) =  9
                                  THEN TO_DATE(ptnt_prblm.problem_start_date,  'dd-MMM-yy')
                                  ELSE NULL                                           
                             END,
                             CASE WHEN LENGTH(COALESCE(ptnt_prblm.problem_end_date,'')) =  11  
                                  THEN TO_DATE(ptnt_prblm.problem_end_date,  'dd-MMM-yyyy')
                                  WHEN LENGTH(COALESCE(ptnt_prblm.problem_end_date,'')) =  9
                                  THEN TO_DATE(ptnt_prblm.problem_end_date,  'dd-MMM-yy')
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
	ptnt_prblm.deidentified_master_patient_id                                 AS deidentified_master_patient_id,
	ptnt_prblm.deidentified_patient_id                                        AS deidentified_patient_id,
    ptnt_prblm.data_source                                                    AS data_source,
    CAST (null    AS STRING)                                                  AS problem,
    ptnt_prblm.problem_id                                                     AS problem_id,
    ptnt_prblm.visit_encounter_id                                             AS visit_encounter_id,
    ptnt_prblm.prob_concept_name                                              AS prob_concept_name,
    ----------------------------------------------------------------------------------------------------------------------
    -- prob_code
    ----------------------------------------------------------------------------------------------------------------------
    CASE WHEN SUBSTR(UPPER(ptnt_prblm.prob_code_system_name), 1,3) = 'ICD'  
         THEN
            CLEAN_UP_DIAGNOSIS_CODE
            (
                ptnt_prblm.prob_code,
                -- ICD9 or ICD10
                CASE WHEN SUBSTR(REPLACE(REPLACE(REPLACE (ptnt_prblm.prob_code_system_name, ' ', ''), '-', ''), '*', ''), 4,1) = '9'
                     THEN '01'
                     ELSE '02'
                END,
    	        COALESCE(    CASE WHEN LENGTH(COALESCE(ptnt_prblm.problem_start_date,'')) =  11  
                                  THEN TO_DATE(ptnt_prblm.problem_start_date,  'dd-MMM-yyyy')
                                  WHEN LENGTH(COALESCE(ptnt_prblm.problem_start_date,'')) =  9
                                  THEN TO_DATE(ptnt_prblm.problem_start_date,  'dd-MMM-yy')
                                  ELSE NULL                                           
                             END,
                             CASE WHEN LENGTH(COALESCE(ptnt_prblm.problem_end_date,'')) =  11  
                                  THEN TO_DATE(ptnt_prblm.problem_end_date,  'dd-MMM-yyyy')
                                  WHEN LENGTH(COALESCE(ptnt_prblm.problem_end_date,'')) =  9
                                  THEN TO_DATE(ptnt_prblm.problem_end_date,  'dd-MMM-yy')
                                  ELSE NULL                                           
                             END
                        ) 
            )       ---If date is yyyy, it will be null and the UDF should still work fine                                 
        ELSE
            ptnt_prblm.prob_code 
    END                                                                       AS prob_code,
    ----------------------------------------------------------------------------------------------------------------------
    ptnt_prblm.prob_code_system_name                                          AS prob_code_system_name,
    ----------------------------------------------------------------------------------------------------------------------
    -- source_prob_code
    ----------------------------------------------------------------------------------------------------------------------
    CASE WHEN SUBSTR(UPPER(ptnt_prblm.source_prob_code_system_name), 1,3) = 'ICD'  
         THEN
            CLEAN_UP_DIAGNOSIS_CODE
            (
                 ptnt_prblm.source_prob_code,
                -- ICD9 or ICD10
                CASE WHEN SUBSTR(REPLACE(REPLACE(REPLACE (ptnt_prblm.source_prob_code_system_name, ' ', ''), '-', ''), '*', ''), 4,1) = '9'
                     THEN '01'
                     ELSE '02'
                END,
    	        COALESCE(    CASE WHEN LENGTH(COALESCE(ptnt_prblm.problem_start_date,'')) =  11  
                                  THEN TO_DATE(ptnt_prblm.problem_start_date,  'dd-MMM-yyyy')
                                  WHEN LENGTH(COALESCE(ptnt_prblm.problem_start_date,'')) =  9
                                  THEN TO_DATE(ptnt_prblm.problem_start_date,  'dd-MMM-yy')
                                  ELSE NULL                                           
                             END,
                             CASE WHEN LENGTH(COALESCE(ptnt_prblm.problem_end_date,'')) =  11  
                                  THEN TO_DATE(ptnt_prblm.problem_end_date,  'dd-MMM-yyyy')
                                  WHEN LENGTH(COALESCE(ptnt_prblm.problem_end_date,'')) =  9
                                  THEN TO_DATE(ptnt_prblm.problem_end_date,  'dd-MMM-yy')
                                  ELSE NULL                                           
                             END
                        )
            )       ---If date is yyyy, it will be null and the UDF should still work fine                                 
        ELSE
             ptnt_prblm.source_prob_code 
    END                                                                      AS source_prob_code,
    ----------------------------------------------------------------------------------------------------------------------
    ptnt_prblm.source_prob_code_system_name                                  AS source_prob_code_system_name,
    ptnt_prblm.source_prob_desc                                              AS source_prob_desc,                
    --ptnt_prblm.problem                                                     AS problem,                
    ptnt_prblm.problem_type                                                  AS problem_type,
    ----------------------------------------------------------------------------------------------------------------------
    --   problem_start_date
    ----------------------------------------------------------------------------------------------------------------------
    CAST(
            CASE WHEN LENGTH(COALESCE(ptnt_prblm.problem_start_date,'')) =  11  
                 THEN TO_DATE(ptnt_prblm.problem_start_date,  'dd-MMM-yyyy')
                 WHEN LENGTH(COALESCE(ptnt_prblm.problem_start_date,'')) =  9
                 THEN TO_DATE(ptnt_prblm.problem_start_date,  'dd-MMM-yy')
                 WHEN LENGTH(COALESCE(ptnt_prblm.problem_start_date,'')) =  4 
                 THEN ptnt_prblm.problem_start_date
                 ELSE NULL                                           
            END  AS STRING
        )                                                     AS problem_start_date,  
    ----------------------------------------------------------------------------------------------------------------------
    --   problem_end_date
    ----------------------------------------------------------------------------------------------------------------------
    CAST(
            CASE WHEN LENGTH(COALESCE(ptnt_prblm.problem_end_date,'')) =  11  
                 THEN TO_DATE(ptnt_prblm.problem_end_date,  'dd-MMM-yyyy')
                 WHEN LENGTH(COALESCE(ptnt_prblm.problem_end_date,'')) =  9
                 THEN TO_DATE(ptnt_prblm.problem_end_date,  'dd-MMM-yy')
                 WHEN LENGTH(COALESCE(ptnt_prblm.problem_end_date,'')) =  4 
                 THEN ptnt_prblm.problem_end_date
                 ELSE NULL                                           
            END  AS STRING
        )                                                     AS problem_end_date,                                         
   ----------------------------------------------------------------------------------------------------------------------
    ptnt_prblm.primary_secondary                                              AS primary_secondary,
    'patient_problem'                                                         AS prmy_src_tbl_nm,
    'ccf'                                                                     AS part_provider,
    DATE_FORMAT(current_date, 'yyyy-MM-dd')                                AS part_mth
FROM  patient_problem ptnt_prblm
LEFT OUTER JOIN master_patient mptnt
ON  LOWER(COALESCE(ptnt_prblm.deidentified_master_patient_id, 'NA')) = LOWER(COALESCE(mptnt.deidentified_master_patient_id, 'empty')) 
LEFT OUTER JOIN matching_payload pay 
ON LOWER(COALESCE(mptnt.deidentified_master_patient_id, 'NA')) = LOWER(COALESCE(pay.claimid, 'empty')) 
WHERE ptnt_prblm.problem_id <> 'PROBLEM_ID' 
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18,19,20,21,22,23,24,25, 26
--limit 10
