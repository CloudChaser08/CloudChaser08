SELECT
    CURRENT_DATE()                                                             AS crt_dt,
    ----------------------------------------------------------------------------------------------------------------------
    -- data_set_nm
    ----------------------------------------------------------------------------------------------------------------------
    SPLIT(ptnt_hx.input_file_name, '/')[SIZE(SPLIT(ptnt_hx.input_file_name, '/')) - 1] AS data_set_nm,    
    ----------------------------------------------------------------------------------------------------------------------
    pay.hvid 																   AS hvid,
    ----------------------------------------------------------------------------------------------------------------------
    -- ptnt_birth_yr
    ----------------------------------------------------------------------------------------------------------------------
	CAST(
    	CAP_YEAR_OF_BIRTH
    	    (
    	        CAST(COALESCE(pay.age,  mptnt.current_age) AS INT),
    	        CASE WHEN LENGTH(COALESCE(ptnt_hx.event_onset_date,'')) =  11  
                     THEN TO_DATE(ptnt_hx.event_onset_date,  'dd-MMM-yyyy')
                     WHEN LENGTH(COALESCE(ptnt_hx.event_onset_date,'')) =  9
                     THEN TO_DATE(ptnt_hx.event_onset_date,  'dd-MMM-yy')
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
    	        CASE WHEN LENGTH(COALESCE(ptnt_hx.event_onset_date,'')) =  11  
                     THEN TO_DATE(ptnt_hx.event_onset_date,  'dd-MMM-yyyy')
                     WHEN LENGTH(COALESCE(ptnt_hx.event_onset_date,'')) =  9
                     THEN TO_DATE(ptnt_hx.event_onset_date,  'dd-MMM-yy')
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
	ptnt_hx.deidentified_master_patient_id                                 AS deidentified_master_patient_id,
	ptnt_hx.deidentified_patient_id                                        AS deidentified_patient_id,
    ptnt_hx.data_source                                                    AS data_source,
    ptnt_hx.hist_id                                                        AS hist_id,
    ptnt_hx.history_type                                                   AS history_type,
    ptnt_hx.relation                                                       AS relation,
    ptnt_hx.history_concept_name                                           AS history_concept_name,
    ----------------------------------------------------------------------------------------------------------------------
    -- history_concept_code
    ----------------------------------------------------------------------------------------------------------------------
    CASE WHEN SUBSTR(UPPER(ptnt_hx.history_system_name), 1,3) = 'ICD'  
         THEN
            CLEAN_UP_DIAGNOSIS_CODE
            (
                ptnt_hx.history_concept_code,
                -- ICD9 or ICD10
                CASE WHEN SUBSTR(REPLACE(REPLACE(REPLACE (ptnt_hx.history_system_name, ' ', ''), '-', ''), '*', ''), 4,1) = '9'
                     THEN '01'
                     ELSE '02'
                END,
                CASE WHEN LENGTH(COALESCE(ptnt_hx.event_onset_date,'')) =  11  
                     THEN TO_DATE(ptnt_hx.event_onset_date,  'dd-MMM-yyyy')
                     WHEN LENGTH(COALESCE(ptnt_hx.event_onset_date,'')) =  9
                     THEN TO_DATE(ptnt_hx.event_onset_date,  'dd-MMM-yy')
                     ELSE NULL                                           
                END 
            )       ---If date is yyyy, it will be null and the UDF should still work fine                                 
        ELSE
            ptnt_hx.history_concept_code 
    END                                                                     AS history_concept_code,
   ----------------------------------------------------------------------------------------------------------------------
    ptnt_hx.history_system_name                                            AS history_system_name,
    ----------------------------------------------------------------------------------------------------------------------
    -- src_history_concept_code
    ----------------------------------------------------------------------------------------------------------------------
    CASE WHEN SUBSTR(UPPER(ptnt_hx.src_history_system_name), 1,3) = 'ICD'  
         THEN
            CLEAN_UP_DIAGNOSIS_CODE
            (
                ptnt_hx.src_history_concept_code,
                -- ICD9 or ICD10
                CASE WHEN SUBSTR(REPLACE(REPLACE(REPLACE (ptnt_hx.src_history_system_name, ' ', ''), '-', ''), '*', ''), 4,1) = '9'
                     THEN '01'
                     ELSE '02'
                END,
                CASE WHEN LENGTH(COALESCE(ptnt_hx.event_onset_date,'')) =  11  
                     THEN TO_DATE(ptnt_hx.event_onset_date,  'dd-MMM-yyyy')
                     WHEN LENGTH(COALESCE(ptnt_hx.event_onset_date,'')) =  9
                     THEN TO_DATE(ptnt_hx.event_onset_date,  'dd-MMM-yy')
                     ELSE NULL                                           
                END 
            )       ---If date is yyyy, it will be null and the UDF should still work fine                                 
        ELSE
            ptnt_hx.src_history_concept_code 
    END                                                                     AS src_history_concept_code,
   ----------------------------------------------------------------------------------------------------------------------
    ptnt_hx.src_history_system_name                                        AS src_history_system_name,
    ptnt_hx.src_history_concept_name                                       AS src_history_concept_name,
    ----------------------------------------------------------------------------------------------------------------------
    --   event_onset_date
    ----------------------------------------------------------------------------------------------------------------------
    CAST(
            CASE WHEN LENGTH(COALESCE(ptnt_hx.event_onset_date,'')) =  11  
                 THEN TO_DATE(ptnt_hx.event_onset_date,  'dd-MMM-yyyy')
                 WHEN LENGTH(COALESCE(ptnt_hx.event_onset_date,'')) =  9
                 THEN TO_DATE(ptnt_hx.event_onset_date,  'dd-MMM-yy')
                 WHEN LENGTH(COALESCE(ptnt_hx.event_onset_date,'')) =  4 
                 THEN ptnt_hx.event_onset_date 
                 ELSE NULL                                           
            END  AS STRING
        )                                                   AS event_onset_date,
   ----------------------------------------------------------------------------------------------------------------------
    'patient_history'                                                          AS prmy_src_tbl_nm,
    'ccf'                                                                     AS part_provider,
    DATE_FORMAT(current_date, 'yyyy-MM-dd')                                AS part_mth
FROM  patient_history ptnt_hx
LEFT OUTER JOIN master_patient mptnt
ON  LOWER(COALESCE(ptnt_hx.deidentified_master_patient_id, 'NA')) = LOWER(COALESCE(mptnt.deidentified_master_patient_id, 'empty')) 
LEFT OUTER JOIN matching_payload pay 
ON LOWER(COALESCE(mptnt.deidentified_master_patient_id, 'NA')) = LOWER(COALESCE(pay.claimid, 'empty')) 
WHERE ptnt_hx.hist_id <> 'HIST_ID' 
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18,19,20,21,22,23
--limit 10
