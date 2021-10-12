SELECT
    CURRENT_DATE()                                                             AS crt_dt,
    ----------------------------------------------------------------------------------------------------------------------
    -- data_set_nm
    ----------------------------------------------------------------------------------------------------------------------
    SPLIT(proc_emr.input_file_name, '/')[SIZE(SPLIT(proc_emr.input_file_name, '/')) - 1] AS data_set_nm,    
    ----------------------------------------------------------------------------------------------------------------------
    pay.hvid 																   AS hvid,
    ----------------------------------------------------------------------------------------------------------------------
    -- ptnt_birth_yr
    ----------------------------------------------------------------------------------------------------------------------
	CAST(
    	CAP_YEAR_OF_BIRTH
    	    (
    	        CAST(COALESCE(pay.age,  mptnt.current_age) AS INT),
                COALESCE(   CASE WHEN LENGTH(COALESCE(proc_emr.proc_start_date ,'')) =  11  
                                 THEN TO_DATE(proc_emr.proc_start_date ,  'dd-MMM-yyyy')
                                 WHEN LENGTH(COALESCE(proc_emr.proc_start_date ,'')) =  9
                                 THEN TO_DATE(proc_emr.proc_start_date ,  'dd-MMM-yy')
                                 ELSE NULL                                           
                            END,
                            CASE WHEN LENGTH(COALESCE(proc_emr.proc_end_date ,'')) =  11  
                                 THEN TO_DATE(proc_emr.proc_end_date ,  'dd-MMM-yyyy')
                                 WHEN LENGTH(COALESCE(proc_emr.proc_end_date ,'')) =  9
                                 THEN TO_DATE(proc_emr.proc_end_date ,  'dd-MMM-yy')
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
                COALESCE(   CASE WHEN LENGTH(COALESCE(proc_emr.proc_start_date ,'')) =  11  
                                 THEN TO_DATE(proc_emr.proc_start_date ,  'dd-MMM-yyyy')
                                 WHEN LENGTH(COALESCE(proc_emr.proc_start_date ,'')) =  9
                                 THEN TO_DATE(proc_emr.proc_start_date ,  'dd-MMM-yy')
                                 ELSE NULL                                           
                            END,
                            CASE WHEN LENGTH(COALESCE(proc_emr.proc_end_date ,'')) =  11  
                                 THEN TO_DATE(proc_emr.proc_end_date ,  'dd-MMM-yyyy')
                                 WHEN LENGTH(COALESCE(proc_emr.proc_end_date ,'')) =  9
                                 THEN TO_DATE(proc_emr.proc_end_date ,  'dd-MMM-yy')
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
	proc_emr.deidentified_master_patient_id                                 AS deidentified_master_patient_id,
	proc_emr.deidentified_patient_id                                        AS deidentified_patient_id,
    proc_emr.data_source                                                    AS data_source,
    proc_emr.proc_id                                                        AS proc_id,
    proc_emr.visit_encounter_id                                             AS visit_encounter_id,

    proc_emr.proc_concept_name                                              AS proc_concept_name,
    ----------------------------------------------------------------------------------------------------------------------
    --  proc_concept_code
    ----------------------------------------------------------------------------------------------------------------------
    CASE WHEN SUBSTR(UPPER(proc_emr.proc_system_name), 1,3) = 'ICD'  
         THEN
            CLEAN_UP_DIAGNOSIS_CODE
            (
                proc_emr.proc_concept_code,
                -- ICD9 or ICD10
                CASE WHEN SUBSTR(REPLACE(REPLACE(REPLACE (proc_emr.proc_system_name, ' ', ''), '-', ''), '*', ''), 4,1) = '9'
                     THEN '01'
                     ELSE '02'
                END,
                COALESCE(   CASE WHEN LENGTH(COALESCE(proc_emr.proc_start_date ,'')) =  11  
                                 THEN TO_DATE(proc_emr.proc_start_date ,  'dd-MMM-yyyy')
                                 WHEN LENGTH(COALESCE(proc_emr.proc_start_date ,'')) =  9
                                 THEN TO_DATE(proc_emr.proc_start_date ,  'dd-MMM-yy')
                                 ELSE NULL                                           
                             END,
                             CASE WHEN LENGTH(COALESCE(proc_emr.proc_end_date ,'')) =  11  
                                  THEN TO_DATE(proc_emr.proc_end_date ,  'dd-MMM-yyyy')
                                  WHEN LENGTH(COALESCE(proc_emr.proc_end_date ,'')) =  9
                                  THEN TO_DATE(proc_emr.proc_end_date ,  'dd-MMM-yy')
                                  ELSE NULL                                           
                             END
                        )
            )       ---If date is yyyy, it will be null and the UDF should still work fine                                 
        ELSE
             proc_emr.proc_concept_code
    END                                                                     AS proc_concept_code,
    ----------------------------------------------------------------------------------------------------------------------
    proc_emr.proc_system_name                                               AS proc_system_name,
    ----------------------------------------------------------------------------------------------------------------------
    --  src_proc_concept_code
    ----------------------------------------------------------------------------------------------------------------------
    CASE WHEN SUBSTR(UPPER(proc_emr.src_proc_system_name), 1,3) = 'ICD'  
         THEN
            CLEAN_UP_DIAGNOSIS_CODE
            (
                proc_emr.src_proc_concept_code,
                -- ICD9 or ICD10
                CASE WHEN SUBSTR(REPLACE(REPLACE(REPLACE (proc_emr.src_proc_system_name, ' ', ''), '-', ''), '*', ''), 4,1) = '9'
                     THEN '01'
                     ELSE '02'
                END,
                COALESCE(   CASE WHEN LENGTH(COALESCE(proc_emr.proc_start_date ,'')) =  11  
                                 THEN TO_DATE(proc_emr.proc_start_date ,  'dd-MMM-yyyy')
                                 WHEN LENGTH(COALESCE(proc_emr.proc_start_date ,'')) =  9
                                 THEN TO_DATE(proc_emr.proc_start_date ,  'dd-MMM-yy')
                                 ELSE NULL                                           
                             END,
                             CASE WHEN LENGTH(COALESCE(proc_emr.proc_end_date ,'')) =  11  
                                  THEN TO_DATE(proc_emr.proc_end_date ,  'dd-MMM-yyyy')
                                  WHEN LENGTH(COALESCE(proc_emr.proc_end_date ,'')) =  9
                                  THEN TO_DATE(proc_emr.proc_end_date ,  'dd-MMM-yy')
                                  ELSE NULL                                           
                             END
                        )
            )       ---If date is yyyy, it will be null and the UDF should still work fine                                 
        ELSE
             proc_emr.src_proc_concept_code
    END                                                                     AS src_proc_concept_code,
    ----------------------------------------------------------------------------------------------------------------------
    proc_emr.src_proc_system_name                                           AS src_proc_system_name,
    proc_emr.src_proc_concept_name                                          AS src_proc_concept_name,
    ----------------------------------------------------------------------------------------------------------------------
    --   proc_start_date
    ----------------------------------------------------------------------------------------------------------------------
    CAST(
            CASE WHEN LENGTH(COALESCE(proc_emr.proc_start_date ,'')) =  11  
                 THEN TO_DATE(proc_emr.proc_start_date ,  'dd-MMM-yyyy')
                 WHEN LENGTH(COALESCE(proc_emr.proc_start_date ,'')) =  9
                 THEN TO_DATE(proc_emr.proc_start_date ,  'dd-MMM-yy')
                 WHEN LENGTH(COALESCE(proc_emr.proc_start_date ,'')) =  4 
                 THEN proc_emr.proc_start_date 
                 ELSE NULL                                           
            END  AS STRING
        )                                                                   AS proc_start_date,
    ----------------------------------------------------------------------------------------------------------------------
    --   proc_end_date
    ----------------------------------------------------------------------------------------------------------------------
    CAST(
            CASE WHEN LENGTH(COALESCE(proc_emr.proc_end_date ,'')) =  11  
                 THEN TO_DATE(proc_emr.proc_end_date ,  'dd-MMM-yyyy')
                 WHEN LENGTH(COALESCE(proc_emr.proc_end_date ,'')) =  9
                 THEN TO_DATE(proc_emr.proc_end_date ,  'dd-MMM-yy')
                 WHEN LENGTH(COALESCE(proc_emr.proc_end_date ,'')) =  4 
                 THEN proc_emr.proc_end_date 
                 ELSE NULL                                           
            END  AS STRING
        )                                                                   AS proc_end_date,
    ----------------------------------------------------------------------------------------------------------------------
    proc_emr.procedure_domain                                               AS procedure_domain,
    proc_emr.place_of_service                                               AS place_of_service,
    'procedures_emr'                                                            AS prmy_src_tbl_nm,
    'ccf'                                                                       AS part_provider,
    DATE_FORMAT(current_date, 'yyyy-MM-dd')                                     AS part_mth
FROM  procedures_emr proc_emr
LEFT OUTER JOIN master_patient mptnt
ON  LOWER(COALESCE(proc_emr.deidentified_master_patient_id, 'NA')) = LOWER(COALESCE(mptnt.deidentified_master_patient_id, 'empty')) 
LEFT OUTER JOIN matching_payload pay 
ON LOWER(COALESCE(mptnt.deidentified_master_patient_id, 'NA')) = LOWER(COALESCE(pay.claimid, 'empty')) 
WHERE proc_emr.proc_id <> 'PROC_ID'
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18,19,20,21,22,23,24,25
--limit 10
