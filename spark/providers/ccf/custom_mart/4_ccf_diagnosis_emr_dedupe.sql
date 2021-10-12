SELECT
    CURRENT_DATE()                                                             AS crt_dt,
    ----------------------------------------------------------------------------------------------------------------------
    -- data_set_nm
    ----------------------------------------------------------------------------------------------------------------------
    SPLIT(diag_emr.input_file_name, '/')[SIZE(SPLIT(diag_emr.input_file_name, '/')) - 1] AS data_set_nm,    
    ----------------------------------------------------------------------------------------------------------------------
    pay.hvid 																   AS hvid,
    ----------------------------------------------------------------------------------------------------------------------
    -- ptnt_birth_yr
    ----------------------------------------------------------------------------------------------------------------------
	CAST(
    	CAP_YEAR_OF_BIRTH
    	    (
    	        CAST(COALESCE(pay.age,  mptnt.current_age) AS INT),
    	        CASE WHEN LENGTH(COALESCE(diag_emr.diagnosis_date,'')) =  11  
                     THEN TO_DATE(diag_emr.diagnosis_date,  'dd-MMM-yyyy')
                     WHEN LENGTH(COALESCE(diag_emr.diagnosis_date,'')) =  9
                     THEN TO_DATE(diag_emr.diagnosis_date,  'dd-MMM-yy')
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
    	        CASE WHEN LENGTH(COALESCE(diag_emr.diagnosis_date,'')) =  11  
                     THEN TO_DATE(diag_emr.diagnosis_date,  'dd-MMM-yyyy')
                     WHEN LENGTH(COALESCE(diag_emr.diagnosis_date,'')) =  9
                     THEN TO_DATE(diag_emr.diagnosis_date,  'dd-MMM-yy')
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
    diag_emr.deidentified_master_patient_id                                    AS deidentified_master_patient_id,
	diag_emr.deidentified_patient_id                                           AS deidentified_patient_id,
    diag_emr.data_source                                                       AS data_source,
    diag_emr.diag_id                                                           AS diag_id,
    diag_emr.visit_encounter_id                                                AS visit_encounter_id,
    diag_emr.diag_concept_name                                                 AS diag_concept_name,
    ----------------------------------------------------------------------------------------------------------------------
    --  diag_concept_code
    ----------------------------------------------------------------------------------------------------------------------
    CASE WHEN SUBSTR(UPPER(diag_emr.diag_system_name), 1,3) = 'ICD'  
         THEN
            CLEAN_UP_DIAGNOSIS_CODE
            (
                diag_emr.diag_concept_code,
                -- ICD9 or ICD10
                CASE WHEN SUBSTR(REPLACE(REPLACE(REPLACE (diag_emr.diag_system_name, ' ', ''), '-', ''), '*', ''), 4,1) = '9'
                     THEN '01'
                     ELSE '02'
                END,
                COALESCE(    CASE WHEN LENGTH(COALESCE(diag_emr.diagnosis_date,'')) =  11  
                                  THEN TO_DATE(diag_emr.diagnosis_date,  'dd-MMM-yyyy')
                                  WHEN LENGTH(COALESCE(diag_emr.diagnosis_date,'')) =  9
                                  THEN TO_DATE(diag_emr.diagnosis_date,  'dd-MMM-yy')
                                  ELSE NULL                                           
                             END,
                             CASE WHEN LENGTH(COALESCE(diag_emr.disease_onset_date,'')) =  11  
                                  THEN TO_DATE(diag_emr.disease_onset_date,  'dd-MMM-yyyy')
                                  WHEN LENGTH(COALESCE(diag_emr.disease_onset_date,'')) =  9
                                  THEN TO_DATE(diag_emr.disease_onset_date,  'dd-MMM-yy')
                                  ELSE NULL                                           
                             END
                        )
            )       ---If date is yyyy, it will be null and the UDF should still work fine                                 
        ELSE
            diag_emr.diag_concept_code
    END                                                                        AS diag_concept_code,
    ---------------------------------------------------------------------------------------------------------------------- 
    diag_emr.diag_system_name                                                  AS diag_system_name,
    ----------------------------------------------------------------------------------------------------------------------
    --  src_diag_concept_code
    ----------------------------------------------------------------------------------------------------------------------
    CASE WHEN SUBSTR(UPPER(diag_emr.src_diag_system_name), 1,3) = 'ICD'  
         THEN
            CLEAN_UP_DIAGNOSIS_CODE
            (
                diag_emr.src_diag_concept_code,
                -- ICD9 or ICD10
                CASE WHEN SUBSTR(REPLACE(REPLACE(REPLACE (diag_emr.src_diag_system_name, ' ', ''), '-', ''), '*', ''), 4,1) = '9'
                     THEN '01'
                     ELSE '02'
                END,
                COALESCE(    CASE WHEN LENGTH(COALESCE(diag_emr.diagnosis_date,'')) =  11  
                                  THEN TO_DATE(diag_emr.diagnosis_date,  'dd-MMM-yyyy')
                                  WHEN LENGTH(COALESCE(diag_emr.diagnosis_date,'')) =  9
                                  THEN TO_DATE(diag_emr.diagnosis_date,  'dd-MMM-yy')
                                  ELSE NULL                                           
                             END,
                             CASE WHEN LENGTH(COALESCE(diag_emr.disease_onset_date,'')) =  11  
                                  THEN TO_DATE(diag_emr.disease_onset_date,  'dd-MMM-yyyy')
                                  WHEN LENGTH(COALESCE(diag_emr.disease_onset_date,'')) =  9
                                  THEN TO_DATE(diag_emr.disease_onset_date,  'dd-MMM-yy')
                                  ELSE NULL                                           
                             END
                        )
            )       ---If date is yyyy, it will be null and the UDF should still work fine                                 
        ELSE
            diag_emr.src_diag_concept_code
    END                                                                        AS src_diag_concept_code,
    diag_emr.src_diag_system_name                                              AS src_diag_system_name,
    diag_emr.src_diag_concept_name                                             AS src_diag_concept_name,    
   
    diag_emr.diag_status_concept_code                                          AS diag_status_concept_code,
    diag_emr.diag_status_concept_name                                          AS diag_status_concept_name,
    diag_emr.diagnosis_domain                                                  AS diagnosis_domain,
    ----------------------------------------------------------------------------------------------------------------------
    --  diagnosis_date
    ----------------------------------------------------------------------------------------------------------------------
    CAST(
            CASE WHEN LENGTH(COALESCE(diag_emr.diagnosis_date,'')) =  11  
                 THEN TO_DATE(diag_emr.diagnosis_date,  'dd-MMM-yyyy')
                 WHEN LENGTH(COALESCE(diag_emr.diagnosis_date,'')) =  9
                 THEN TO_DATE(diag_emr.diagnosis_date,  'dd-MMM-yy')
                 WHEN LENGTH(COALESCE(diag_emr.diagnosis_date,'')) =  4 
                 THEN diag_emr.diagnosis_date 
                 ELSE NULL                                           
            END 
            AS STRING
        )                                                                        AS diagnosis_date,
    ----------------------------------------------------------------------------------------------------------------------
    --  disease_onset_date
    ----------------------------------------------------------------------------------------------------------------------
    CAST(
            CASE WHEN LENGTH(COALESCE(diag_emr.disease_onset_date,'')) =  11  
                 THEN TO_DATE(diag_emr.disease_onset_date,  'dd-MMM-yyyy')
                 WHEN LENGTH(COALESCE(diag_emr.disease_onset_date,'')) =  9
                 THEN TO_DATE(diag_emr.disease_onset_date,  'dd-MMM-yy')
                 WHEN LENGTH(COALESCE(diag_emr.disease_onset_date,'')) =  4 
                 THEN diag_emr.disease_onset_date 
                 ELSE NULL                                           
            END 
            AS STRING
        )                                                                       AS disease_onset_date,
    ----------------------------------------------------------------------------------------------------------------------
    diag_emr.primary_diagnosis_flag                                           AS primary_diagnosis_flag,
    diag_emr.place_of_service                                                 AS place_of_service,
    diag_emr.diag_typ_concept_code                                            AS diag_typ_concept_code,
    diag_emr.diag_typ_concept_name                                            AS diag_typ_concept_name,
    diag_emr.diag_cncpt_descr                                                 AS diag_cncpt_descr,
    'diagnosis_emr'                                                           AS prmy_src_tbl_nm,
    'ccf'                                                                     AS part_provider,
    DATE_FORMAT(current_date, 'yyyy-MM-dd')                                     AS part_mth
FROM  diagnosis_emr  diag_emr
LEFT OUTER JOIN master_patient mptnt
ON  LOWER(COALESCE(diag_emr.deidentified_master_patient_id, 'NA')) = LOWER(COALESCE(mptnt.deidentified_master_patient_id, 'empty')) 
LEFT OUTER JOIN matching_payload pay 
ON LOWER(COALESCE(mptnt.deidentified_master_patient_id, 'NA')) = LOWER(COALESCE(pay.claimid, 'empty')) 
WHERE diag_emr.diag_id <> 'DIAG_ID'  
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18,19,20,21,22,23,24,25,26,27,28,29,30,31  
--limit 10
