SELECT
    CURRENT_DATE()                                                             AS crt_dt,
    ----------------------------------------------------------------------------------------------------------------------
    -- data_set_nm
    ----------------------------------------------------------------------------------------------------------------------
    SPLIT(lab_emr.input_file_name, '/')[SIZE(SPLIT(lab_emr.input_file_name, '/')) - 1] AS data_set_nm,    
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
    	                    CASE WHEN LENGTH(COALESCE(lab_emr.lab_test_requested_date,'')) =  11  
                                 THEN TO_DATE(lab_emr.lab_test_requested_date,  'dd-MMM-yyyy')
                                 WHEN LENGTH(COALESCE(lab_emr.lab_test_requested_date,'')) =  9
                                 THEN TO_DATE(lab_emr.lab_test_requested_date,  'dd-MMM-yy')
                                 ELSE NULL                                           
                            END,
    	                    CASE WHEN LENGTH(COALESCE(lab_emr.lab_test_result_date,'')) =  11  
                                 THEN TO_DATE(lab_emr.lab_test_result_date,  'dd-MMM-yyyy')
                                 WHEN LENGTH(COALESCE(lab_emr.lab_test_result_date,'')) =  9
                                 THEN TO_DATE(lab_emr.lab_test_result_date,  'dd-MMM-yy')
                                 ELSE NULL                                           
                            END,
    	                    CASE WHEN LENGTH(COALESCE(lab_emr.specimen_collection_date,'')) =  11  
                                 THEN TO_DATE(lab_emr.specimen_collection_date,  'dd-MMM-yyyy')
                                 WHEN LENGTH(COALESCE(lab_emr.specimen_collection_date,'')) =  9
                                 THEN TO_DATE(lab_emr.specimen_collection_date,  'dd-MMM-yy')
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
    	                    CASE WHEN LENGTH(COALESCE(lab_emr.lab_test_requested_date,'')) =  11  
                                 THEN TO_DATE(lab_emr.lab_test_requested_date,  'dd-MMM-yyyy')
                                 WHEN LENGTH(COALESCE(lab_emr.lab_test_requested_date,'')) =  9
                                 THEN TO_DATE(lab_emr.lab_test_requested_date,  'dd-MMM-yy')
                                 ELSE NULL                                           
                            END,
    	                    CASE WHEN LENGTH(COALESCE(lab_emr.lab_test_result_date,'')) =  11  
                                 THEN TO_DATE(lab_emr.lab_test_result_date,  'dd-MMM-yyyy')
                                 WHEN LENGTH(COALESCE(lab_emr.lab_test_result_date,'')) =  9
                                 THEN TO_DATE(lab_emr.lab_test_result_date,  'dd-MMM-yy')
                                 ELSE NULL                                           
                            END,
    	                    CASE WHEN LENGTH(COALESCE(lab_emr.specimen_collection_date,'')) =  11  
                                 THEN TO_DATE(lab_emr.specimen_collection_date,  'dd-MMM-yyyy')
                                 WHEN LENGTH(COALESCE(lab_emr.specimen_collection_date,'')) =  9
                                 THEN TO_DATE(lab_emr.specimen_collection_date,  'dd-MMM-yy')
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
	lab_emr.deidentified_master_patient_id                                 AS deidentified_master_patient_id,
	lab_emr.deidentified_patient_id                                        AS deidentified_patient_id,
    lab_emr.data_source                                                    AS data_source,
    lab_emr.lab_id                                                         AS lab_id,
    lab_emr.visit_encounter_id                                             AS visit_encounter_id,
    
    lab_emr.lab_test_concept_name                                          AS lab_test_concept_name,
    lab_emr.lab_test_concept_code                                          AS lab_test_concept_code,
    lab_emr.lab_test_system_name                                           AS lab_test_system_name,
    
    lab_emr.src_lab_test_concept_code                                          AS src_lab_test_concept_code,
    lab_emr.src_lab_test_system_name                                           AS src_lab_test_system_name,
    lab_emr.src_lab_test_concept_name                                          AS src_lab_test_concept_name,
    
    lab_emr.lab_test_sts_concept_code                                          AS lab_test_sts_concept_code,
    lab_emr.lab_test_sts_concept_name                                          AS lab_test_sts_concept_name,
    
    lab_emr.lab_results                                          AS lab_results,
    lab_emr.test_unit                                            AS test_unit,
    lab_emr.test_result_numeric                                  AS test_result_numeric,
    ----------------------------------------------------------------------------------------------------------------------
    -- specimen_collection_date
    ----------------------------------------------------------------------------------------------------------------------
    CAST(
            CASE WHEN LENGTH(COALESCE(lab_emr.specimen_collection_date,'')) = 11  
                 THEN TO_DATE(lab_emr.specimen_collection_date,  'dd-MMM-yyyy')
                 WHEN LENGTH(COALESCE(lab_emr.specimen_collection_date,'')) = 9
                 THEN TO_DATE(lab_emr.specimen_collection_date,  'dd-MMM-yy')
                 WHEN LENGTH(COALESCE(lab_emr.specimen_collection_date,'')) = 4 
                 THEN lab_emr.specimen_collection_date 
                 ELSE NULL                                           
            END 
            AS STRING
        )                                                                AS specimen_collection_date,
    ----------------------------------------------------------------------------------------------------------------------
    -- lab_test_requested_date
    ----------------------------------------------------------------------------------------------------------------------
    CAST(
            CASE WHEN LENGTH(COALESCE(lab_emr.lab_test_requested_date,'')) = 11  
                 THEN TO_DATE(lab_emr.lab_test_requested_date,  'dd-MMM-yyyy')
                 WHEN LENGTH(COALESCE(lab_emr.lab_test_requested_date,'')) = 9
                 THEN TO_DATE(lab_emr.lab_test_requested_date,  'dd-MMM-yy')
                 WHEN LENGTH(COALESCE(lab_emr.lab_test_requested_date,'')) = 4 
                 THEN lab_emr.lab_test_requested_date 
                 ELSE NULL                                           
            END 
            AS STRING
        )                                                             AS lab_test_requested_date,
    ----------------------------------------------------------------------------------------------------------------------
    -- lab_test_result_date
    ----------------------------------------------------------------------------------------------------------------------
    CAST(
            CASE WHEN LENGTH(COALESCE(lab_emr.lab_test_result_date,'')) = 11  
                 THEN TO_DATE(lab_emr.lab_test_result_date,  'dd-MMM-yyyy')
                 WHEN LENGTH(COALESCE(lab_emr.lab_test_result_date,'')) = 9
                 THEN TO_DATE(lab_emr.lab_test_result_date,  'dd-MMM-yy')
                 WHEN LENGTH(COALESCE(lab_emr.lab_test_result_date,'')) = 4 
                 THEN lab_emr.lab_test_result_date 
                 ELSE NULL                                           
            END 
            AS STRING
        )                                                          AS lab_test_result_date,
    lab_emr.abnormal_flag                                          AS abnormal_flag,
    lab_emr.lab_test_re_concept_code                         AS lab_test_re_concept_code,
    lab_emr.lab_test_re_concept_name                         AS lab_test_re_concept_name,
    lab_emr.lab_test_me_concept_code                         AS lab_test_me_concept_code,
    lab_emr.lab_test_me_concept_name                         AS lab_test_me_concept_name,
    'lab_emr'                                                           AS prmy_src_tbl_nm,
    'ccf'                                                                     AS part_provider,
    DATE_FORMAT(current_date, 'yyyy-MM-dd')                                     AS part_mth
FROM  lab_test_emr lab_emr
LEFT OUTER JOIN master_patient mptnt
ON  LOWER(COALESCE(lab_emr.deidentified_master_patient_id, 'NA')) = LOWER(COALESCE(mptnt.deidentified_master_patient_id, 'empty')) 
LEFT OUTER JOIN matching_payload pay 
ON LOWER(COALESCE(mptnt.deidentified_master_patient_id, 'NA')) = LOWER(COALESCE(pay.claimid, 'empty')) 
WHERE lab_emr.lab_id <> 'LAB_ID'
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34
--limit 10
