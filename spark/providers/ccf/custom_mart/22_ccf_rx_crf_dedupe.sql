SELECT
    CURRENT_DATE()                                                             AS crt_dt,
    ----------------------------------------------------------------------------------------------------------------------
    -- data_set_nm
    ----------------------------------------------------------------------------------------------------------------------
    SPLIT(rx_crf.input_file_name, '/')[SIZE(SPLIT(rx_crf.input_file_name, '/')) - 1] AS data_set_nm,    
    ----------------------------------------------------------------------------------------------------------------------
    pay.hvid 																   AS hvid,
    ----------------------------------------------------------------------------------------------------------------------
    -- ptnt_birth_yr
    ----------------------------------------------------------------------------------------------------------------------
	CAST(
    	CAP_YEAR_OF_BIRTH
    	    (
    	        CAST(COALESCE(pay.age,  mptnt.current_age) AS INT),
                COALESCE(   CASE WHEN LENGTH(COALESCE(rx_crf.med_start_date ,'')) =  11  
                                 THEN TO_DATE(rx_crf.med_start_date ,  'dd-MMM-yyyy')
                                 WHEN LENGTH(COALESCE(rx_crf.med_start_date ,'')) =  9
                                 THEN TO_DATE(rx_crf.med_start_date ,  'dd-MMM-yy')
                                 ELSE NULL                                           
                            END,
                            CASE WHEN LENGTH(COALESCE(rx_crf.med_end_date ,'')) =  11  
                                 THEN TO_DATE(rx_crf.med_end_date ,  'dd-MMM-yyyy')
                                 WHEN LENGTH(COALESCE(rx_crf.med_end_date ,'')) =  9
                                 THEN TO_DATE(rx_crf.med_end_date ,  'dd-MMM-yy')
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
                COALESCE(   CASE WHEN LENGTH(COALESCE(rx_crf.med_start_date ,'')) =  11  
                                 THEN TO_DATE(rx_crf.med_start_date ,  'dd-MMM-yyyy')
                                 WHEN LENGTH(COALESCE(rx_crf.med_start_date ,'')) =  9
                                 THEN TO_DATE(rx_crf.med_start_date ,  'dd-MMM-yy')
                                 ELSE NULL                                           
                            END,
                            CASE WHEN LENGTH(COALESCE(rx_crf.med_end_date ,'')) =  11  
                                 THEN TO_DATE(rx_crf.med_end_date ,  'dd-MMM-yyyy')
                                 WHEN LENGTH(COALESCE(rx_crf.med_end_date ,'')) =  9
                                 THEN TO_DATE(rx_crf.med_end_date ,  'dd-MMM-yy')
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
	rx_crf.deidentified_master_patient_id                                 AS deidentified_master_patient_id,
	rx_crf.deidentified_patient_id                                        AS deidentified_patient_id,
    rx_crf.data_source                                                    AS data_source,
    
    rx_crf.med_id                                                         AS med_id, 
    rx_crf.visit_encounter_id                                             AS visit_encounter_id,
    rx_crf.medication_name                                                           AS medication_name,
    rx_crf.drug_code                                                                 AS drug_code,
    rx_crf.drug_code_sys_nm                                                          AS drug_code_sys_nm,
    rx_crf.src_drug_code                                                             AS src_drug_code,
    rx_crf.src_drug_code_sys_nm                                                      AS src_drug_code_sys_nm,
    rx_crf.src_drug_code_concept_name                                                AS src_drug_code_concept_name,
    rx_crf.med_action_concept_name                                                   AS med_action_concept_name,
    rx_crf.route_of_medication                                                       AS route_of_medication,
    rx_crf.medication_domain                                                         AS medication_domain,
    ----------------------------------------------------------------------------------------------------------------------
    --   med_start_date
    ----------------------------------------------------------------------------------------------------------------------
    CAST(
            CASE WHEN LENGTH(COALESCE(rx_crf.med_start_date ,'')) =  11  
                 THEN TO_DATE(rx_crf.med_start_date ,  'dd-MMM-yyyy')
                 WHEN LENGTH(COALESCE(rx_crf.med_start_date ,'')) =  9
                 THEN TO_DATE(rx_crf.med_start_date ,  'dd-MMM-yy')
                 WHEN LENGTH(COALESCE(rx_crf.med_start_date ,'')) =  4 
                 THEN rx_crf.med_start_date 
                 ELSE NULL                                           
            END  AS STRING
        )                                                                           AS med_start_date,
    ----------------------------------------------------------------------------------------------------------------------
    --   med_end_date
    ----------------------------------------------------------------------------------------------------------------------
    CAST(
            CASE WHEN LENGTH(COALESCE(rx_crf.med_end_date ,'')) =  11  
                 THEN TO_DATE(rx_crf.med_end_date ,  'dd-MMM-yyyy')
                 WHEN LENGTH(COALESCE(rx_crf.med_end_date ,'')) =  9
                 THEN TO_DATE(rx_crf.med_end_date ,  'dd-MMM-yy')
                 WHEN LENGTH(COALESCE(rx_crf.med_end_date ,'')) =  4 
                 THEN rx_crf.med_end_date 
                 ELSE NULL                                           
            END  AS STRING
        )                                                                           AS med_end_date,
    ----------------------------------------------------------------------------------------------------------------------
    rx_crf.dose_of_medication                                                        AS dose_of_medication,
    rx_crf.current_medication                                                        AS current_medication,
    rx_crf.other_medication                                                          AS other_medication,
    rx_crf.unit_of_measure_for_medication                                            AS unit_of_measure_for_medication,
    rx_crf.medication_frequence                                                      AS medication_frequence,
    rx_crf.medication_administrated_code                                             AS medication_administrated_code,
    rx_crf.medication_administrated                                                  AS medication_administrated,
    rx_crf.frequency_in_days                                                         AS frequency_in_days,
    rx_crf.reason_stopped                                                            AS reason_stopped,
    rx_crf.summary                                                                   AS summary,
    'prescriptions_crf'                                                         AS prmy_src_tbl_nm,
    'ccf'                                                                       AS part_provider,
    DATE_FORMAT(current_date, 'yyyy-MM-dd')                                     AS part_mth
FROM  prescriptions_crf rx_crf
LEFT OUTER JOIN master_patient mptnt
ON  LOWER(COALESCE(rx_crf.deidentified_master_patient_id, 'NA')) = LOWER(COALESCE(mptnt.deidentified_master_patient_id, 'empty')) 
LEFT OUTER JOIN matching_payload pay 
ON LOWER(COALESCE(mptnt.deidentified_master_patient_id, 'NA')) = LOWER(COALESCE(pay.claimid, 'empty')) 
WHERE rx_crf.med_id <> 'MED_ID' 
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36
--limit 10
