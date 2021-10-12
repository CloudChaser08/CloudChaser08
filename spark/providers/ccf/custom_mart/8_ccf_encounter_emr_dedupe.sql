SELECT
    CURRENT_DATE()                                                             AS crt_dt,
    ----------------------------------------------------------------------------------------------------------------------
    -- data_set_nm
    ----------------------------------------------------------------------------------------------------------------------
    SPLIT(enc_emr.input_file_name, '/')[SIZE(SPLIT(enc_emr.input_file_name, '/')) - 1] AS data_set_nm,    
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
    	                     CASE WHEN LENGTH(COALESCE(enc_emr.visit_encounter_start_date,'')) =  11  
                                 THEN TO_DATE(enc_emr.visit_encounter_start_date,  'dd-MMM-yyyy')
                                 WHEN LENGTH(COALESCE(enc_emr.visit_encounter_start_date,'')) =  9
                                 THEN TO_DATE(enc_emr.visit_encounter_start_date,  'dd-MMM-yy')
                                 ELSE NULL                                           
                            END,
                            CASE WHEN LENGTH(COALESCE(enc_emr.visit_encounter_end_date,'')) =  11  
                                 THEN TO_DATE(enc_emr.visit_encounter_end_date,  'dd-MMM-yyyy')
                                 WHEN LENGTH(COALESCE(enc_emr.visit_encounter_end_date,'')) =  9
                                 THEN TO_DATE(enc_emr.visit_encounter_end_date,  'dd-MMM-yy')
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
    	                    CASE WHEN LENGTH(COALESCE(enc_emr.visit_encounter_start_date,'')) = 11  
                                 THEN TO_DATE(enc_emr.visit_encounter_start_date,  'dd-MMM-yyyy')
                                 WHEN LENGTH(COALESCE(enc_emr.visit_encounter_start_date,'')) = 9
                                 THEN TO_DATE(enc_emr.visit_encounter_start_date,  'dd-MMM-yy')
                                 ELSE NULL                                           
                            END,
                            CASE WHEN LENGTH(COALESCE(enc_emr.visit_encounter_end_date,'')) = 11  
                                 THEN TO_DATE(enc_emr.visit_encounter_end_date,  'dd-MMM-yyyy')
                                 WHEN LENGTH(COALESCE(enc_emr.visit_encounter_end_date,'')) = 9
                                 THEN TO_DATE(enc_emr.visit_encounter_end_date,  'dd-MMM-yy')
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
	enc_emr.deidentified_master_patient_id                                 AS deidentified_master_patient_id,
	enc_emr.deidentified_patient_id                                        AS deidentified_patient_id,
    enc_emr.data_source                                                    AS data_source,
    enc_emr.visitenc_id                                                    AS visitenc_id,
    enc_emr.type_of_encounter                                              AS type_of_encounter,
    enc_emr.lgth_of_stay_during_an_enc                                     AS lgth_of_stay_during_an_enc,
    enc_emr.uom_for_length_of_stay                                         AS uom_for_length_of_sta,
    ----------------------------------------------------------------------------------------------------------------------
    --  visit_encounter_start_date
    ----------------------------------------------------------------------------------------------------------------------
    CAST(
            CASE WHEN LENGTH(COALESCE(enc_emr.visit_encounter_start_date,'')) = 11  
                 THEN TO_DATE(enc_emr.visit_encounter_start_date,  'dd-MMM-yyyy')
                 WHEN LENGTH(COALESCE(enc_emr.visit_encounter_start_date,'')) = 9
                 THEN TO_DATE(enc_emr.visit_encounter_start_date,  'dd-MMM-yy')
                 WHEN LENGTH(COALESCE(enc_emr.visit_encounter_start_date,'')) = 4 
                 THEN enc_emr.visit_encounter_start_date 
                 ELSE NULL                                           
            END 
            AS STRING
        )                                                                  AS visit_encounter_start_date,
    ----------------------------------------------------------------------------------------------------------------------
    -- visit_encounter_end_date
    ----------------------------------------------------------------------------------------------------------------------
    CAST(
            CASE WHEN LENGTH(COALESCE(enc_emr.visit_encounter_end_date,'')) = 11  
                 THEN TO_DATE(enc_emr.visit_encounter_end_date,  'dd-MMM-yyyy')
                 WHEN LENGTH(COALESCE(enc_emr.visit_encounter_end_date,'')) = 9
                 THEN TO_DATE(enc_emr.visit_encounter_end_date,  'dd-MMM-yy')
                 WHEN LENGTH(COALESCE(enc_emr.visit_encounter_end_date,'')) = 4 
                 THEN enc_emr.visit_encounter_end_date 
                 ELSE NULL                                           
            END 
            AS STRING
        )                                                                   AS visit_encounter_end_date,
        
    'encounter_emr'                                                           AS prmy_src_tbl_nm,
    'ccf'                                                                     AS part_provider,
    DATE_FORMAT(current_date, 'yyyy-MM-dd')                                     AS part_mth
FROM  encounter_emr enc_emr
LEFT OUTER JOIN master_patient mptnt
ON  LOWER(COALESCE(enc_emr.deidentified_master_patient_id, 'NA')) = LOWER(COALESCE(mptnt.deidentified_master_patient_id, 'empty')) 
LEFT OUTER JOIN matching_payload pay 
ON LOWER(COALESCE(mptnt.deidentified_master_patient_id, 'NA')) = LOWER(COALESCE(pay.claimid, 'empty')) 
WHERE enc_emr.visitenc_id <> 'VISITENC_ID'
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18,19
--limit 10
