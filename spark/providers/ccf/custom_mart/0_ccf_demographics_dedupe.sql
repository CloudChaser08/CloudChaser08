SELECT
    CURRENT_DATE()                                                                  AS crt_dt,
    SPLIT(demo.input_file_name, '/')[SIZE(SPLIT(demo.input_file_name, '/')) - 1]    AS data_set_nm,    
    pay.hvid 																        AS hvid,
    ----------------------------------------------------------------------------------------------------------------------
    -- patientyob
    ----------------------------------------------------------------------------------------------------------------------
	CAST(
    	CAP_YEAR_OF_BIRTH
    	    (
    	        CAST(COALESCE(pay.age,  mptnt.current_age) AS INT),
    	        CASE WHEN LENGTH(COALESCE(demo.date_of_consent,'')) =  11  
                    THEN TO_DATE(demo.date_of_consent,  'dd-MMM-yyyy')
                    WHEN LENGTH(COALESCE(demo.date_of_consent,'')) =  9
                    THEN TO_DATE(demo.date_of_consent,  'dd-MMM-yy')
                    ELSE NULL
                END,
    	        CAST(COALESCE(pay.yearofbirth, mptnt.birth_year) AS INT)
    	    )																					
	    AS INT)                                                            AS ptnt_birth_yr,
    ----------------------------------------------------------------------------------------------------------------------
    -- patientage
    ----------------------------------------------------------------------------------------------------------------------

    CAP_AGE(
        VALIDATE_AGE
            (
                CAST(COALESCE(pay.age,  mptnt.current_age) AS INT),
                CASE WHEN LENGTH(COALESCE(demo.date_of_consent,'')) =  11  
                    THEN TO_DATE(demo.date_of_consent,  'dd-MMM-yyyy')
                    WHEN LENGTH(COALESCE(demo.date_of_consent,'')) =  9
                    THEN TO_DATE(demo.date_of_consent,  'dd-MMM-yy')
                    ELSE NULL
                END,
                CAST(COALESCE(pay.yearofbirth, mptnt.birth_year) AS INT)
            )
          )                                                                 AS ptnt_age_num,
    ----------------------------------------------------------------------------------------------------------------------
    -- patientgender
    ----------------------------------------------------------------------------------------------------------------------
   
    CASE
    	WHEN demo.gender IS NULL AND pay.gender IS NULL AND mptnt.gender IS NULL THEN NULL 
    	WHEN SUBSTR(UPPER(demo.gender), 1, 1) IN ('F', 'M', 'U')                 THEN SUBSTR(UPPER(demo.gender), 1, 1)
    	WHEN SUBSTR(UPPER(pay.gender ), 1, 1) IN ('F', 'M', 'U')                 THEN SUBSTR(UPPER(pay.gender ), 1, 1)
    	WHEN SUBSTR(UPPER(mptnt.gender ), 1, 1) IN ('F', 'M', 'U')               THEN SUBSTR(UPPER(mptnt.gender ), 1, 1)
        ELSE 'U' 
    END                                                                    AS ptnt_gender_cd,
    ----------------------------------------------------------------------------------------------------------------------
    -- patientstate
    ----------------------------------------------------------------------------------------------------------------------
    VALIDATE_STATE_CODE(UPPER(pay.state))	                               AS ptnt_state_cd,
    ----------------------------------------------------------------------------------------------------------------------
    -- patientzip
    ----------------------------------------------------------------------------------------------------------------------
	MASK_ZIP_CODE(pay.threedigitzip)                                       AS ptnt_zip3_cd,
    ----------------------------------------------------------------------------------------------------------------------
    demo.deidentified_master_patient_id                                    AS deidentified_master_patient_id,
	demo.deidentified_patient_id                                           AS deidentified_patient_id,
    demo.data_source                                                       AS data_source,
    demo.ethnicity                                                         AS ethnicity,
    demo.race                                                              AS race,
    ----------------------------------------------------------------------------------------------------------------------
    -- Below values are removed from the mapping (JKS 2021-09-17)
    ----------------------------------------------------------------------------------------------------------------------
    --demo.religion                                                          AS religion,
    --demo.organ_donor_flag                                                  AS organ_donor_flag,
    ----------------------------------------------------------------------------------------------------------------------
    -- date_of_consent
    ----------------------------------------------------------------------------------------------------------------------
    CAST(
            CASE WHEN LENGTH(COALESCE(demo.date_of_consent,'')) =  11
                 THEN TO_DATE(demo.date_of_consent,  'dd-MMM-yyyy')
                 WHEN LENGTH(COALESCE(demo.date_of_consent,'')) =  9
                 THEN TO_DATE(demo.date_of_consent,  'dd-MMM-yy')
                 WHEN LENGTH(COALESCE(demo.date_of_consent,'')) =  4
                 THEN demo.date_of_consent
                 ELSE NULL
            END
            AS STRING
        )                                                                   AS date_of_consent,
    ----------------------------------------------------------------------------------------------------------------------
    -- date_of_consent_withdrawn
    ----------------------------------------------------------------------------------------------------------------------
    CAST(
            CASE WHEN LENGTH(COALESCE(demo.date_of_consent_withdrawn,'')) =  11
                 THEN TO_DATE(demo.date_of_consent_withdrawn,  'dd-MMM-yyyy')
                 WHEN LENGTH(COALESCE(demo.date_of_consent_withdrawn,'')) =  9
                 THEN TO_DATE(demo.date_of_consent_withdrawn,  'dd-MMM-yy')
                 WHEN LENGTH(COALESCE(demo.date_of_consent_withdrawn,'')) =  4
                 THEN demo.date_of_consent_withdrawn
                 ELSE NULL
            END
            AS STRING
        )                                                                  AS date_of_consent_withdrawn,
    ----------------------------------------------------------------------------------------------------------------------
    -- New Fields added (JKS 2021-09-17)
    ----------------------------------------------------------------------------------------------------------------------
    CASE
        WHEN UPPER(demo.deceased_death_flag) = 'DECEASED' THEN
            CASE
                WHEN CAST(TO_DATE(encounter_date,  'dd-MMM-yyyy') AS DATE) IS NULL  THEN NULL
                WHEN LENGTH(COALESCE(enc.encounter_date,'')) =  11 AND MONTH(TO_DATE(enc.encounter_date,  'dd-MMM-yyyy')) IN (7, 8, 9)
                 AND  YEAR(TO_DATE(enc.encounter_date,  'dd-MMM-yyyy')) = 2021 THEN NULL
                WHEN LENGTH(COALESCE(enc.encounter_date,'')) =  9 AND  MONTH(TO_DATE(enc.encounter_date,  'dd-MMM-yy')) IN (7, 8, 9)
                 AND  YEAR(TO_DATE(enc.encounter_date,  'dd-MMM-yy')) = 2021 THEN NULL

            ELSE demo.deceased_death_flag
            END
        ELSE
        demo.deceased_death_flag
    END                                                                   AS deceased_death_flag,
    TO_DATE(enc.encounter_date,  'dd-MMM-yyyy')                           AS encounter_date,

    'demography'                                                           AS prmy_src_tbl_nm,
    'ccf'                                                                  AS part_provider,
    DATE_FORMAT(current_date, 'yyyy-MM-dd')                                AS part_mth
FROM  demography demo
LEFT OUTER JOIN master_patient mptnt
ON  LOWER(COALESCE(demo.deidentified_master_patient_id, 'NA')) = LOWER(COALESCE(mptnt.deidentified_master_patient_id, 'empty'))
LEFT OUTER JOIN matching_payload pay
ON LOWER(COALESCE(mptnt.deidentified_master_patient_id, 'NA')) = LOWER(COALESCE(pay.claimid, 'empty'))
LEFT OUTER JOIN
/* Get the  max dates for each patient */
(
    SELECT
        enc_date.deidentified_master_patient_id,
        MAX(COALESCE(enc_date.visit_encounter_start_date, enc_date.visit_encounter_end_date)) AS encounter_date
    FROM encounter_crf enc_date
    GROUP BY 1
) enc    ON  enc.deidentified_master_patient_id = demo.deidentified_master_patient_id

WHERE  UPPER(COALESCE(demo.data_source, '')) <> 'DATA_SOURCE'
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19