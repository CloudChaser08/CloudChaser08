SELECT
    CURRENT_DATE()                                                             AS crt_dt,
    ----------------------------------------------------------------------------------------------------------------------
    -- data_set_nm
    ----------------------------------------------------------------------------------------------------------------------
    SPLIT(vac_crf.input_file_name, '/')[SIZE(SPLIT(vac_crf.input_file_name, '/')) - 1] AS data_set_nm,    
    ----------------------------------------------------------------------------------------------------------------------
    pay.hvid 																   AS hvid,
    ----------------------------------------------------------------------------------------------------------------------
    -- ptnt_birth_yr
    ----------------------------------------------------------------------------------------------------------------------
	CAST(
    	CAP_YEAR_OF_BIRTH
    	    (
    	        CAST(COALESCE(pay.age,  mptnt.current_age) AS INT),
                CASE WHEN LENGTH(COALESCE(vac_crf.vaccine_date ,'')) =  11  
                     THEN TO_DATE(vac_crf.vaccine_date ,  'dd-MMM-yyyy')
                     WHEN LENGTH(COALESCE(vac_crf.vaccine_date ,'')) =  9
                     THEN TO_DATE(vac_crf.vaccine_date ,  'dd-MMM-yy')
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
                CASE WHEN LENGTH(COALESCE(vac_crf.vaccine_date ,'')) =  11  
                     THEN TO_DATE(vac_crf.vaccine_date ,  'dd-MMM-yyyy')
                     WHEN LENGTH(COALESCE(vac_crf.vaccine_date ,'')) =  9
                     THEN TO_DATE(vac_crf.vaccine_date ,  'dd-MMM-yy')
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
	vac_crf.deidentified_master_patient_id                                 AS deidentified_master_patient_id,
	vac_crf.deidentified_patient_id                                        AS deidentified_patient_id,
    vac_crf.data_source                                                    AS data_source,
    
    vac_crf.vaccination_id                                                 AS vaccination_id,
    vac_crf.visit_encounter_id                                             AS visit_encounter_id,

    vac_crf.vacc_concept_name                                      AS vacc_concept_name,
    vac_crf.vacc_concept_code                                      AS vacc_concept_code,
    vac_crf.vacc_system_name                                       AS vacc_system_name,
    vac_crf.vaccine_name                                           AS vaccine_name,
    ----------------------------------------------------------------------------------------------------------------------
    --   vaccine_date
    ----------------------------------------------------------------------------------------------------------------------
    CAST(
            CASE WHEN LENGTH(COALESCE(vac_crf.vaccine_date ,'')) =  11  
                 THEN TO_DATE(vac_crf.vaccine_date ,  'dd-MMM-yyyy')
                 WHEN LENGTH(COALESCE(vac_crf.vaccine_date ,'')) =  9
                 THEN TO_DATE(vac_crf.vaccine_date ,  'dd-MMM-yy')
                 WHEN LENGTH(COALESCE(vac_crf.vaccine_date ,'')) =  4 
                 THEN vac_crf.vaccine_date 
                 ELSE NULL                                           
            END  AS STRING
        )                                                          AS vaccine_date,
    ----------------------------------------------------------------------------------------------------------------------
    vac_crf.route_of_administration                                AS route_of_administration,
    vac_crf.vaccine_site                                           AS vaccine_site,
    vac_crf.immunization_active                                    AS immunization_active,
    vac_crf.manufacturer                                           AS manufacturer,
    vac_crf.lot_number                                             AS lot_number,
    vac_crf.vaccine_info_stmt_provided                             AS vaccine_info_stmt_provided,
    vac_crf.place_of_service                                       AS place_of_service,
    'vaccines_crf'                                                            AS prmy_src_tbl_nm,
    'ccf'                                                                     AS part_provider,
    DATE_FORMAT(current_date, 'yyyy-MM-dd')                                     AS part_mth
FROM vaccines_crf vac_crf
LEFT OUTER JOIN master_patient mptnt
ON  LOWER(COALESCE(vac_crf.deidentified_master_patient_id, 'NA')) = LOWER(COALESCE(mptnt.deidentified_master_patient_id, 'empty')) 
LEFT OUTER JOIN matching_payload pay 
ON LOWER(COALESCE(mptnt.deidentified_master_patient_id, 'NA')) = LOWER(COALESCE(pay.claimid, 'empty')) 
WHERE vac_crf.vaccination_id <> 'VACCINATION_ID'
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18,19,20,21,22,23,24,25,26,27
--limit 10
