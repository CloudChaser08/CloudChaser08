SELECT
    CURRENT_DATE()                                                              AS crt_dt,
	'07'                                                                        AS mdl_vrsn_num,
    CONCAT(
        'AmazingCharts_HV_','{VDR_FILE_DT}', '_' ,
        SPLIT(vit.input_file_name, '/')[SIZE(SPLIT(vit.input_file_name, '/')) - 1]
        )                                                                       AS data_set_nm,
    5                                                                           AS hvm_vdr_id,
	5                                                                           AS hvm_vdr_feed_id,
    -------------------------------------------------------------------------------------------------------------------------
    --  hv_clin_obsn_id - JKS 2021-12-15
    -------------------------------------------------------------------------------------------------------------------------
    CONCAT('5_', COALESCE(SUBSTR(cln.encounter_date, 1, 10), SUBSTR(cln.date_row_added, 1, 10), '0000-00-00' ),
        '_', cln.practice_key,
        '_', cln.patient_key
    )                                                                           AS hv_clin_obsn_id,

    cln.practice_key                                                            AS vdr_org_id,
    -------------------------------------------------------------------------------------------------------------------------
    --  vdr_clin_obsn_id and vdr_clin_obsn_id_qual - JKS 2021-12-15
    -------------------------------------------------------------------------------------------------------------------------
    vit.visit_key                                                               AS vdr_clin_obsn_id,
    CASE
        WHEN vit.visit_key IS NULL THEN NULL
        ELSE 'VISIT_KEY'
    END                                                                         AS vdr_clin_obsn_id_qual,
    
    pay.hvid                                                                    AS hvid,
    CAP_YEAR_OF_BIRTH(                                                          
        pay.age,
        CAST(
            EXTRACT_DATE(COALESCE(cln.encounter_date, SUBSTR(cln.date_row_added, 1, 10)), '%Y-%m-%d')
            AS DATE),
        COALESCE(SUBSTR(ptn.birth_date, 5, 4),  pay.yearOfBirth)
    )                                                                           AS ptnt_birth_yr,
    VALIDATE_AGE(
        pay.age,
        CAST(
            EXTRACT_DATE(COALESCE(cln.encounter_date, SUBSTR(cln.date_row_added, 1, 10)), '%Y-%m-%d')
            AS DATE),
        COALESCE(SUBSTR(ptn.birth_date, 5, 4),  pay.yearOfBirth)
    )                                                                           AS ptnt_age_num,
    COALESCE(ptn.gender, pay.gender)                                            AS ptnt_gender_cd,
    VALIDATE_STATE_CODE(
        UPPER(COALESCE(ptn.state, pay.state, ''))
    )                                                                           AS ptnt_state_cd,
    MASK_ZIP_CODE(
        SUBSTR(COALESCE(ptn.zip, pay.threeDigitZip), 1, 3)
    )                                                                           AS ptnt_zip3_cd,
    -------------------------------------------------------------------------------------------------------------------------
    --  hv_enc_id - JKS 2021-12-15
    -------------------------------------------------------------------------------------------------------------------------
    CONCAT('5_', COALESCE(SUBSTR(cln.encounter_date, 1, 10), SUBSTR(cln.date_row_added, 1, 10), '0000-00-00' ),
        '_', cln.practice_key,
        '_', cln.patient_key
    )                                                                           AS hv_enc_id,
    
    
    EXTRACT_DATE(
        SUBSTR(COALESCE(cln.encounter_date, cln.date_row_added), 1, 10),
        '%Y-%m-%d',
        CAST('{EARLIEST_SERVICE_DATE}' AS DATE),
        CAST('{VDR_FILE_DT}' AS DATE)
    )                                                                           AS enc_dt,
    EXTRACT_DATE(
        SUBSTR(COALESCE(cln.encounter_date, cln.date_row_added), 1, 10),
        '%Y-%m-%d',
        CAST('{EARLIEST_SERVICE_DATE}' AS DATE),
        CAST('{VDR_FILE_DT}' AS DATE)
    )                                                                           AS clin_obsn_dt,
    cln.provider_key                                                            AS clin_obsn_rndrg_prov_vdr_id,

    CASE
        WHEN cln.provider_key IS NULL THEN NULL
        ELSE 'PROVIDER_KEY'
    END                                                                         AS clin_obsn_rndrg_prov_vdr_id_qual,

    cln.practice_key                                                            AS clin_obsn_rndrg_prov_alt_id,

    CASE
        WHEN cln.practice_key IS NULL THEN NULL
        ELSE 'PRACTICE_KEY'
    END                                                                         AS clin_obsn_rndrg_prov_alt_id_qual,
    
    prv.specialty                                                               AS clin_obsn_rndrg_prov_alt_speclty_id,
    
    CASE
        WHEN prv.specialty IS NULL THEN NULL
        ELSE 'SPECIALTY'
    END                                                                         AS clin_obsn_rndrg_prov_alt_speclty_id_qual,
    VALIDATE_STATE_CODE(
        UPPER(COALESCE(prv.state, ''))
        )                                                                       AS clin_obsn_rndrg_prov_state_cd,

    vit.data_columns                                                            AS clin_obsn_cd,
    CAST(NULL AS STRING)                                                        AS clin_obsn_nm,
    CAST(NULL AS STRING)                                                        AS clin_obsn_result_desc,

    CASE
        WHEN vit.data_columns = 'TEMPERATURE' AND UPPER(SUBSTR(REVERSE(vit.data_values ), 1, 1) ) ='F' 
            THEN REGEXP_REPLACE(vit.data_values, '[F]+', '')
    
        WHEN vit.data_columns = 'TEMPERATURE'
                AND   UPPER(SUBSTR(REVERSE(vit.data_values ), 1, 1) ) ='C'
            THEN  CONVERT_CELSIUS_TO_FAHRENHEIT(CAST(REGEXP_REPLACE(data_values, '[C]+', '') AS FLOAT))

        WHEN vit.data_columns = 'RESPIRATORY_RATE'
            THEN data_values
        
        WHEN vit.data_columns = 'WEIGHT'
                AND   UPPER(SUBSTR(REVERSE(vit.data_values ), 1, 2) ) ='GK'         
            THEN CONVERT_KG_TO_LB  (CAST(  REGEXP_REPLACE(data_values, '[kg]+', '') AS FLOAT ) )
            
        WHEN vit.data_columns = 'WEIGHT'
                AND   UPPER(SUBSTR(REVERSE(vit.data_values ), 1, 2) ) ='BL'         
            THEN REGEXP_REPLACE(vit.data_values, '[lb]+', '')

        WHEN vit.data_columns = 'HEIGHT'                             
                AND UPPER(SUBSTR(REVERSE(vit.data_values ), 1, 1)) ='M' 
            THEN CONVERT_VALUE(CAST(  REGEXP_REPLACE(data_values, '[m]+', '')       AS FLOAT ) * 100 , 'CENTIMETERS_TO_INCHES')

        WHEN vit.data_columns = 'HEIGHT'                             
                AND UPPER(SUBSTR(REVERSE(vit.data_values ), 1, 2)) ='NI' 
            THEN REGEXP_REPLACE(data_values, '[in]+', '')

        WHEN  vit.data_columns = 'HEAD_CIRCUMFERENCE'   
                AND UPPER(SUBSTR(REVERSE(vit.data_values ), 1, 2)) ='NI'  
            THEN REGEXP_REPLACE(data_values, '[in]+', '')                                  

        WHEN  vit.data_columns = 'HEAD_CIRCUMFERENCE'   
                AND UPPER(SUBSTR(REVERSE(vit.data_values ), 1, 2)) ='MC' 
            THEN  CONVERT_CM_TO_IN(CAST(REGEXP_REPLACE(data_values, '[cm]+', '') AS FLOAT))
        
        WHEN vit.data_columns = 'OXYGEN_SATURATION_ROOM_AIR'
                AND UPPER(vit.data_values ) IN ('1', 'TRUE')
            THEN 'Y'

        WHEN vit.data_columns = 'OXYGEN_SATURATION_ROOM_AIR'
                AND UPPER(vit.data_values ) IN ('O', 'FALSE')
            THEN 'N'

        WHEN vit.data_columns = 'LAST_MENSTRUAL_PERIOD'
            THEN CAST(EXTRACT_DATE(SUBSTR(vit.data_values,1, 10), '%Y-%m-%d') AS DATE)
            
        WHEN vit.data_columns = 'BODY_MASS_INDEX'
            THEN
                CLEAN_UP_VITAL_SIGN
                    (
                    'BMI',
                    vit.data_columns,
                    'PERCENT',
                    COALESCE(ptn.gender, pay.gender),
                    pay.age, 
                    CAP_YEAR_OF_BIRTH
                        (                                                          
                        pay.age,
                        CAST(EXTRACT_DATE(COALESCE(cln.encounter_date, SUBSTR(cln.date_row_added, 1, 10)), '%Y-%m-%d') AS DATE),
                        COALESCE(SUBSTR(ptn.birth_date, 5, 4),  pay.yearOfBirth)), 
                    CAST(EXTRACT_DATE(COALESCE(cln.encounter_date, SUBSTR(cln.date_row_added, 1, 10)), '%Y-%m-%d') AS DATE),
                    CAST(EXTRACT_DATE(COALESCE(cln.encounter_date, SUBSTR(cln.date_row_added, 1, 10)), '%Y-%m-%d') AS DATE)
                    )
        ELSE vit.data_values                                                        
    END                                                                         AS clin_obsn_msrmt,
        
    CASE
        WHEN  vit.data_columns LIKE '%BLOOD_PRESSURE%'              THEN 'mmHg'
        WHEN  vit.data_columns = 'SYSTOLIC'                         THEN 'mmHg'
        WHEN  vit.data_columns = 'DIASTOLIC'                        THEN 'mmHg'
        WHEN  vit.data_columns = 'TEMPERATURE'                      THEN 'FAHRENHEIT'
        WHEN  vit.data_columns = 'TEMP_IN_FAHRENHEIT'               THEN 'FAHRENHEIT'
        WHEN  vit.data_columns = 'RESPIRATORY_RATE'                 THEN 'BREATHS_PER_MINUTE'
        WHEN  vit.data_columns = 'PULSE'                            THEN 'BEATS_PER_MINUTE'
        WHEN  vit.data_columns = 'WEIGHT'                           THEN 'POUNDS'
        WHEN  vit.data_columns = 'WEIGHT_IN_POUNDS'                 THEN 'POUNDS'
        WHEN  vit.data_columns = 'HEIGHT'                           THEN 'INCHES'
        WHEN  vit.data_columns = 'HEIGHT_IN_INCHES'                 THEN 'INCHES'
        WHEN  vit.data_columns = 'BODY_MASS_INDEX'                  THEN 'BMI'
        WHEN  vit.data_columns = 'HEAD_CIRCUMFERENCE'               THEN 'INCHES'
        WHEN  vit.data_columns = 'VITAL_COMMENTS'                   THEN NULL
        WHEN  vit.data_columns = 'OXYGEN_SATURATION'                THEN 'O2_SATURATION'
        WHEN  vit.data_columns = 'PAIN_SCALE'                       THEN '0_THROUGH_10'
        WHEN  vit.data_columns = 'PULMONARY_FUNCTION'               THEN 'LITERS_PER_MINUTE'
        WHEN  vit.data_columns = 'OTHER_VITALS'                     THEN NULL
        WHEN  vit.data_columns = 'OXYGEN_SATURATION_ROOM_AIR'       THEN 'YES/NO'
        WHEN vit.data_columns  = 'SUPPLEMENTAL_O2_AMOUNT'  
            AND vit.data_values IN ('0', 'False')                   THEN 'LITERS_PER_MINUTE' 
        WHEN vit.data_columns = 'SUPPLEMENTAL_O2_TYPE'   
            AND vit.data_values IN ('1', 'True')                    THEN 'PERCENT'
        WHEN  vit.data_columns = 'PEAK_FLOW_POST_BRONCHODILATOR'    THEN 'LITERS_PER_MINUTE'
        WHEN  vit.data_columns = 'LAST_MENSTRUAL_PERIOD'            THEN 'DATE'
        WHEN  vit.data_columns = 'VISION_OS'                        THEN 'SNELLEN_CHART'
        WHEN  vit.data_columns = 'VISION_OD'                        THEN 'SNELLEN_CHART'
        WHEN  vit.data_columns = 'HEARING'                          THEN 'PASS/FAIL'
        WHEN  vit.data_columns = 'HEARING_COMMENTS'                 THEN 'BREATHS_PER_MINUTE'
        ELSE NULL                                                           
    END                                                                         AS clin_obsn_uom,
    

    EXTRACT_DATE(
        SUBSTR(cln.date_row_added, 1, 10),
        '%Y-%m-%d',
        CAST('{EARLIEST_SERVICE_DATE}' AS DATE),
        CAST('{VDR_FILE_DT}' AS DATE)
    )                                                                           AS data_captr_dt,

    'f_vital'                                                                   AS prmy_src_tbl_nm,
    '5'										                                    AS part_hvm_vdr_feed_id,
    -------------------------------------------------------------------------------------------------------------------------
    --  part_mth
    -------------------------------------------------------------------------------------------------------------------------

    CASE
	    WHEN CAP_DATE
	            (
                    CAST(EXTRACT_DATE(SUBSTR(COALESCE(cln.encounter_date, cln.date_row_added), 1, 10), '%Y-%m-%d') AS DATE),
                    CAST('{AVAILABLE_START_DATE}' AS DATE),
                    CAST('{VDR_FILE_DT}' AS DATE)
                )
                    IS NULL THEN '0_PREDATES_HVM_HISTORY'
	    ELSE SUBSTR(COALESCE(cln.encounter_date, cln.date_row_added), 1, 7)
	END                                                                         AS part_mth



FROM f_vital vit 
LEFT OUTER JOIN f_clinical_note cln     ON COALESCE(vit.visit_key, 'NULL')    = COALESCE(cln.visit_key, 'empty')
LEFT OUTER JOIN d_patient ptn           ON COALESCE(cln.patient_key, 'NULL')  = COALESCE(ptn.patient_key, 'empty') 
LEFT OUTER JOIN matching_payload pay    ON COALESCE(ptn.patient_key, 'NULL')  = COALESCE(pay.personid, 'empty') 
LEFT OUTER JOIN d_provider prv          ON COALESCE(cln.provider_key, 'NULL') = COALESCE(prv.provider_key, 'empty')
WHERE TRIM(UPPER(COALESCE(cln.practice_key, 'empty'))) <> 'PRACTICE_KEY'

-- LIMIT 10
