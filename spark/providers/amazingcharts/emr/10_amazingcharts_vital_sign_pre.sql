SELECT
    CURRENT_DATE()                                                              AS crt_dt,
	'07'                                                                        AS mdl_vrsn_num,
    CONCAT(
        'AmazingCharts_HV_{VDR_FILE_DT}_',
        SPLIT(enc.input_file_name, '/')[SIZE(SPLIT(enc.input_file_name, '/')) - 1]
        )                                                                       AS data_set_nm,
	5                                                                           AS hvm_vdr_id,
	5                                                                           AS hvm_vdr_feed_id,
    CONCAT(
        '5_',
        SUBSTR(
            COALESCE(
                enc.encounter_date,
                enc.date_row_added,
                '0000-00-00'
            ),
            1,
            10
        ),
        '_',
        enc.practice_key,
        '_',
        enc.patient_key
    )                                                                           AS hv_vit_sign_id,
    enc.practice_key                                                            AS vdr_org_id,
    pay.hvid                                                                    AS hvid,
    CAP_YEAR_OF_BIRTH(
        pay.age,
        CAST(
            EXTRACT_DATE(COALESCE(enc.encounter_date, SUBSTR(enc.date_row_added, 1, 10)), '%Y-%m-%d')
            AS DATE),
        COALESCE(SUBSTR(ptn.birth_year, 1, 4),  pay.yearOfBirth)
    )                                                                           AS ptnt_birth_yr,
    VALIDATE_AGE(
        pay.age,
        CAST(
            EXTRACT_DATE(COALESCE(enc.encounter_date, SUBSTR(enc.date_row_added, 1, 10)), '%Y-%m-%d')
            AS DATE),
        COALESCE(SUBSTR(ptn.birth_year, 1, 4),  pay.yearOfBirth)
    )                                                                           AS ptnt_age_num,
    COALESCE(ptn.gender, pay.gender)                                            AS ptnt_gender_cd,
    VALIDATE_STATE_CODE(
        UPPER(COALESCE(ptn.state, pay.state, ''))
    )                                                                           AS ptnt_state_cd,
    MASK_ZIP_CODE(
        SUBSTR(COALESCE(ptn.zip, pay.threeDigitZip), 1, 3)
    )                                                                           AS ptnt_zip3_cd,
    CONCAT(
        '5_',
        SUBSTR(
            COALESCE(
                enc.encounter_date,
                enc.date_row_added,
                '0000-00-00'
            ),
            1,
            10
        ),
        '_',
        enc.practice_key,
        '_',
        enc.patient_key
    )                                                                           AS hv_enc_id,
    EXTRACT_DATE(
        SUBSTR(COALESCE(enc.encounter_date, enc.date_row_added), 1, 10),
        '%Y-%m-%d',
        CAST('{EARLIEST_SERVICE_DATE}' AS DATE),
        CAST('{VDR_FILE_DT}' AS DATE)
    )                                                                           AS enc_dt,
    EXTRACT_DATE(
        SUBSTR(COALESCE(enc.encounter_date, enc.date_row_added), 1, 10),
        '%Y-%m-%d',
        CAST('{EARLIEST_SERVICE_DATE}' AS DATE),
        CAST('{VDR_FILE_DT}' AS DATE)
    )                                                                           AS vit_sign_dt,
    enc.provider_key                                                            AS vit_sign_rndrg_prov_vdr_id,
    CASE
        WHEN enc.provider_key IS NULL THEN NULL
        ELSE 'PROVIDER_KEY'
    END                                                                         AS vit_sign_rndrg_prov_vdr_id_qual,
    enc.practice_key                                                            AS vit_sign_rndrg_prov_alt_id,
    CASE
        WHEN enc.practice_key IS NULL THEN NULL
        ELSE 'PRACTICE_KEY'
    END                                                                         AS vit_sign_rndrg_prov_alt_id_qual,
    prv.specialty                                                               AS vit_sign_rndrg_prov_alt_speclty_id,
    CASE
        WHEN prv.specialty IS NULL THEN NULL
        ELSE 'SPECIALTY'
    END                                                                         AS vit_sign_rndrg_prov_alt_speclty_id_qual,
    VALIDATE_STATE_CODE(
        UPPER(COALESCE(prv.state, '')))                                         AS vit_sign_rndrg_prov_state_cd,
    ARRAY(
        'BMI',
        'DIASTOLIC',
        'HEAD_CIRCUMFERENCE',
        'HEARING',
        'HEIGHT',
        'O2_SATURATION',
        'O2_SATURATION_ROOM_AIR',
        'PAIN',
        'PEFR_POST_BRONCHODILATION',
        'PEFR',
        'PULSE',
        'RESPIRATION',
        'SUPPLEMENTAL_O2',
        'SYSTOLIC',
        'BODY_TEMPERATURE',
        'VISION_OD',
        'VISION_OS',
        'WEIGHT'
    )[vital_sign_exploder.n]                                                    AS vit_sign_typ_cd,
    ARRAY(
        enc.body_mass_index,
        enc.diastolic,
        CASE
            WHEN UPPER(SUBSTR(enc.head_circumference, -2)) = 'IN' THEN
                REGEXP_REPLACE(enc.head_circumference, '[^0-9|\.]', '')
            WHEN UPPER(SUBSTR(enc.head_circumference, -2)) = 'CM' THEN
                CONVERT_VALUE(REGEXP_REPLACE(enc.head_circumference, '[^0-9|\.]', ''), 'CENTIMETERS_TO_INCHES')
            ELSE NULL
        END,
        enc.hearing,
        enc.height_in_inches,
        enc.oxygen_saturation,
        CASE
            WHEN enc.oxygen_saturation_room_air = '1' THEN 'Y'
            WHEN enc.oxygen_saturation_room_air = '0' THEN 'N'
            ELSE NULL
        END,
        enc.pain_scale,
        enc.peak_flow_post_bronchodilator,
        enc.pulmonary_function,
        enc.pulse,
        enc.respiratory_rate,
        enc.supplemental_o2_amount,
        enc.systolic,
        CASE
            WHEN UPPER(SUBSTR(enc.temperature, -1)) = 'F' THEN
                REGEXP_REPLACE(enc.temperature, '[^0-9|\.]', '')
            WHEN UPPER(SUBSTR(enc.temperature, -1)) = 'C' THEN
                CONVERT_VALUE(REGEXP_REPLACE(enc.temperature, '[^0-9|\.]', ''), 'CENTIGRADE_TO_FAHRENHEIT')
            ELSE NULL
        END,
        enc.vision_od,
        enc.vision_os,
        enc.weight_in_pounds
    )[vital_sign_exploder.n]                                                    AS vit_sign_msrmt,
    ARRAY(
        'INDEX',
        'mmHg',
        'INCHES',
        'PASS/FAIL',
        'INCHES',
        'PERCENT',
        'YES/NO',
        '0_THROUGH_10',
        'LITERS_PER_MINUTE',
        'LITERS_PER_MINUTE',
        'BEATS_PER_MINUTE',
        'BREATHS_PER_MINUTE',
        CASE
            WHEN enc.supplemental_o2_type = '0' THEN 'LITERS_PER_MINUTE'
            WHEN enc.supplemental_o2_type = '1' THEN 'PERCENT'
            ELSE NULL
        END,
        'mmHg',
        'FAHRENHEIT',
        'SNELLEN_CHART',
        'SNELLEN_CHART',
        'POUNDS'
    )[vital_sign_exploder.n]                                                    AS vit_sign_uom,
    EXTRACT_DATE(
        SUBSTR(enc.date_row_added, 1, 10),
        '%Y-%m-%d',
        CAST('{EARLIEST_SERVICE_DATE}' AS DATE),
        CAST('{VDR_FILE_DT}' AS DATE)
    )                                                                           AS data_captr_dt,
    'f_encounter'                                                               AS prmy_src_tbl_nm,
    '5'										                                    AS part_hvm_vdr_feed_id,
    -------------------------------------------------------------------------------------------------------------------------
    --  part_mth
    -------------------------------------------------------------------------------------------------------------------------
    CASE
	    WHEN 0 = LENGTH(
	    COALESCE(
            CAP_DATE(
                CAST(EXTRACT_DATE(COALESCE(enc.encounter_date, SUBSTR(enc.date_row_added, 1, 10)), '%Y-%m-%d') AS DATE),
                CAST(COALESCE('{AVAILABLE_START_DATE}', '{EARLIEST_SERVICE_DATE}') AS DATE),
                CAST('{VDR_FILE_DT}' AS DATE)
                ),
                ''
            )
        ) THEN '0_PREDATES_HVM_HISTORY'
	    ELSE SUBSTR(COALESCE(enc.encounter_date, SUBSTR(enc.date_row_added, 1, 10)), 1, 7)
	END                                                                         AS part_mth
FROM f_encounter enc
    LEFT OUTER JOIN d_patient ptn ON enc.patient_key = ptn.patient_key
    LEFT OUTER JOIN matching_payload pay ON ptn.patient_key = pay.personid
    LEFT OUTER JOIN d_provider prv ON enc.provider_key = prv.provider_key
    INNER JOIN (SELECT EXPLODE(ARRAY(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17)) AS n) vital_sign_exploder
WHERE
-- Only keep a vital if the measurement is not null --
-- Don't create a target row with NULL code if measurements are all NULL --
    TRIM(UPPER(COALESCE(enc.practice_key, 'empty'))) <> 'PRACTICE_KEY'
    AND
    ARRAY(
        enc.body_mass_index,
        enc.diastolic,
        enc.head_circumference,
        enc.hearing,
        enc.height_in_inches,
        enc.oxygen_saturation,
        enc.oxygen_saturation_room_air,
        enc.pain_scale,
        enc.peak_flow_post_bronchodilator,
        enc.pulmonary_function,
        enc.pulse,
        enc.respiratory_rate,
        enc.supplemental_o2_amount,
        enc.systolic,
        enc.temperature,
        enc.vision_od,
        enc.vision_os,
        enc.weight_in_pounds
    )[vital_sign_exploder.n] IS NOT NULL
    AND
    ((vital_sign_exploder.n != 1 AND vital_sign_exploder.n != 2 AND vital_sign_exploder.n != 4) OR CAST(ARRAY(
        enc.body_mass_index,
        enc.diastolic,
        REGEXP_REPLACE(enc.head_circumference, '[^0-9|\.]', ''),
        enc.hearing,
        enc.height_in_inches,
        enc.oxygen_saturation,
        enc.oxygen_saturation_room_air,
        enc.pain_scale,
        enc.peak_flow_post_bronchodilator,
        enc.pulmonary_function,
        enc.pulse,
        enc.respiratory_rate,
        enc.supplemental_o2_amount,
        enc.systolic,
        enc.temperature,
        enc.vision_od,
        enc.vision_os,
        enc.weight_in_pounds
    )[vital_sign_exploder.n] AS FLOAT) != 0)
