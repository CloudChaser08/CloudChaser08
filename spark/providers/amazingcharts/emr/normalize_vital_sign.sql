SELECT
    CONCAT(
        '5_',
        SUBSTR(
            COALESCE(
                enc.encounter_date,
                enc.date_row_added
            ),
            1,
            10
        ),
        '_',
        enc.practice_key,
        '_',
        enc.patient_key
    )                                       AS hv_vit_sign_id,
    enc.practice_key                        AS vdr_org_id,
    pay.hvid                                AS hvid,
    COALESCE(
        SUBSTR(ptn.birth_date, 1, 4),
        pay.yearOfBirth
    )                                       AS ptnt_birth_yr,
    pay.age                                 AS ptnt_age_num,
    COALESCE(
        ptn.gender,
        pay.gender
    )                                       AS ptnt_gender_cd,
    UPPER(
        COALESCE(
            ptn.state,
            pay.state,
            ''
        )
    )                                       AS ptnt_state_cd,
    SUBSTR(
        COALESCE(
            ptn.zip,
            pay.threeDigitZip
        ),
        1,
        3
    )                                       AS ptnt_zip3_cd,
    CONCAT(
        '5_',
        SUBSTR(
            COALESCE(
                enc.encounter_date,
                enc.date_row_added
            ),
            1,
            10
        ),
        '_',
        enc.practice_key,
        '_',
        enc.patient_key
    )                                       AS hv_enc_id,
    SUBSTR(
        COALESCE(
            enc.encounter_date,
            enc.date_row_added
        ),
        1,
        10
    )                                       AS enc_dt,
    SUBSTR(
        COALESCE(
            enc.encounter_date,
            enc.date_row_added
        ),
        1,
        10
    )                                       AS vit_sign_dt,
    enc.provider_key                        AS vit_sign_rndrg_prov_vdr_id,
    CASE
        WHEN enc.provider_key IS NULL THEN NULL
        ELSE 'PROVIDER_KEY'
    END                                     AS vit_sign_rndrg_prov_vdr_id_qual,
    enc.practice_key                        AS vit_sign_rndrg_prov_alt_id,
    CASE
        WHEN enc.practice_key IS NULL THEN NULL
        ELSE 'PRACTICE_KEY'
    END                                     AS vit_sign_rndrg_prov_alt_id_qual,
    prv.specialty                           AS vit_sign_rndrg_prov_alt_speclty_id,
    CASE
        WHEN prv.specialty IS NULL THEN NULL
        ELSE 'SPECIALTY'
    END                                     AS vit_sign_rndrg_prov_alt_speclty_id_qual,
    UPPER(COALESCE(prv.state, ''))          AS vit_sign_rndrg_prov_state_cd,
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
    )[e.n]                                  AS vit_sign_typ_cd,
    ARRAY(
        enc.body_mass_index,
        enc.diastolic,
        CASE
            WHEN UPPER(SUBSTR(enc.head_circumference, -2)) = 'IN' THEN REGEXP_REPLACE(enc.head_circumference, '[^0-9|\.]', '')
            WHEN UPPER(SUBSTR(enc.head_circumference, -2)) = 'CM' THEN CONVERT_VALUE(REGEXP_REPLACE(enc.head_circumference, '[^0-9|\.]', ''), 'CENTIMETERS_TO_INCHES')
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
        enc.rest_rate,
        enc.supplemental_o2_amount,
        enc.systolic,
        CASE
            WHEN UPPER(SUBSTR(enc.temperature, -1)) = 'F' THEN REGEXP_REPLACE(enc.temperature, '[^0-9|\.]', '')
            WHEN UPPER(SUBSTR(enc.temperature, -1)) = 'C' THEN CONVERT_VALUE(REGEXP_REPLACE(enc.temperature, '[^0-9|\.]', ''), 'CENTIGRADE_TO_FAHRENHEIT')
            ELSE NULL
        END,
        enc.vision_od,
        enc.vision_os,
        enc.weight_in_pounds
    )[e.n]                                  AS vit_sign_msrmt,
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
        'SNELLEL_CHART',
        'POUNDS'
    )[e.n]                                  AS vit_sign_uom,
    extract_date(
        SUBSTR(enc.date_row_added, 1, 10),
        '%Y-%m-%d'
    )                                       AS data_captr_dt,
    'f_encounter'                           AS prmy_src_tbl_nm
FROM f_encounter enc
    LEFT OUTER JOIN d_patient ptn ON enc.patient_key = ptn.patient_key
    LEFT OUTER JOIN matching_payload_deduped pay ON ptn.patient_key = pay.personid
    LEFT OUTER JOIN d_provider prv ON enc.provider_key = prv.provider_key
    INNER JOIN vital_sign_exploder e
WHERE
-- Only keep a vital if the measurement is not null --
-- Don't create a target row with NULL code if measurements are all NULL --
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
        enc.rest_rate,
        enc.supplemental_o2_amount,
        enc.systolic,
        enc.temperature,
        enc.vision_od,
        enc.vision_os,
        enc.weight_in_pounds
    )[e.n] IS NOT NULL
