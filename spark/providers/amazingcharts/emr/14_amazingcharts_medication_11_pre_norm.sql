SELECT
    CURRENT_DATE()                                                                      AS crt_dt,
	'07'                                                                                AS mdl_vrsn_num,
    CONCAT(
        'AmazingCharts_HV_','{VDR_FILE_DT}', '_' ,
        SPLIT(med.input_file_name, '/')[SIZE(SPLIT(med.input_file_name, '/')) - 1])     AS data_set_nm,
	5                                                                                   AS hvm_vdr_id,
	5                                                                                   AS hvm_vdr_feed_id,
    CONCAT(
        '5_',
        COALESCE(
            SUBSTR(med.date_initiated, 1, 10),
            '0000-00-00'
        ),
        '_',
        med.practice_key,
        '_',
        med.patient_key
    )                                                                                   AS hv_medctn_id,
    med.practice_key                                                                    AS vdr_org_id,
    pay.hvid                                                                            AS hvid,
    COALESCE(
        SUBSTR(ptn.birth_date, 5, 4),
        pay.yearOfBirth
    )                                                                                   AS ptnt_birth_yr,
    pay.age                                                                             AS ptnt_age_num,
    COALESCE(
        ptn.gender,
        pay.gender
    )                                                                                   AS ptnt_gender_cd,
    UPPER(
        COALESCE(
            ptn.state,
            pay.state,
            ''
        )
    )                                                                                   AS ptnt_state_cd,
    SUBSTR(
        COALESCE(
            ptn.zip,
            pay.threeDigitZip
        ),
        1,
        3
    )                                                                                   AS ptnt_zip3_cd,
    EXTRACT_DATE(
        SUBSTR(med.date_initiated, 1, 10),
        '%Y-%m-%d',
        CAST('{EARLIEST_SERVICE_DATE}' AS DATE),
        CAST('{VDR_FILE_DT}' AS DATE)
    )                                                                                   AS medctn_ord_dt,
    EXTRACT_DATE(
        SUBSTR(med.date_initiated, 1, 10),
        '%Y-%m-%d',
        CAST('{EARLIEST_SERVICE_DATE}' AS DATE),
        CAST('{VDR_FILE_DT}' AS DATE)
    )                                                                                   AS medctn_admin_dt,
    med.provider_key                                                                    AS medctn_ordg_prov_vdr_id,
    CASE
        WHEN med.provider_key IS NULL THEN NULL
        ELSE 'PROVIDER_KEY'
    END                                                                                 AS medctn_ordg_prov_vdr_id_qual,
    med.practice_key                                                                    AS medctn_ordg_prov_alt_id,
    CASE
        WHEN med.practice_key IS NULL THEN NULL
        ELSE 'PRACTICE_KEY'
    END                                                                                 AS medctn_ordg_prov_alt_id_qual,
    prv.specialty                                                                       AS medctn_ordg_prov_alt_speclty_id,
    CASE
        WHEN prv.specialty IS NULL THEN NULL
        ELSE 'SPECIALTY'
    END                                                                                 AS medctn_ordg_prov_alt_speclty_id_qual,
    VALIDATE_STATE_CODE(UPPER(COALESCE(prv.state, '')))                                 AS medctn_ordg_prov_state_cd,
    EXTRACT_DATE(
        SUBSTR(med.date_initiated, 1, 10),
        '%Y-%m-%d',
        CAST('{EARLIEST_SERVICE_DATE}' AS DATE),
        CAST('{VDR_FILE_DT}' AS DATE)
    )                                                                                   AS medctn_start_dt,
    EXTRACT_DATE(
        SUBSTR(med.date_inactivated, 1, 10),
        '%Y-%m-%d',
        CAST('{EARLIEST_SERVICE_DATE}' AS DATE),
        CAST('{VDR_FILE_DT}' AS DATE)
    )                                                                                   AS medctn_end_dt,
    CLEAN_UP_NDC_CODE(ndc.ndc)                                                          AS medctn_ndc,
    med.med_name                                                                        AS medctn_brd_nm,
    drg.generic_name                                                                    AS medctn_genc_nm,
    drg.dosage_form                                                                     AS medctn_admin_form_nm,
    drg.strength                                                                        AS medctn_dose_txt,
    CASE
        WHEN drg.strength IS NULL THEN NULL
        ELSE 'D_DRUG.STRENGTH'
    END                                                                                 AS medctn_dose_txt_qual,
    drg.strength_uom                                                                    AS medctn_dose_uom,
    drg.route                                                                           AS medctn_admin_rte_txt,
    EXTRACT_DATE(
        SUBSTR(med.date_last_refilled, 1, 10),
        '%Y-%m-%d',
        CAST('{EARLIEST_SERVICE_DATE}' AS DATE),
        CAST('{VDR_FILE_DT}' AS DATE)
    )                                                                                   AS medctn_last_rfll_dt,
    CASE
        WHEN med.deleted = 'True' OR med.deleted = '1' THEN 'DELETED'
        ELSE NULL
    END                                                                                 AS rec_stat_cd,
    'f_medication'                                                                      AS prmy_src_tbl_nm,
    '5'										                                            AS part_hvm_vdr_feed_id,
    -------------------------------------------------------------------------------------------------------------------------
    --  part_mth
    -------------------------------------------------------------------------------------------------------------------------

    CASE
	    WHEN CAP_DATE
	            (
                    CAST(EXTRACT_DATE(SUBSTR(med.date_initiated, 1, 10), '%Y-%m-%d') AS DATE),
                    CAST('{AVAILABLE_START_DATE}' AS DATE),
                    CAST('{VDR_FILE_DT}' AS DATE)
                )
                    IS NULL THEN '0_PREDATES_HVM_HISTORY'
	    ELSE SUBSTR(med.date_initiated, 1, 7)
	END                                                                         AS part_mth

FROM f_medication med
LEFT OUTER JOIN d_patient ptn ON med.patient_key = ptn.patient_key
LEFT OUTER JOIN matching_payload pay ON ptn.patient_key = pay.personid
LEFT OUTER JOIN d_provider prv ON med.provider_key = prv.provider_key
LEFT OUTER JOIN d_drug drg ON med.drug_key = drg.drug_id
LEFT OUTER JOIN raw.amazingcharts_d_multum_to_ndc ndc ON drg.drug_id = ndc.multum_id
-- LEFT OUTER JOIN d_multum_to_ndc ndc ON drg.drug_id = ndc.multum_id
WHERE
    TRIM(UPPER(COALESCE(med.practice_key, 'empty'))) <> 'PRACTICE_KEY'
-- LIMIT 10
