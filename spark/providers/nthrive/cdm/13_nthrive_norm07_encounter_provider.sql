SELECT  
    MONOTONICALLY_INCREASING_ID()                                                           AS row_id,
    /* hv_enc_id */
    CASE 
        WHEN COALESCE(epi.provider_id, epi.record_id) IS NOT NULL
            THEN CONCAT
                    (
                        '149_',
                        COALESCE(epi.provider_id, 'UNAVAILABLE'),
                        '_',
                        COALESCE(epi.record_id, 'UNAVAILABLE')
                    )
        ELSE NULL
    END                                                                                     AS hv_enc_id,
    CURRENT_DATE()                                                                          AS crt_dt,
	'01'                                                                                    AS mdl_vrsn_num,
    SPLIT(epi.input_file_name, '/')[SIZE(SPLIT(epi.input_file_name, '/')) - 1]              AS data_set_nm,
	492                                                                                     AS hvm_vdr_id,
	149                                                                                     AS hvm_vdr_feed_id,
	epi.provider_id                                                                         AS vdr_org_id,
	epi.record_id                                                                           AS vdr_enc_id,
    /* hvid */
    COALESCE
        (
            pay.hvid, 
            CONCAT
                (
                    '492_', 
                    COALESCE
                        (
                            epi.unique_patient_id, 
                            ptn.unique_patient_id
                        )
                )
        )                                                                                   AS hvid,
    /* ptnt_birth_yr */
	CAP_YEAR_OF_BIRTH
	    (
            COALESCE(epi.age, pay.age),
            to_date(epi.discharge_dt, 'yyyyMMdd'),
            SUBSTR(COALESCE(ptn.patientdob, pay.yearofbirth), 1, 4)
        )                                                                                   AS ptnt_birth_yr,
    /* ptnt_age_num */
	VALIDATE_AGE
	    (
            COALESCE(epi.age, pay.age),
            to_date(epi.discharge_dt, 'yyyyMMdd'),
            SUBSTR(COALESCE(ptn.patientdob, pay.yearofbirth), 1, 4)
	    )                                                                                   AS ptnt_age_num,
	/* ptnt_gender_cd */
	CASE
	    WHEN SUBSTR(UPPER(epi.gender), 1, 1) IN ('F', 'M')
	        THEN SUBSTR(UPPER(epi.gender), 1, 1)
	    WHEN SUBSTR(UPPER(pay.gender), 1, 1) IN ('F', 'M')
	        THEN SUBSTR(UPPER(pay.gender), 1, 1)
	    ELSE 'U'
	END                                                                                     AS ptnt_gender_cd,
	VALIDATE_STATE_CODE(RIGHT(COALESCE(ptn.patientstate, pay.state), 2))                    AS ptnt_state_cd,
    /* ptnt_zip3_cd */
    MASK_ZIP_CODE
        (
            SUBSTR
                (
                    COALESCE
                        (
                            epi.zip_code,
                            ptn.patientzipcode,
                            pay.threedigitzip,
                            ptn.facilityzip
                        ), 1, 3
                )
        )                                                                                   AS ptnt_zip3_cd,
	/* enc_start_dt */
	CAP_DATE
	    (
            to_date(epi.admit_dt, 'yyyyMMdd'),
            CAST('{EARLIEST_SERVICE_DATE}' AS DATE),
            CAST('{VDR_FILE_DT}' AS DATE)
	    )                                                                                   AS enc_start_dt,
	/* enc_end_dt */
	CAP_DATE
	    (
            to_date(epi.discharge_dt, 'yyyyMMdd'),
            CAST('{EARLIEST_SERVICE_DATE}' AS DATE),
            CAST('{VDR_FILE_DT}' AS DATE)
	    )                                                                                   AS enc_end_dt,
	CLEAN_UP_NPI_CODE
	    (
	        ARRAY
	            (
                    epi.attendingphysiciannpi,
                    epi.operatingphysiciannpi,
                    epi.referringphysiciannpi,
                    epi.admittingphysiciannpi,
                    epi.servingphysiciannpi
                )[prv_explode.idx]
	    )                                                                                   AS enc_prov_id,
    ARRAY
        (
            'RENDERING_PROVIDER_NPI',
            'OPERATING_PROVIDER_NPI',
            'REFERRING_PROVIDER_NPI',
            'ADMITTING_PROVIDER_NPI',
            'SERVING_PROVIDER_NPI'
        )[prv_explode.idx]                                                                  AS enc_prov_id_qual,
    'episodes'                                                                              AS prmy_src_tbl_nm,
	'149'                                                                                   AS part_hvm_vdr_feed_id,
	/* part_mth */
	CASE
	    WHEN 0 = LENGTH(TRIM(COALESCE(CAP_DATE
                                        (
                                            to_date(epi.admit_dt, 'yyyyMMdd'),
                                            COALESCE(CAST('{AVAILABLE_START_DATE}' AS DATE), CAST('{EARLIEST_SERVICE_DATE}' AS DATE)),
                                            CAST('{VDR_FILE_DT}' AS DATE)
                                        ), '')))
	        THEN '0_PREDATES_HVM_HISTORY'
	    ELSE CONCAT
	            (
                    SUBSTR(epi.admit_dt, 1, 4), '-',
                    SUBSTR(epi.admit_dt, 5, 2)
                )
	END                                                                                     AS part_mth
 FROM episodes epi
 LEFT OUTER JOIN patient ptn
   ON COALESCE(epi.record_id, 'EMPTY') = COALESCE(ptn.record_id, 'DUMMY')
 LEFT OUTER JOIN matching_payload pay
   ON COALESCE(ptn.hvjoinkey, 'EMPTY') = COALESCE(pay.hvjoinkey, 'DUMMY')
CROSS JOIN (SELECT EXPLODE(ARRAY(0, 1, 2, 3, 4)) AS idx) AS prv_explode
/* Eliminate column headers. */
WHERE UPPER(COALESCE(epi.record_id, '')) <> 'RECORD_ID'
  AND
ARRAY
    (
        epi.attendingphysiciannpi,
        epi.operatingphysiciannpi,
        epi.referringphysiciannpi,
        epi.admittingphysiciannpi,
        epi.servingphysiciannpi
    )[prv_explode.idx] IS NOT NULL
