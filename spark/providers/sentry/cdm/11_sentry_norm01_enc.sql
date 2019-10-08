SELECT 
    MONOTONICALLY_INCREASING_ID()                                                           AS row_id,
    /* hv_enc_id */
    CONCAT
        (
            '150_',
            COALESCE(enc.hvid, 'UNAVAILABLE'),
            '_',
            COALESCE(enc.admit_dt, 'UNAVAILABLE'),
            '_',
            MD5(COALESCE(SPLIT(enc.input_file_name, '/')[SIZE(SPLIT(enc.input_file_name, '/')) - 1], 'UNAVAILABLE')),
            '_',
            enc.enc_id
        )                                                                                   AS hv_enc_id,
    CURRENT_DATE()                                                                          AS crt_dt,
	'02'                                                                                    AS mdl_vrsn_num,
    SPLIT(enc.input_file_name, '/')[SIZE(SPLIT(enc.input_file_name, '/')) - 1]              AS data_set_nm,
	493                                                                                     AS hvm_vdr_id,
	150                                                                                     AS hvm_vdr_feed_id,
	enc.facility_id                                                                         AS vdr_org_id,
	enc.hvid,
    /* ptnt_birth_yr */
	CAP_YEAR_OF_BIRTH
	    (
            enc.age,
            CAST(EXTRACT_DATE(enc.disch_dt, '%Y-%m-%d') AS DATE),
            SUBSTR(enc.yearofbirth, 1, 4)
        )                                                                                   AS ptnt_birth_yr,
    /* ptnt_age_num */
	VALIDATE_AGE
	    (
            enc.age,
            CAST(EXTRACT_DATE(enc.disch_dt, '%Y-%m-%d') AS DATE),
            SUBSTR(enc.yearofbirth, 1, 4)
	    )                                                                                   AS ptnt_age_num,
	/* ptnt_gender_cd */
	CASE
	    WHEN SUBSTR(UPPER(enc.gender), 1, 1) IN ('F', 'M')
	        THEN SUBSTR(UPPER(enc.gender), 1, 1)
	    ELSE 'U'
	END                                                                                     AS ptnt_gender_cd,
	VALIDATE_STATE_CODE(UPPER(SUBSTR(enc.state, 1, 2)))                                     AS ptnt_state_cd,
    /* ptnt_zip3_cd */
    MASK_ZIP_CODE
        (
            SUBSTR
                (
                    COALESCE
                        (
                            enc.threedigitzip,
                            enc.facility_zip 
                        ), 1, 3
                )
        )                                                                                   AS ptnt_zip3_cd,
	/* enc_start_dt */
	CAP_DATE
	    (
            CAST(EXTRACT_DATE(enc.admit_dt, '%Y-%m-%d') AS DATE),
            esdt.gen_ref_1_dt,
            CAST('{VDR_FILE_DT}' AS DATE)
	    )                                                                                   AS enc_start_dt,
	/* enc_end_dt */
	CAP_DATE
	    (
            CAST(EXTRACT_DATE(enc.disch_dt, '%Y-%m-%d') AS DATE),
            esdt.gen_ref_1_dt,
            CAST('{VDR_FILE_DT}' AS DATE)
	    )                                                                                   AS enc_end_dt,
    enc.facility_id                                                                         AS enc_fclty_id,
    /* enc_fclty_id_qual */
    CASE
        WHEN enc.facility_id IS NULL
            THEN NULL
        ELSE 'RENDERING_FACILITY_VENDOR_ID'
    END                                                                                     AS enc_fclty_id_qual,
    enc.facility_zip                                                                        AS enc_fclty_zip_cd,
    MD5(prv.npi_number)                                                                     AS enc_prov_id,
    /* enc_prov_id_qual */
    CASE
        WHEN prv.npi_number IS NULL
          OR prv.physician_role IS NULL
            THEN NULL
        WHEN SUBSTR(UPPER(COALESCE(prv.physician_role, '')), 1, 9) = 'RENDERING'
            THEN 'RENDERING_PROVIDER_NPI'
        WHEN SUBSTR(UPPER(COALESCE(prv.physician_role, '')), 1, 9) = 'ATTENDING'
            THEN 'ATTENDING_PROVIDER_NPI'
        WHEN SUBSTR(UPPER(COALESCE(prv.physician_role, '')), 1, 9) = 'ADMITTING'
            THEN 'ADMITTING_PROVIDER_NPI'
        WHEN SUBSTR(UPPER(COALESCE(prv.physician_role, '')), 1, 9) = 'OPERATING'
            THEN 'OPERATING_PROVIDER_NPI'
        WHEN SUBSTR(UPPER(COALESCE(prv.physician_role, '')), 1, 9) = 'EMERGENCY'
            THEN 'EMERGENCY_PROVIDER_NPI'
        WHEN SUBSTR(UPPER(COALESCE(prv.physician_role, '')), 1, 5) = 'OTHER'
            THEN 'OTHER_PROVIDER_NPI'
        ELSE NULL
    END                                                                                     AS enc_prov_id_qual,
    enc.patientid                                                                           AS vdr_ptnt_id,
    /* enc_grp_txt */
    CASE
        WHEN COALESCE
                (
                    enc.patient_status_code,
                    enc.patient_status,
                    enc.emergent_status,
                    enc.340b_id
                ) IS NULL
            THEN NULL
        ELSE SUBSTR
                (
                    CONCAT
                        (
                            CASE
                                WHEN COALESCE(enc.patient_status_code, enc.patient_status) IS NULL
                                    THEN ''
                                ELSE CONCAT
                                        (
                                            ' | PATIENT_STATUS: ', 
                                            COALESCE(enc.patient_status_code, ''),
                                            ' - ',
                                            COALESCE(enc.patient_status, '')
                                        )
                            END,
                            CASE
                                WHEN enc.emergent_status IS NULL
                                    THEN ''
                                ELSE CONCAT(' | EMERGENT_STATUS: ', enc.emergent_status)
                            END,
                            CASE
                                WHEN enc.340b_id IS NULL
                                    THEN ''
                                ELSE CONCAT(' | 340B_ID: ', enc.340b_id)
                            END
                        ), 4
                )
	END			                                                                            AS enc_grp_txt,
    enc.admission_source_code                                                               AS admsn_src_std_cd,
    enc.admission_type_code                                                                 AS admsn_typ_std_cd,
	/* bill_typ_std_cd */
	CASE
	    WHEN enc.bill_type IS NULL
	        THEN NULL
	    WHEN SUBSTR(enc.bill_type, 1, 1) = '3'
	        THEN CONCAT('X', SUBSTR(enc.bill_type, 2))
        ELSE enc.bill_type
	END                                                                                     AS bill_typ_std_cd,
    /* payr_grp_txt */
    CASE
        WHEN enc.medicare_provider_number IS NULL
            THEN NULL
        ELSE CONCAT('MEDICARE_PROVIDER_NUMBER: ', enc.medicare_provider_number)
    END                                                                                     AS payr_grp_txt,
    /* data_captr_dt */
	CAP_DATE
	    (
            CAST(EXTRACT_DATE(SUBSTR(enc.imported_on, 1, 10), '%Y-%m-%d') AS DATE),
            esdt.gen_ref_1_dt,
            CAST('{VDR_FILE_DT}' AS DATE)
	    )                                                                                   AS data_captr_dt,
    'claim'                                                                                 AS prmy_src_tbl_nm,
	'150'                                                                                   AS part_hvm_vdr_feed_id,
	/* part_mth */
	CASE
	    WHEN 0 = LENGTH(TRIM(COALESCE(CAP_DATE
                                        (
                                            CAST(EXTRACT_DATE(enc.admit_dt, '%Y-%m-%d') AS DATE), 
                                            COALESCE(ahdt.gen_ref_1_dt, esdt.gen_ref_1_dt),
                                            CAST('{VDR_FILE_DT}' AS DATE)
                                        ), '')))
	        THEN '0_PREDATES_HVM_HISTORY'
	    ELSE SUBSTR(enc.admit_dt, 1, 7)
	END                                                                                     AS part_mth
 FROM sentry_temp18_encounter enc
 LEFT OUTER JOIN sentry_temp19_best_provider prv
              ON COALESCE(enc.enc_id, 'EMPTY') = COALESCE(prv.enc_id, 'DUMMY')
 LEFT OUTER JOIN
    (
        SELECT gen_ref_1_dt
         FROM ref_gen_ref
        WHERE hvm_vdr_feed_id = 150
          AND gen_ref_domn_nm = 'EARLIEST_VALID_SERVICE_DATE'
        LIMIT 1
    ) esdt
   ON 1 = 1
 LEFT OUTER JOIN 
    (
        SELECT gen_ref_1_dt
         FROM ref_gen_ref
        WHERE hvm_vdr_feed_id = 150
          AND gen_ref_domn_nm = 'HVM_AVAILABLE_HISTORY_START_DATE'
        LIMIT 1
    ) ahdt
   ON 1 = 1
