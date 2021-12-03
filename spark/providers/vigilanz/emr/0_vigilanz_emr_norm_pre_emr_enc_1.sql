SELECT
    --------------------------------------------------------------------------------------------------
    --  hv_enc_id
    --------------------------------------------------------------------------------------------------
    CONCAT('250_', enc.client_id, '_', enc.encounter_id)									AS hv_enc_id,
    CURRENT_DATE()                                                                          AS crt_dt,
	'10'                                                                                    AS mdl_vrsn_num,
    SPLIT(enc.input_file_name, '/')[SIZE(SPLIT(enc.input_file_name, '/')) - 1]              AS data_set_nm,
	1816                                                                                    AS hvm_vdr_id,
	250                                                                                     AS hvm_vdr_feed_id,
    enc.client_id                                                                           AS vdr_org_id,
	CONCAT(enc.client_id, '_', enc.encounter_id)                                            AS vdr_enc_id,

	CASE
	    WHEN enc.encounter_id IS NOT NULL THEN 'ENCOUNTER_ID'
	    ELSE NULL
	END																		                AS vdr_enc_id_qual,

    --------------------------------------------------------------------------------------------------
    --  hvid
    --------------------------------------------------------------------------------------------------
    pay.hvid                                    											AS hvid,

	CAP_YEAR_OF_BIRTH  -- Cap year of birth 1927 if age is 85 and over
        (
            VALIDATE_AGE(pay.age, CAST(EXTRACT_DATE(SUBSTR(enc.admission_date, 1, 10), '%Y-%m-%d') AS DATE), pay.yearofbirth),
            CAST(EXTRACT_DATE(SUBSTR(enc.admission_date, 1, 10), '%Y-%m-%d') AS DATE),
            pay.yearofbirth
        )                                                                                   AS ptnt_birth_yr,

    CASE
        WHEN pay.gender IS NULL THEN NULL
        WHEN pay.gender IN ('F', 'M', 'U') THEN pay.gender
        ELSE 'U'
    END                                                                                     AS ptnt_gender_cd,

    --------------------------------------------------------------------------------------------------
    --  ptnt_zip3_cd
    --------------------------------------------------------------------------------------------------
	CAST(MASK_ZIP_CODE(SUBSTR(pay.threedigitzip, 1, 3)) AS STRING)                          AS ptnt_zip3_cd,
       --------------------------------------------------------------------------------------------------
    --  enc_start_dt - Add Capping when date is available - Use CASE Logic for the dates
    --------------------------------------------------------------------------------------------------

	CASE
	    WHEN CAP_DATE
    	    (
                CAST(EXTRACT_DATE(SUBSTR(enc.admission_date, 1, 10), '%Y-%m-%d') AS DATE),
                CAST('{EARLIEST_SERVICE_DATE}' AS DATE),
                CAST('{VDR_FILE_DT}' AS DATE)
    	    )
    	        IS NULL THEN NULL
        ELSE SUBSTR(enc.admission_date, 1, 19)
    END                                                                                     AS enc_start_dt,

	CASE
	    WHEN CAP_DATE
    	    (
                CAST(EXTRACT_DATE(SUBSTR(enc.discharge_date, 1, 10), '%Y-%m-%d') AS DATE),
                CAST('{EARLIEST_SERVICE_DATE}' AS DATE),
                CAST('{VDR_FILE_DT}' AS DATE)
    	    )
    	        IS NULL THEN  NULL
        ELSE SUBSTR(enc.discharge_date, 1, 19)
    END                                                                                     AS enc_end_dt,

    CONCAT('DISCHARGE_DISPOSITION: ', enc.discharge_disposition)                            AS enc_grp_txt,
    'encounter'                                                                             AS prmy_src_tbl_nm,
    '250'                                                                                   AS part_hvm_vdr_feed_id,
    --------------------------------------------------------------------------------------------------
    --  part_mth
    --------------------------------------------------------------------------------------------------
	CASE
	    WHEN CAP_DATE
        	    (
                    CAST(EXTRACT_DATE(SUBSTR(enc.admission_date,1, 10), '%Y-%m-%d') AS DATE),
                    CAST('{AVAILABLE_START_DATE}' AS DATE),
                    CAST('{VDR_FILE_DT}' AS DATE)
        	    )
                    IS NULL THEN '0_PREDATES_HVM_HISTORY'
	    ELSE SUBSTR(enc.admission_date, 1, 7)
	END																					    AS part_mth

FROM    encounter enc
LEFT OUTER JOIN matching_payload pay
            ON  COALESCE(enc.row_id, 'NULL') = COALESCE(pay.claimid, 'empty')
WHERE UPPER(enc.row_id) <> 'ROWID'
-- LIMIT 1
