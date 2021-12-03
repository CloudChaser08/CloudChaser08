SELECT 
    --------------------------------------------------------------------------------------------------
    --  hv_diag_id
    --------------------------------------------------------------------------------------------------
    CONCAT('250_', diag.client_id, '_', diag.encounter_id, '_', diag.icd_code)  			AS hv_diag_id,
    CURRENT_DATE()                                                                          AS crt_dt,
	'10'                                                                                    AS mdl_vrsn_num,
    SPLIT(diag.input_file_name, '/')[SIZE(SPLIT(diag.input_file_name, '/')) - 1]            AS data_set_nm,
	1816                                                                                    AS hvm_vdr_id,
	250                                                                                     AS hvm_vdr_feed_id,
    diag.client_id                                                                          AS vdr_org_id,
	CONCAT(diag.client_id, '_', diag.encounter_id, '_', diag.icd_code)						AS vdr_diag_id,

	CASE 
	    WHEN diag.encounter_id IS NOT NULL AND diag.icd_code IS NOT NULL THEN 'DIAGNOSIS_ID' 
	    ELSE NULL 
	END																		                AS vdr_diag_id_qual,

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
    CONCAT('250_', enc.encounter_id)                                                        AS hv_enc_id,

	CASE
	    WHEN CAP_DATE
    	    (
                CAST(EXTRACT_DATE(SUBSTR(enc.admission_date, 1, 10), '%Y-%m-%d') AS DATE),
                CAST('{EARLIEST_SERVICE_DATE}' AS DATE),
                CAST('{VDR_FILE_DT}' AS DATE)
    	    )																				
    	        IS NULL THEN NULL
        ELSE SUBSTR(enc.admission_date, 1, 19)                                              
    END                                                                                     AS enc_dt,

    CLEAN_UP_DIAGNOSIS_CODE(
        diag.icd_code,  
        CASE 
            WHEN diag.icd_revision = '9' THEN '01'
            WHEN diag.icd_revision = '10' THEN '02'
            ELSE NULL
        END,
        EXTRACT_DATE(SUBSTR(enc.admission_date,1, 10), '%Y-%m-%d')
        )                                                                                   AS diag_cd,

    CASE 
        WHEN diag.icd_revision = '9' THEN '01'
        WHEN diag.icd_revision = '10' THEN '02'
        ELSE NULL
    END                                                                                     AS diag_cd_qual,

    'diagnosis'                                                                             AS prmy_src_tbl_nm,
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

FROM    diagnosis diag
LEFT OUTER JOIN encounter enc
            ON  COALESCE(diag.encounter_id, 'NULL') = COALESCE(enc.encounter_id, 'empty')
            AND COALESCE(diag.client_id, 'NULL')    = COALESCE(enc.client_id, 'empty')
LEFT OUTER JOIN matching_payload pay
            ON  COALESCE(enc.row_id, 'NULL')        = COALESCE(pay.claimid, 'empty')
WHERE UPPER(diag.row_id) <> 'ROWID'
--LIMIT 10
