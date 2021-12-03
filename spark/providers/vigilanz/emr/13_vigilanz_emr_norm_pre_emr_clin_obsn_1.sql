SELECT 
    --------------------------------------------------------------------------------------------------
    --  hv_clin_obsn_id
    --------------------------------------------------------------------------------------------------
    CONCAT  ('250', '_', vit.client_id, '_', vit.encounter_id, 
            '_', vit.collection_date_time, '_', vit.release_date_time, 
            '_', vit.vital_name, '_', COALESCE(vit.unit_of_measure, 'UoM not found'))       AS hv_clin_obsn_id,
    CURRENT_DATE()                                                                          AS crt_dt,
	'09'                                                                                    AS mdl_vrsn_num,
    SPLIT(vit.input_file_name, '/')[SIZE(SPLIT(vit.input_file_name, '/')) - 1]              AS data_set_nm,
	1816                                                                                    AS hvm_vdr_id,
	250                                                                                     AS hvm_vdr_feed_id,
    vit.client_id                                                                           AS vdr_org_id,
    CONCAT  (vit.client_id, '_', vit.encounter_id, 
            '_', vit.collection_date_time, '_', vit.release_date_time, 
            '_', vit.vital_name, '_', COALESCE(vit.unit_of_measure, 'UoM not found'))       AS vdr_clin_obsn_id,
    'CLINICAL_OBSERVATION_ID'                                                               AS vdr_clin_obsn_id_qual,
    --------------------------------------------------------------------------------------------------
    --  hvid
    --------------------------------------------------------------------------------------------------
--     COALESCE(pay.hvid, CONCAT('250_', pay.claimid))											AS hvid,
    pay.hvid                                            										AS hvid,
/*
    CASE 
        WHEN (YEAR(COALESCE(pay.yearofbirth, '{VDR_FILE_DT}', '1900-01-01')) - pay.yearofbirth) > 84 THEN 1927 
        ELSE pay.yearofbirth 
    END                                                                                     AS ptnt_birth_yr,
*/

	CAP_YEAR_OF_BIRTH  -- Cap year of birth 1927 if age is 85 and over
        (
            VALIDATE_AGE(pay.age, CAST(EXTRACT_DATE(enc.admission_date, '%Y%m%d') AS DATE), pay.yearofbirth),
            CAST(EXTRACT_DATE(enc.admission_date, '%Y%m%d') AS DATE),
            pay.yearofbirth
        )                                                                                    AS ptnt_birth_yr,

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

	CASE
	    WHEN CAP_DATE
    	    (
                CAST(EXTRACT_DATE(SUBSTR(vit.collection_date_time, 1, 10), '%Y-%m-%d') AS DATE),
                CAST('{EARLIEST_SERVICE_DATE}' AS DATE),
                CAST('{VDR_FILE_DT}' AS DATE)
    	    )																				
    	        IS NULL THEN NULL
        ELSE SUBSTR(vit.collection_date_time, 1, 19)                                              
    END                                                                                     AS clin_obsn_onset_dt,

	CASE
	    WHEN CAP_DATE
    	    (
                CAST(EXTRACT_DATE(SUBSTR(vit.release_date_time, 1, 10), '%Y-%m-%d') AS DATE),
                CAST('{EARLIEST_SERVICE_DATE}' AS DATE),
                CAST('{VDR_FILE_DT}' AS DATE)
    	    )																				
    	        IS NULL THEN NULL
        ELSE SUBSTR(vit.release_date_time, 1, 19)                                              
    END                                                                                     AS clin_obsn_resltn_dt,

    vit.vital_name                                                                          AS clin_obsn_typ_cd,
    vit.result                                                                              AS clin_obsn_msrmt,
    vit.unit_of_measure                                                                     AS clin_obsn_uom,
    
    CASE
        WHEN vit.abnormal_flag IN ('N') THEN 'N'
        WHEN vit.abnormal_flag IN ('Y', 'L', 'LL', 'H', 'HH') THEN 'Y'
        ELSE NULL
    END                                                                                     AS clin_obsn_abnorm_flg,
   
    SPLIT(vit.reference_range, '-')[0]                                                      AS clin_obsn_norm_min_msrmt,
    SPLIT(vit.reference_range, '-')[1]                                                      clin_obsn_norm_max_msrmt,
    'vital'                                                                                 AS prmy_src_tbl_nm,
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

FROM    vital vit
LEFT OUTER JOIN encounter enc
            ON  COALESCE(vit.encounter_id, 'NULL') = COALESCE(enc.encounter_id, 'empty')
            AND COALESCE(vit.client_id, 'NULL')    = COALESCE(enc.client_id, 'empty')
LEFT OUTER JOIN matching_payload pay
            ON  COALESCE(enc.row_id, 'NULL')       = COALESCE(pay.claimid, 'empty')
WHERE UPPER(vit.vital_name) <> 'VITALNAME'
