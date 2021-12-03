SELECT 
    --------------------------------------------------------------------------------------------------
    --  hv_lab_test_id
    --------------------------------------------------------------------------------------------------
    CONCAT  ('250', '_', lab.encounter_id, '_', 
            lab.collection_date_time, '_', lab.release_date_time, '_', 
            lab.lab_name, '_', lab.unit_of_measure)                                         AS hv_lab_test_id,
    CURRENT_DATE()                                                                          AS crt_dt,
	'03'                                                                                    AS mdl_vrsn_num,
    SPLIT(lab.input_file_name, '/')[SIZE(SPLIT(lab.input_file_name, '/')) - 1]              AS data_set_nm,
	1816                                                                                    AS hvm_vdr_id,
	250                                                                                     AS hvm_vdr_feed_id,
    lab.client_id                                                                           AS vdr_org_id,
    CONCAT  (lab.encounter_id, '_', lab.collection_date_time, '_', 
            lab.release_date_time, '_', lab.lab_name, '_', 
            lab.unit_of_measure)                                                            AS vdr_lab_test_id,
    'LAB_TEST_ID'                                                                           AS vdr_lab_test_id_qual,
    --------------------------------------------------------------------------------------------------
    --  hvid
    --------------------------------------------------------------------------------------------------
    pay.hvid				                                    							AS hvid,
    
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

	CASE
	    WHEN CAP_DATE
    	    (
                CAST(EXTRACT_DATE(SUBSTR(lab.collection_date_time, 1, 10), '%Y-%m-%d') AS DATE),
                CAST('{EARLIEST_SERVICE_DATE}' AS DATE),
                CAST('{VDR_FILE_DT}' AS DATE)
    	    )																				
    	        IS NULL THEN NULL
        ELSE SUBSTR(lab.collection_date_time, 1, 19)                                              
    END                                                                                     AS lab_test_smpl_collctn_dt,

	CASE
	    WHEN CAP_DATE
    	    (
                CAST(EXTRACT_DATE(SUBSTR(lab.release_date_time, 1, 10), '%Y-%m-%d') AS DATE),
                CAST('{EARLIEST_SERVICE_DATE}' AS DATE),
                CAST('{VDR_FILE_DT}' AS DATE)
    	    )																				
    	        IS NULL THEN NULL
        ELSE SUBSTR(lab.release_date_time, 1, 19)                                              
    END                                                                                     AS lab_result_dt,

    CAST(NULL AS STRING)                                                                    AS lab_test_specmn_typ_cd,
    CAST(NULL AS STRING)                                                                    AS lab_test_vdr_id,
    lab.lab_name                                                                            AS lab_result_nm,
    lab.result                                                                              AS lab_result_msrmt,
    lab.unit_of_measure                                                                     AS lab_result_uom,

    CASE
        WHEN lab.abnormal_flag NOT IN ('Normal', 'NORMAL', 'NORM', 'N') THEN 'Y'
        WHEN lab.abnormal_flag IN ('Normal', 'NORMAL',  'NORM', 'N') THEN 'N'
        ELSE NULL
    END                                                                                     AS lab_result_abnorm_flg,

    lab.reference_range                                                                     AS lab_result_ref_rng_txt,
    'lab'                                                                                   AS prmy_src_tbl_nm,
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

FROM    lab lab
LEFT OUTER JOIN encounter enc
            ON  COALESCE(lab.encounter_id, 'NULL') = COALESCE(enc.encounter_id, 'empty')
            AND COALESCE(lab.client_id, 'NULL')    = COALESCE(enc.client_id, 'empty')
LEFT OUTER JOIN matching_payload pay
            ON  COALESCE(enc.row_id, 'NULL')       = COALESCE(pay.claimid, 'empty')
WHERE UPPER(lab.row_id) <> 'ROWID'
-- LIMIT 10
