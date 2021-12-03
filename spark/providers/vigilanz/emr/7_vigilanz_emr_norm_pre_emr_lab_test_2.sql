SELECT 
    --------------------------------------------------------------------------------------------------
    --  hv_lab_test_id
    --------------------------------------------------------------------------------------------------
    CONCAT  ('250_', mic.client_id, '_', mic.encounter_id, 
            '_', mic.collection_date_time, '_', mic.micro_detail_id, 
            '_', mic.antibiotic, '_', mic.dilution)                                         AS hv_lab_test_id,
    CURRENT_DATE()                                                                          AS crt_dt,
	'03'                                                                                    AS mdl_vrsn_num,
    SPLIT(mic.input_file_name, '/')[SIZE(SPLIT(mic.input_file_name, '/')) - 1]              AS data_set_nm,
	1816                                                                                    AS hvm_vdr_id,
	250                                                                                     AS hvm_vdr_feed_id,
    mic.client_id                                                                           AS vdr_org_id,
    CONCAT  (mic.client_id, '_', mic.encounter_id,
            '_', mic.collection_date_time, '_', mic.micro_detail_id,
            '_', mic.antibiotic, '_', mic.dilution)                                         AS vdr_lab_test_id,
    'LAB_TEST_ID'                                                                           AS vdr_lab_test_id_qual,    
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

	CASE
	    WHEN CAP_DATE
    	    (
                CAST(EXTRACT_DATE(SUBSTR(mic.collection_date_time, 1, 10), '%Y-%m-%d') AS DATE),
                CAST('{EARLIEST_SERVICE_DATE}' AS DATE),
                CAST('{VDR_FILE_DT}' AS DATE)
    	    )																				
    	        IS NULL THEN NULL
        ELSE SUBSTR(mic.collection_date_time, 1, 19)                                              
    END                                                                                     AS lab_test_smpl_collctn_dt,

	CASE
	    WHEN CAP_DATE
    	    (
                CAST(EXTRACT_DATE(SUBSTR(mic.release_date_time, 1, 10), '%Y-%m-%d') AS DATE),
                CAST('{EARLIEST_SERVICE_DATE}' AS DATE),
                CAST('{VDR_FILE_DT}' AS DATE)
    	    )																				
    	        IS NULL THEN NULL
        ELSE SUBSTR(mic.release_date_time, 1, 19)                                              
    END                                                                                     AS lab_result_dt,



	mic.organism                                                                            AS lab_test_specmn_typ_cd,
	    
    mic.micro_detail_id                                                                     AS lab_test_vdr_id,	    
    mic.sensitivity                                                                         AS lab_result_nm,
    mic.dilution                                                                            AS lab_result_msrmt,
    CAST(NULL AS STRING)                                                                    AS lab_result_uom,
    CAST(NULL AS STRING)                                                                    AS lab_result_abnorm_flg,
    CAST(NULL AS STRING)                                                                    AS lab_result_ref_rng_txt,
    'microbiology'                                                                          AS prmy_src_tbl_nm,
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

FROM    microbiology mic
LEFT OUTER JOIN encounter enc
            ON  COALESCE(mic.encounter_id, 'NULL') = COALESCE(enc.encounter_id, 'empty')
            AND COALESCE(mic.client_id, 'NULL')    = COALESCE(enc.client_id, 'empty')
LEFT OUTER JOIN matching_payload pay
            ON  COALESCE(enc.row_id, 'NULL')       = COALESCE(pay.claimid, 'empty')
WHERE UPPER(mic.row_id) <> 'ROWID'
-- LIMIT 1
