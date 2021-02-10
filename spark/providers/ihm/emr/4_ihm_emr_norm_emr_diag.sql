SELECT 
    MONOTONICALLY_INCREASING_ID()                                                           AS row_id,
    --------------------------------------------------------------------------------------------------
    ---  hv_enc_id
    -- CONCAT('210_', diag.hospital_id, '_', diag.encounter_id, '_', diag.diagnosis, '_', diag.sequence_number)
    --------------------------------------------------------------------------------------------------
    CASE
        WHEN COALESCE
                (
                    diag.hospital_id, 
                    diag.encounter_id, 
                    diag.diagnosis
                ) IS NULL
            THEN NULL
        ELSE TRIM(SUBSTR
                (
                    CONCAT
                        (  '210_',
                            CASE
                                WHEN COALESCE(diag.hospital_id, diag.encounter_id) IS NULL THEN ''
                                ELSE CONCAT(COALESCE(diag.hospital_id, ''), '_', COALESCE(diag.encounter_id, ''))
                            END,
                            CASE
                                WHEN diag.diagnosis IS NULL  OR         	
                                    CLEAN_UP_DIAGNOSIS_CODE
                                    (
                            	        UPPER(diag.diagnosis),
                            	        '02',
                            	        CAST(COALESCE(pln.start_date, pln.end_date) AS DATE)
            	                    )  IS NULL THEN ''
                                ELSE CONCAT('_', diag.diagnosis)
                            END
                        ), 1
                ))
    END                                                                                     AS hv_diag_id,
    
    CURRENT_DATE()                                                                          AS crt_dt,
	'10'                                                                                    AS mdl_vrsn_num,
    SPLIT(diag.input_file_name, '/')[SIZE(SPLIT(diag.input_file_name, '/')) - 1]            AS data_set_nm,
	555                                                                                     AS hvm_vdr_id,
	210                                                                                     AS hvm_vdr_feed_id,
	diag.hospital_id                                                                        AS vdr_org_id,
    --------------------------------------------------------------------------------------------------
    --- hvid
    --------------------------------------------------------------------------------------------------
	CASE 
	    WHEN 0 <> LENGTH(TRIM(COALESCE(pay.hvid, '')))      THEN pay.hvid
	    WHEN 0 <> LENGTH(TRIM(COALESCE(pay.patientid, ''))) THEN CONCAT('210_', pay.patientid) 
	    ELSE NULL 
	END																				        AS hvid,	
    --------------------------------------------------------------------------------------------------
    --- ptnt_birth_yr
    --------------------------------------------------------------------------------------------------	
	CAST(
	CAP_YEAR_OF_BIRTH
	    (
            pay.age,
            CAST(COALESCE(pln.start_date, pln.end_date) AS DATE), 
            pay.yearofbirth
	    )																					
	    AS INT)                                                                             AS ptnt_birth_yr,
    --------------------------------------------------------------------------------------------------
    --- ptnt_age_num
    --------------------------------------------------------------------------------------------------	
	CAP_AGE
	    (
    	    VALIDATE_AGE
        	    (
                    pay.age, 
                    CAST(COALESCE(pln.start_date, pln.end_date) AS DATE),                   
                    pay.yearofbirth
                )
        )                                                                                  AS ptnt_age_num,
    --------------------------------------------------------------------------------------------------
    --- ptnt_gender_cd
    --------------------------------------------------------------------------------------------------	
	CASE 
	    WHEN SUBSTR(UPPER(pln.sex)   , 1, 1) IN ('F', 'M', 'U')  THEN SUBSTR(UPPER(pln.sex)   , 1, 1)
	    WHEN SUBSTR(UPPER(pay.gender), 1, 1) IN ('F', 'M', 'U')  THEN SUBSTR(UPPER(pay.gender), 1, 1)
	    ELSE NULL
	END																				       AS ptnt_gender_cd,
    --------------------------------------------------------------------------------------------------
    --- ptnt_zip3_cd
    --------------------------------------------------------------------------------------------------	
	MASK_ZIP_CODE(SUBSTR(COALESCE(pay.threedigitzip, '000'), 1, 3))						   AS ptnt_zip3_cd,
	
    --------------------------------------------------------------------------------------------------
    --- hv_enc_id
    --------------------------------------------------------------------------------------------------
    CONCAT('210_', pln.hospital_id, '_', pln.encounter_id)                                  AS hv_enc_id,
    --------------------------------------------------------------------------------------------------
    --- enc_dt
    --------------------------------------------------------------------------------------------------	
    CASE 
        WHEN CAST(COALESCE(pln.start_date, pln.end_date) AS DATE)  < CAST('{EARLIEST_SERVICE_DATE}' AS DATE)
          OR CAST(COALESCE(pln.start_date, pln.end_date) AS DATE)  > '{VDR_FILE_DT}' THEN NULL
        --------------- For Inpatient we could not update NULL start date with end date  
    	WHEN UPPER(pln.status) IN ('IN' , 'INO', 'ER') THEN
                CONCAT(
                        SUBSTR(pln.start_date,  1, 10),' ',
                        SUBSTR(pln.start_date, 12, 08),'.',
                        SUBSTR(pln.start_date, 21, 01)
                    )
    ELSE
        --------------- For Other than Inpatient we could update NULL start date with end date  
        COALESCE(
                CONCAT(
                        SUBSTR(pln.start_date,  1, 10),' ',
                        SUBSTR(pln.start_date, 12, 08),'.',
                        SUBSTR(pln.start_date, 21, 01)
                    ),
                CONCAT(
                        SUBSTR(pln.end_date,  1, 10),' ',
                        SUBSTR(pln.end_date, 12, 08),'.',
                        SUBSTR(pln.end_date, 21, 01)
                    )
                )
    END                                                                                   AS enc_dt,
    --------------------------------------------------------------------------------------------------
    --- diag_cd and diag_cd_qual
    --------------------------------------------------------------------------------------------------
    CASE 
        WHEN diag.diagnosis IS NOT NULL AND LENGTH(diag.diagnosis) < 9 THEN 
        	CLEAN_UP_DIAGNOSIS_CODE
        	    (
        	        UPPER(diag.diagnosis),
        	        '02',
        	        CAST(COALESCE(pln.start_date, pln.end_date) AS DATE)
        	    )                                                                                   
	    ELSE NULL
	END	                                                                                    AS diag_cd,

	CASE
	    WHEN diag.diagnosis IS NOT NULL THEN '02'
        ELSE NULL
	END                                                                                     AS diag_cd_qual,
    --------------------------------------------------------------------------------------------------
    --- diag_nm
    --------------------------------------------------------------------------------------------------
    -- CASE
    --     WHEN UPPER(diag.diagnosis) LIKE 'NONE%' THEN NULL
    --     WHEN LENGTH(diag.diagnosis) > 9 OR  diag.diagnosis rlike '[^0-9]'    THEN  UPPER(diag.diagnosis) 
    -- ELSE NULL END	                                                                        AS diag_nm,
    --------------------------------------------------------------------------------------------------
    --- diag_grp_txt
    --------------------------------------------------------------------------------------------------	
	CASE
	    WHEN  NULLIFY_DRG_BLACKLIST(pln.drug_code) IS NULL THEN NULL
	ELSE CONCAT('DIAGNOSIS_RELATED_GROUP_CODE: ', pln.drug_code)                             
    END                                                                                     AS diag_grp_txt,

	'patient_diagnosis'															            AS prmy_src_tbl_nm,
	'210'																			        AS part_hvm_vdr_feed_id,
    --------------------------------------------------------------------------------------------------
    --- part_mth
    --------------------------------------------------------------------------------------------------	
     CASE 
        WHEN UPPER(pln.status) IN ('IN' , 'INO', 'ER') AND 
            (
              CAST(pln.start_date AS DATE)  < CAST('{AVAILABLE_START_DATE}' AS DATE)
            OR CAST(pln.start_date AS DATE)  > '{VDR_FILE_DT}' 
            )                                                                       THEN '0_PREDATES_HVM_HISTORY'
        WHEN UPPER(pln.status) NOT IN ('IN' , 'INO', 'ER') AND 
            (
              CAST(COALESCE(pln.start_date, pln.end_date) AS DATE)  < CAST('{AVAILABLE_START_DATE}' AS DATE)
            OR CAST(COALESCE(pln.start_date, pln.end_date) AS DATE)  > '{VDR_FILE_DT}'
            )                                                                       THEN '0_PREDATES_HVM_HISTORY'

        WHEN UPPER(pln.status) IN ('IN' , 'INO', 'ER') THEN 
                 CONCAT
                   (
	                SUBSTR(pln.start_date, 1, 4), '-',
	                SUBSTR(pln.start_date, 6, 2)
                   )
    ELSE  CONCAT
	            (
	                SUBSTR(COALESCE(pln.start_date, pln.end_date), 1, 4), '-',
	                SUBSTR(COALESCE(pln.start_date, pln.end_date), 6, 2)
                )
    END                                                                         AS part_mth 

FROM diag
LEFT OUTER JOIN pln ON diag.hospital_id = pln.hospital_id AND  diag.encounter_id = pln.encounter_id
LEFT OUTER JOIN matching_payload  pay ON pln.hvjoinkey = pay.hvjoinkey
WHERE
    TRIM(UPPER(COALESCE(diag.hospital_id, 'empty'))) <> 'HOSPITALID'
    AND diag.diagnosis IS NOT NULL AND LENGTH(diag.diagnosis) < 9
--LIMIT 10
