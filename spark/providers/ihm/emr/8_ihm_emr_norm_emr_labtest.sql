SELECT 
    MONOTONICALLY_INCREASING_ID()   AS row_id,
    --------------------------------------------------------------------------------------------------
    ---  hv_lab_test_id
    --------------------------------------------------------------------------------------------------
    CASE
        WHEN COALESCE
                (
                    rslt.hospital_id, 
                    rslt.encounter_id, 
                    rslt.test_name, 
                    rslt.name,
                    rslt.result_time
                ) IS NULL
            THEN NULL
        ELSE TRIM(SUBSTR
                (
                    CONCAT
                        ( '210_',
                            CASE
                                WHEN COALESCE(rslt.hospital_id, rslt.encounter_id) IS NULL
                                    THEN ''
                                ELSE CONCAT(COALESCE(rslt.hospital_id, ''), '_', COALESCE(rslt.encounter_id, ''))
                            END,
                            CASE
                                WHEN rslt.test_name IS NULL 
                                    THEN ''
                                ELSE CONCAT('_', rslt.test_name)
                            END,
                            CASE
                                WHEN rslt.name IS NULL
                                    THEN ''
                                ELSE CONCAT('_', rslt.name)
                            END,
                            CASE
                                WHEN CAST(rslt.result_time AS DATE) IS NULL
                                    THEN ''
                                ELSE CONCAT('_', CONCAT(
                                                        SUBSTR(rslt.result_time,  1, 10),' ',
                                                        SUBSTR(rslt.result_time, 12, 08),'.',
                                                        SUBSTR(rslt.result_time, 21, 01)
                                                       )
                                           )
                            END
                                                       
                        ), 1
                ))
    END                                                                                     AS hv_lab_test_id,
    
    CURRENT_DATE()                                                                          AS crt_dt,
	'03'                                                                                    AS mdl_vrsn_num,
    SPLIT(rslt.input_file_name, '/')[SIZE(SPLIT(rslt.input_file_name, '/')) - 1]            AS data_set_nm,
	555                                                                                     AS hvm_vdr_id,
	210                                                                                     AS hvm_vdr_feed_id,
	rslt.hospital_id                                                                         AS vdr_org_id,
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
    --- lab_result_dt
    --------------------------------------------------------------------------------------------------	
    CASE 
        WHEN CAST(rslt.result_time AS DATE)  < CAST('{EARLIEST_SERVICE_DATE}' AS DATE)
          OR CAST(rslt.result_time AS DATE)  > '{VDR_FILE_DT}' THEN NULL
    ELSE     
    COALESCE(
                CONCAT(
                        SUBSTR(rslt.result_time,  1, 10),' ',
                        SUBSTR(rslt.result_time, 12, 08),'.',
                        SUBSTR(rslt.result_time, 21, 01)
                    ),
                CONCAT(
                        SUBSTR(rslt.result_time,  1, 10),' ',
                        SUBSTR(rslt.result_time, 12, 08),'.',
                        SUBSTR(rslt.result_time, 21, 01)
                    )
                )    
    END                                                                                     AS lab_result_dt,
    CLEAN_UP_LOINC_CODE(rslt.test_code)                                                     AS lab_test_loinc_cd,
    rslt.name			                                                                    AS lab_result_nm,
    rslt.results			                                                                AS lab_result_msrmt,
    rslt.units			                                                                    AS lab_result_uom,
    rslt.normal_range		                                                                AS lab_result_ref_rng_txt,
	'patient_test_result'															        AS prmy_src_tbl_nm,
	'210'																			        AS part_hvm_vdr_feed_id,
    --------------------------------------------------------------------------------------------------
    --- part_mth
    --------------------------------------------------------------------------------------------------	

    CASE 
        WHEN 
            (
               CAST(COALESCE(rslt.result_time, pln.start_date, pln.end_date) AS DATE)  < CAST('{AVAILABLE_START_DATE}' AS DATE)
            OR CAST(COALESCE(rslt.result_time, pln.start_date, pln.end_date) AS DATE)  > '{VDR_FILE_DT}'
            )                                                                       THEN '0_PREDATES_HVM_HISTORY'

        WHEN UPPER(pln.status) IN ('IN' , 'INO', 'ER') THEN 
                 CONCAT
                   (
	                SUBSTR(COALESCE(rslt.result_time, pln.start_date), 1, 4), '-',
	                SUBSTR(COALESCE(rslt.result_time, pln.start_date), 6, 2)
                   )
    ELSE  CONCAT
	            (
	                SUBSTR(COALESCE(rslt.result_time, pln.start_date, pln.end_date), 1, 4), '-',
	                SUBSTR(COALESCE(rslt.result_time, pln.start_date, pln.end_date), 6, 2)
                )
    END                                                                         AS part_mth 
    

FROM rslt
LEFT OUTER JOIN pln ON rslt.hospital_id = pln.hospital_id AND  rslt.encounter_id = pln.encounter_id
LEFT OUTER JOIN matching_payload  pay ON pln.hvjoinkey = pay.hvjoinkey
WHERE
    TRIM(UPPER(COALESCE(rslt.hospital_id, 'empty'))) <> 'HOSPITALID'
