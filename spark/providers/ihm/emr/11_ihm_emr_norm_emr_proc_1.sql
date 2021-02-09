SELECT 

    --------------------------------------------------------------------------------------------------
    ---  hv_enc_id
    -- CONCAT('210_', cpt.hospital_id, '_', cpt.encounter_id, '_', cpt.cpt, '_', cpt.seq)
    --------------------------------------------------------------------------------------------------
    CASE
        WHEN COALESCE
                (
                    cpt.hospital_id, 
                    cpt.encounter_id, 
                    cpt.cpt
                ) IS NULL
            THEN NULL
        ELSE TRIM(SUBSTR
                (
                    CONCAT
                        (   '210_',
                            CASE
                                WHEN COALESCE(cpt.hospital_id, cpt.encounter_id) IS NULL
                                    THEN ''
                                ELSE CONCAT(COALESCE(cpt.hospital_id, ''), '_', COALESCE(cpt.encounter_id, ''))
                            END,
                            CASE
                                WHEN cpt.cpt IS NULL OR CLEAN_UP_PROCEDURE_CODE(cpt.cpt) IS NULL
                                    THEN ''
                                ELSE CONCAT('_', cpt.cpt)
                            END,
                            CASE
                                WHEN pln.start_date IS NULL
                                    THEN ''
                                ELSE CONCAT('_', 
                                        CONCAT(
                                                SUBSTR(pln.start_date,  1, 10),' ',
                                                SUBSTR(pln.start_date, 12, 08),'.',
                                                SUBSTR(pln.start_date, 21, 01)
                                               )
                                            )
                            END                            
                        ), 1
                ))
    END                                                                                     AS hv_proc_id,
    
    CURRENT_DATE()                                                                          AS crt_dt,
	'12'                                                                                    AS mdl_vrsn_num,
    SPLIT(cpt.input_file_name, '/')[SIZE(SPLIT(cpt.input_file_name, '/')) - 1]              AS data_set_nm,
	555                                                                                     AS hvm_vdr_id,
	210                                                                                     AS hvm_vdr_feed_id,
	cpt.hospital_id                                                                         AS vdr_org_id,
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
    CAST(NULL AS STRING)                                                                    AS proc_dt, 
    --------------------------------------------------------------------------------------------------
    --- proc_cd and proc_cd_qual
    --------------------------------------------------------------------------------------------------
    CLEAN_UP_PROCEDURE_CODE(UPPER(SUBSTR(cpt.cpt,1,5)))                                     AS proc_cd,
    CASE
        WHEN CLEAN_UP_PROCEDURE_CODE(UPPER(SUBSTR(cpt.cpt,1,5))) IS NOT NULL THEN 'HCPCS'                                                                         
        ELSE NULL
    END                                                                                     AS proc_cd_qual,
	'patient_cpt'															                AS prmy_src_tbl_nm,
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

FROM cpt
LEFT OUTER JOIN  pln ON cpt.hospital_id = pln.hospital_id AND  cpt.encounter_id = pln.encounter_id
LEFT OUTER JOIN matching_payload  pay ON pln.hvjoinkey = pay.hvjoinkey
-- Filter so that valid CPT Codes are returned

WHERE
    TRIM(UPPER(COALESCE(cpt.hospital_id, 'empty'))) <> 'HOSPITALID'
    AND cpt.cpt  IS NOT NULL
