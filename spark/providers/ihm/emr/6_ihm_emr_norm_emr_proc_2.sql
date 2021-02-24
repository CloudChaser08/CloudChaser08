SELECT 
    --------------------------------------------------------------------------------------------------
    ---  hv_enc_id
    --------------------------------------------------------------------------------------------------
    CASE
        WHEN COALESCE
                (
                    proc.hospital_id, 
                    proc.encounter_id, 
                    proc.procedure_name
                ) IS NULL
            THEN NULL
        ELSE TRIM(SUBSTR
                (
                    CONCAT
                        (   '210_',
                            CASE
                                WHEN COALESCE(proc.hospital_id, proc.encounter_id) IS NULL
                                    THEN ''
                                ELSE CONCAT(COALESCE(proc.hospital_id, ''), '_', COALESCE(proc.encounter_id, ''))
                            END,
                            CASE
                                WHEN proc.procedure_name IS NULL OR CLEAN_UP_PROCEDURE_CODE(UPPER(proc.procedure_name)) IS NULL
                                    THEN ''
                                ELSE CONCAT('_', proc.procedure_name)
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
    SPLIT(proc.input_file_name, '/')[SIZE(SPLIT(proc.input_file_name, '/')) - 1]            AS data_set_nm,
	555                                                                                     AS hvm_vdr_id,
	210                                                                                     AS hvm_vdr_feed_id,
	proc.hospital_id                                                                         AS vdr_org_id,
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
          OR CAST(COALESCE(pln.start_date, pln.end_date) AS DATE)  > CAST('{VDR_FILE_DT}' AS DATE) THEN NULL
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
    --- proc_dt
    --------------------------------------------------------------------------------------------------	
    CASE 
        WHEN CAST(proc.date_performed AS DATE)  < CAST('{EARLIEST_SERVICE_DATE}' AS DATE)
          OR CAST(proc.date_performed AS DATE)  > CAST('{VDR_FILE_DT}' AS DATE) THEN NULL
    ELSE                     
    CONCAT(
            SUBSTR(date_performed,  1, 10),' ',
            SUBSTR(date_performed, 12, 08),'.',
            SUBSTR(date_performed, 21, 01)
            )
    END                                                                                     AS proc_dt,    
    --------------------------------------------------------------------------------------------------
    --- proc_cd and proc_cd_qual
    --------------------------------------------------------------------------------------------------
    CLEAN_UP_PROCEDURE_CODE(UPPER(proc.procedure_name))                                     AS proc_cd,
    CASE
        WHEN proc.procedure_name IS NOT NULL  THEN 'ICDCODE'
    ELSE NULL END	                                                                        AS proc_cd_qual,
	'patient_procedure'															            AS prmy_src_tbl_nm,
	'210'																			        AS part_hvm_vdr_feed_id,
    --------------------------------------------------------------------------------------------------
    --- part_mth
    --------------------------------------------------------------------------------------------------
    CASE 
        WHEN 
            (
              CAST(COALESCE(proc.date_performed, pln.start_date, pln.end_date) AS DATE)  < CAST('{AVAILABLE_START_DATE}' AS DATE)
            OR CAST(COALESCE(proc.date_performed, pln.start_date, pln.end_date) AS DATE)  > CAST('{VDR_FILE_DT}' AS DATE)'
            )                                                                       THEN '0_PREDATES_HVM_HISTORY'

        WHEN UPPER(pln.status) IN ('IN' , 'INO', 'ER') THEN 
                 CONCAT
                   (
	                SUBSTR(COALESCE(proc.date_performed, pln.start_date), 1, 4), '-',
	                SUBSTR(COALESCE(proc.date_performed, pln.start_date), 6, 2)
                   )
    ELSE  CONCAT
	            (
	                SUBSTR(COALESCE(proc.date_performed, pln.start_date, pln.end_date), 1, 4), '-',
	                SUBSTR(COALESCE(proc.date_performed, pln.start_date, pln.end_date), 6, 2)
                )
    END                                                                         AS part_mth 
    

FROM proc
LEFT OUTER JOIN pln ON proc.hospital_id = pln.hospital_id AND  proc.encounter_id = pln.encounter_id
LEFT OUTER JOIN matching_payload  pay ON pln.hvjoinkey = pay.hvjoinkey
WHERE
    TRIM(UPPER(COALESCE(proc.hospital_id, 'empty'))) <> 'HOSPITALID'
    AND proc.procedure_name IS NOT NULL

