SELECT
--    MONOTONICALLY_INCREASING_ID()                                                           AS row_id,
    --------------------------------------------------------------------------------------------------
    ---  hv_enc_id
    -- CONCAT('210_', pln.hospital_id, '_', pln.encounter_id)
    --------------------------------------------------------------------------------------------------
    CASE 
        WHEN COALESCE(pln.hospital_id, pln.encounter_id) IS NOT NULL
            THEN CONCAT
                    (
                        '210_',
                        COALESCE(pln.hospital_id, 'UNAVAILABLE'),
                        '_',
                        COALESCE(pln.encounter_id, 'UNAVAILABLE')
                    )
        ELSE NULL
    END                                                                                     AS hv_enc_id,
    CURRENT_DATE()                                                                          AS crt_dt,
	'10'                                                                                    AS mdl_vrsn_num,
    SPLIT(pln.input_file_name, '/')[SIZE(SPLIT(pln.input_file_name, '/')) - 1]              AS data_set_nm,
	555                                                                                     AS hvm_vdr_id,
	210                                                                                     AS hvm_vdr_feed_id,
	pln.hospital_id                                                                         AS vdr_org_id,
    --------------------------------------------------------------------------------------------------
    --- vdr_enc_id
    --------------------------------------------------------------------------------------------------
	CONCAT(pln.hospital_id, '_', pln.encounter_id)                                          AS vdr_enc_id,
	CASE
	    WHEN COALESCE(pln.hospital_id, pln.encounter_id) IS NOT NULL THEN 'ENCOUNTER_ID'
        ELSE NULL
	END                                                                                     AS vdr_enc_id_qual,
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
    --- enc_start_dt
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
    END                                                                                   AS enc_start_dt,
    --------------------------------------------------------------------------------------------------
    --- enc_end_dt
    --------------------------------------------------------------------------------------------------	
    CASE 
        WHEN CAST(COALESCE(pln.end_date, pln.start_date) AS DATE)  < CAST('{EARLIEST_SERVICE_DATE}' AS DATE)
          OR CAST(COALESCE(pln.end_date, pln.start_date) AS DATE)  > '{VDR_FILE_DT}'  THEN NULL
        --------------- For Inpatient we could not update NULL end date with start date  
     	WHEN UPPER(pln.status) IN ('IN' , 'INO', 'ER') THEN
                CONCAT(
                        SUBSTR(pln.end_date,  1, 10),' ',
                        SUBSTR(pln.end_date, 12, 08),'.',
                        SUBSTR(pln.end_date, 21, 01)
                    )
    ELSE
        --------------- For Other than Inpatient we could update NULL end date with start date  
        COALESCE(
                CONCAT(
                        SUBSTR(pln.end_date,  1, 10),' ',
                        SUBSTR(pln.end_date, 12, 08),'.',
                        SUBSTR(pln.end_date, 21, 01)
                    ),
                CONCAT(
                        SUBSTR(pln.start_date,  1, 10),' ',
                        SUBSTR(pln.start_date, 12, 08),'.',
                        SUBSTR(pln.start_date, 21, 01)
                    )
                )
    END                                                                                  AS enc_end_dt, 
    --------------------------------------------------------------------------------------------------
    --- enc_typ_nm  
    --------------------------------------------------------------------------------------------------	
    CASE 
        WHEN UPPER(pln.status) = 'IN'                                THEN 'INPATIENT' 
        WHEN UPPER(pln.status) = 'INO'                               THEN 'INPATIENT OBSERVATION'
        WHEN UPPER(pln.status) = 'ER'                                THEN 'EMERGENCY ROOM' 
        WHEN UPPER(pln.status) IN ('SDC', 'SDCO')                    THEN 'DAY SURGERY'
        WHEN UPPER(pln.status) IN ('CLI', 'REF', 'RCR', 'POV', 'NB') THEN 'OUTPATIENT' 
        ELSE NULL
    END                                                                                     AS enc_typ_nm,
    --------------------------------------------------------------------------------------------------
    --- enc_grp_txt
    --------------------------------------------------------------------------------------------------
          CASE
            WHEN COALESCE
                    (  
                        ins.group_name, 
                        pln.discharge_code
                    ) IS NULL
                THEN NULL
            ELSE TRIM(
            SUBSTR(
                        CONCAT
                            (  
                                CASE
                                    WHEN ins.sequence_number  IS NULL OR  ins.group_name  IS NULL  THEN ''
                                    ELSE CONCAT( '| INSURANCE_SEQUENCE_NUMBER: ', ins.sequence_number )
                                END,
                                CASE
                                    WHEN ins.group_name  IS NULL THEN ''
                                    ELSE CONCAT(' | INSURANCE_GROUP_NAME: ', ins.group_name)
                                END,
                                CASE
                                    WHEN SCRUB_DISCHARGE_STATUS(pln.discharge_code)  IS NULL THEN ''
                                    ELSE CONCAT(' | DISCHARGE_CODE: ', LPAD( pln.discharge_code, 2, '0'))
                                END
                            ), 3
                    ))
            END                                                                                 

        
        AS enc_grp_txt,
    
	'patient_hospital_encounter'															AS prmy_src_tbl_nm,
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
FROM pln
LEFT OUTER JOIN matching_payload pay ON pln.hvjoinkey   = pay.hvjoinkey
LEFT OUTER JOIN ins ON pln.encounter_id = ins.encounter_id AND pln.hospital_id = ins.hospital_id

