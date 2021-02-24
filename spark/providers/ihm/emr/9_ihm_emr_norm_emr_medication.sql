SELECT 
    MONOTONICALLY_INCREASING_ID()   AS row_id,
    --------------------------------------------------------------------------------------------------
    ---  CONCAT(210, '_', drug.hospital_id, '_', drug.encounter_id,  '_', drug.mnemonic, '_', drug.ndc_number, '_', CAST(drug.start_date AS DATE), '_', CAST(drug.date_given AS DATE)) 
    --------------------------------------------------------------------------------------------------
    CASE
        WHEN COALESCE
                (
                    drug.hospital_id, 
                    drug.encounter_id, 
                    drug.mnemonic,
                    drug.ndc_number,
                    drug.start_date,
                    drug.date_given 
                ) IS NULL
            THEN NULL
        ELSE TRIM(SUBSTR
                (
                    CONCAT
                        (   '210_',
                            CASE
                                WHEN COALESCE(drug.hospital_id, drug.encounter_id) IS NULL
                                    THEN ''
                                ELSE CONCAT(COALESCE(drug.hospital_id, ''), '_', COALESCE(drug.encounter_id, ''))
                            END,
                            CASE
                                WHEN drug.mnemonic IS NULL
                                    THEN ''
                                ELSE CONCAT('_', drug.mnemonic)
                            END,
                            CASE
                                WHEN drug.ndc_number IS NULL
                                    THEN ''
                                ELSE CONCAT('_', drug.ndc_number)
                            END,
                            CASE
                                WHEN drug.start_date IS NULL
                                    THEN ''
                                ELSE CONCAT('_', 
                                        CONCAT(
                                                SUBSTR(drug.start_date,  1, 10),' ',
                                                SUBSTR(drug.start_date, 12, 08),'.',
                                                SUBSTR(drug.start_date, 21, 01)
                                               )
                                            )
                            END,
                            CASE
                                WHEN drug.date_given IS NULL
                                    THEN ''
                                ELSE CONCAT('_', 
                                                CONCAT(
                                                        SUBSTR(drug.date_given,  1, 10),' ',
                                                        SUBSTR(drug.date_given, 12, 08),'.',
                                                        SUBSTR(drug.date_given, 21, 01)
                                                      )
                                            )
                            END
                        ), 1
                ))
    END                                                                                     AS hv_medctn_id,
    
    CURRENT_DATE()                                                                          AS crt_dt,
	'11'                                                                                    AS mdl_vrsn_num,
    SPLIT(drug.input_file_name, '/')[SIZE(SPLIT(drug.input_file_name, '/')) - 1]            AS data_set_nm,
	555                                                                                     AS hvm_vdr_id,
	210                                                                                     AS hvm_vdr_feed_id,
	drug.hospital_id                                                                         AS vdr_org_id,
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
    --- medctn_ord_dt
    --------------------------------------------------------------------------------------------------	
    CASE 
        WHEN CAST(drug.start_date AS DATE)  < CAST('{EARLIEST_SERVICE_DATE}' AS DATE)
          OR CAST(drug.start_date AS DATE)  > CAST('{VDR_FILE_DT}' AS DATE) THEN NULL
    ELSE     
                CONCAT(
                        SUBSTR(drug.start_date,  1, 10),' ',
                        SUBSTR(drug.start_date, 12, 08),'.',
                        SUBSTR(drug.start_date, 21, 01)
                    )
    END                                                                                     AS medctn_ord_dt,
     --------------------------------------------------------------------------------------------------
    --- medctn_admin_dt
    --------------------------------------------------------------------------------------------------	
    CASE 
        WHEN CAST(drug.date_given AS DATE)  < CAST('{EARLIEST_SERVICE_DATE}' AS DATE)
          OR CAST(drug.date_given AS DATE)  > CAST('{VDR_FILE_DT}' AS DATE) THEN NULL
    ELSE     
                CONCAT(
                        SUBSTR(drug.date_given,  1, 10),' ',
                        SUBSTR(drug.date_given, 12, 08),'.',
                        SUBSTR(drug.date_given, 21, 01)
                    )
    
    END                                                                                     AS medctn_admin_dt,
    
    CLEAN_UP_NDC_CODE(drug.ndc_number)                                                      AS medctn_ndc,
    drug.mnemonic		                                                                    AS medctn_brd_nm,
    drug.meds			                                                                    AS medctn_genc_nm,
    drug.doses			                                                                    AS medctn_dose_txt,
    drug.units			                                                                    AS medctn_dose_uom,
    drug.route			                                                                    AS medctn_admin_rte_txt,
    
	'patient_drug'															                AS prmy_src_tbl_nm,
	'210'																			        AS part_hvm_vdr_feed_id,
    --------------------------------------------------------------------------------------------------
    --- part_mth
    --------------------------------------------------------------------------------------------------
    CASE 
        WHEN 
            (
             CAST(COALESCE(drug.start_date, drug.date_given, pln.start_date, pln.end_date) AS DATE)  < CAST('{AVAILABLE_START_DATE}' AS DATE)
          OR CAST(COALESCE(drug.start_date, drug.date_given, pln.start_date, pln.end_date) AS DATE)  > CAST('{VDR_FILE_DT}' AS DATE)
            )                                                                       THEN '0_PREDATES_HVM_HISTORY'

        WHEN UPPER(pln.status) IN ('IN' , 'INO', 'ER') THEN 
                 CONCAT
                   (
	                SUBSTR(COALESCE(drug.start_date, drug.date_given, pln.start_date), 1, 4), '-',
	                SUBSTR(COALESCE(drug.start_date, drug.date_given, pln.start_date), 6, 2)
                   )
    ELSE  CONCAT
	            (
	                SUBSTR(COALESCE(drug.start_date, drug.date_given, pln.start_date, pln.end_date), 1, 4), '-',
	                SUBSTR(COALESCE(drug.start_date, drug.date_given, pln.start_date, pln.end_date), 6, 2)
                )
    END                                                                         AS part_mth 
    
 
FROM drug
LEFT OUTER JOIN pln ON drug.hospital_id = pln.hospital_id AND  drug.encounter_id = pln.encounter_id
LEFT OUTER JOIN matching_payload  pay ON pln.hvjoinkey = pay.hvjoinkey
WHERE
    TRIM(UPPER(COALESCE(drug.hospital_id, 'empty'))) <> 'HOSPITALID'

--LIMIT 10
