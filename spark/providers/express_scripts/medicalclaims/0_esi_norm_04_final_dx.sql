SELECT
    MONOTONICALLY_INCREASING_ID()                                                           AS record_id,
    COALESCE(dx_pay.privateidone, 'UNAVAILABLE')                                            AS claim_id,    
    CASE 
        WHEN rx_pay.hvid IS NOT NULL 
            THEN rx_pay.hvid 
        ELSE CONCAT('17_', COALESCE(dx_pay.patientid, 'UNAVAILABLE')) 
    END                                                                                     AS hvid, 
    CURRENT_DATE()                                                                          AS created, 
	'09'                                                                                    AS model_version, 
    SPLIT(txn.input_file_name, '/')[SIZE(SPLIT(txn.input_file_name, '/')) - 1]              AS data_set, 
	'155'                                                                                   AS data_feed, 
	'17'                                                                                    AS data_vendor, 
	/* patient_gender */
	CLEAN_UP_GENDER
    	(
        	CASE
        	    WHEN SUBSTR(UPPER(rx_pay.gender), 1, 1) IN ('F', 'M') THEN SUBSTR(UPPER(rx_pay.gender), 1, 1)
        	    ELSE 'U' 
        	END
	    )                                                                                   AS patient_gender, 
	/* patient_year_of_birth */
	cap_year_of_birth
        (
            NULL,
            CAST(EXTRACT_DATE(txn.first_serviced_date, '%Y%m%d') AS DATE), 
            rx_pay.year_of_birth
        )                                                                                   AS patient_year_of_birth, 
    /* patient_zip3 */
    MASK_ZIP_CODE(SUBSTR(rx_pay.zip, 1, 3))                                       AS patient_zip3,
    /* date_service */
	CAP_DATE
        (
            CAST(EXTRACT_DATE(txn.first_serviced_date, '%Y%m%d') AS DATE), 
            esdt.gen_ref_1_dt, 
            CAST(EXTRACT_DATE('{VDR_FILE_DT}', '%Y-%m-%d') AS DATE)
        )                                                                                   AS date_service, 
    /* date_service_end */
	CAP_DATE
        (
            CAST(EXTRACT_DATE(txn.last_serviced_date, '%Y%m%d') AS DATE), 
            esdt.gen_ref_1_dt, 
            CAST(EXTRACT_DATE('{VDR_FILE_DT}', '%Y-%m-%d') AS DATE)
        )                                                                                   AS date_service_end, 
   /* inst_discharge_status_std_id */
    CASE
        WHEN COALESCE(txn.patient_status_code, '') IN ('20', '21', '40', '41', '42', '69', '87') 
            THEN NULL
        ELSE txn.patient_status_code
    END                                                                                     AS inst_discharge_status_std_id,    
    /* place_of_service_std_id */

    CASE 
        WHEN txn.place_of_service_code IS NULL                                                                      
            THEN NULL 
        WHEN LENGTH(txn.place_of_service_code) = 3 
         AND SUBSTR(txn.place_of_service_code, 1, 1) ='0' 
         AND SUBSTR(txn.place_of_service_code, 2, 2) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33')   
            THEN '99' 
        WHEN LENGTH(txn.place_of_service_code) < 3 
         AND LPAD(txn.place_of_service_code, 2, 0) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33')     
            THEN '99'                 
        WHEN LENGTH(txn.place_of_service_code) = 3 
         AND SUBSTR(txn.place_of_service_code, 1, 1) ='0'                                                          
            THEN SUBSTR(txn.place_of_service_code, 2, 2) 
        ELSE LPAD(txn.place_of_service_code, 2, 0) 
    END	                                                                                    AS place_of_service_std_id,    
    ------------------ diagnosis
    /* diagnosis_code */
    CASE
        WHEN LPAD(txn.medical_qualifier_code, 2, 0 ) IN ('01', '02')
            THEN CLEAN_UP_DIAGNOSIS_CODE
                    (
                        txn.medical_code, 
                        txn.medical_qualifier_code, 
                        CAST(EXTRACT_DATE(COALESCE(txn.last_serviced_date, txn.first_serviced_date), '%Y%m%d') AS DATE)
                    )
     ELSE CAST(NULL AS STRING)
    END                                                                                     AS diagnosis_code, 
    /* diagnosis_code_qual */
    CASE
        WHEN LPAD(txn.medical_qualifier_code, 2, 0 ) IN ('01', '02') 
            AND txn.medical_code IS NOT NULL 
            THEN txn.medical_qualifier_code
    ELSE CAST(NULL AS STRING)
    END                                                                                     AS diagnosis_code_qual,   

    CASE
        WHEN LPAD(txn.medical_qualifier_code, 2, 0 ) IN ('01', '02') 
            AND txn.medical_code IS NOT NULL AND UPPER(txn.record_type) = 'PDX' 
            THEN '1' 
    ELSE CAST(NULL AS STRING) 
    END                                                                                     AS diagnosis_priority, 
    ---------------------- Procedure
    CASE
        WHEN COALESCE(txn.medical_qualifier_code, '') IN ('CP', 'HC')   
            THEN CLEAN_UP_PROCEDURE_CODE(txn.medical_code)
    ELSE NULL
    END                                                                                     AS procedure_code, 
    /* procedure_code_qual */  
    CASE
        WHEN COALESCE(txn.medical_qualifier_code, '') IN ('CP', 'HC')
            AND txn.medical_code IS NOT NULL
            THEN txn.medical_qualifier_code
    ELSE CAST(NULL AS STRING)
    END                                                                                     AS procedure_code_qual, 
    /* procedure_modifier_1 */      
    CASE
        WHEN COALESCE(txn.medical_qualifier_code, '') IN ('CP', 'HC')
         AND txn.cpt_modifier_code_01 IS NOT NULL
            THEN CLEAN_UP_ALPHANUMERIC_CODE(SUBSTR(UPPER(txn.cpt_modifier_code_01), 1, 2))
    ELSE CAST(NULL AS STRING)
    END                                                                                     AS procedure_modifier_1, 
    /* procedure_modifier_2 */      
    CASE
        WHEN COALESCE(txn.medical_qualifier_code, '') IN ('CP', 'HC')
         AND txn.cpt_modifier_code_02 IS NOT NULL
            THEN CLEAN_UP_ALPHANUMERIC_CODE(SUBSTR(UPPER(txn.cpt_modifier_code_02), 1, 2))
    ELSE CAST(NULL AS STRING)
    END                                                                                     AS procedure_modifier_2, 
 
    /* procedure_modifier_3 */      
    CASE
        WHEN COALESCE(txn.medical_qualifier_code, '') IN ('CP', 'HC')
         AND txn.cpt_modifier_code_03 IS NOT NULL
            THEN CLEAN_UP_ALPHANUMERIC_CODE(SUBSTR(UPPER(txn.cpt_modifier_code_03), 1, 2))
    ELSE CAST(NULL AS STRING)
    END                                                                                     AS procedure_modifier_3, 
    /* procedure_modifier_4 */      
    CASE
        WHEN COALESCE(txn.medical_qualifier_code, '') IN ('CP', 'HC')
         AND txn.cpt_modifier_code_04 IS NOT NULL
            THEN CLEAN_UP_ALPHANUMERIC_CODE(SUBSTR(UPPER(txn.cpt_modifier_code_04), 1, 2))
    ELSE CAST(NULL AS STRING)
    END                                                                                     AS procedure_modifier_4,     
    ---------------------- Revenue Code
    CASE
        WHEN UPPER(COALESCE(txn.medical_qualifier_code, '')) = 'RV'   
            THEN CLEAN_UP_PROCEDURE_CODE(txn.medical_code)
    ELSE CAST(NULL AS STRING)
    END                                                                                     AS revenue_code, 
    ----------------------
    /* prov_billing_npi */
    CLEAN_UP_NPI_CODE
    (
    CASE
        WHEN RIGHT(CONCAT('00', COALESCE(txn.place_of_service_code, '99')), 2)
                IN ('05', '06', '07', '08', '09', 
                    '12', '13', '14', '33', '99')
            THEN NULL
        ELSE txn.provider_npi_number  
    END                                                                                     
    )                                                                                       AS prov_billing_npi, 
    /* prov_billing_name_1 */    
    CASE
        WHEN RIGHT(CONCAT('00', COALESCE(txn.place_of_service_code, '99')), 2)
                IN ('05', '06', '07', '08', '09', 
                    '12', '13', '14', '33', '99')
            THEN NULL
        WHEN COALESCE
                (
                    txn.provider_first_name, 
                    txn.provider_middle_name, 
                    txn.provider_last_name, 
                    txn.provider_name_suffix_text, 
                    txn.professional_title_text
                ) IS NULL
            THEN NULL
        ELSE SUBSTR
                (
                    CONCAT
                        (
                            CASE
                                WHEN txn.provider_last_name IS NULL 
                                    THEN ''
                                ELSE CONCAT(', ', txn.provider_last_name)
                            END, 
                            CASE
                                WHEN txn.provider_first_name IS NULL 
                                    THEN ''
                                ELSE CONCAT(', ', txn.provider_first_name)
                            END, 
                            CASE
                                WHEN txn.provider_middle_name IS NULL 
                                    THEN ''
                                ELSE CONCAT(', ', txn.provider_middle_name)
                            END, 
                            CASE
                                WHEN txn.provider_name_suffix_text IS NULL 
                                    THEN ''
                                ELSE CONCAT(', ', txn.provider_name_suffix_text)
                            END, 
                            CASE
                                WHEN txn.professional_title_text IS NULL 
                                    THEN ''
                                -- DO NOT add the professional title if it is already present in the last name
                                WHEN LOCATE(txn.professional_title_text, RIGHT(txn.provider_last_name,LENGTH(txn.professional_title_text))) > 0
                                    THEN ''                                    
                                ELSE CONCAT(', ', txn.professional_title_text)
                            END
                        ), 3
                )
    END                                                                                     AS prov_billing_name_1, 
    /* prov_billing_address_1 */      
    CASE
        WHEN RIGHT(CONCAT('00', COALESCE(txn.place_of_service_code, '99')), 2)
                IN ('05', '06', '07', '08', '09', 
                    '12', '13', '14', '33', '99')
            THEN NULL
        ELSE txn.provider_street_addr_line_1  
    END                                                                                     AS prov_billing_address_1, 
    /* prov_billing_address_2 */       
    CASE
        WHEN RIGHT(CONCAT('00', COALESCE(txn.place_of_service_code, '99')), 2)
                IN ('05', '06', '07', '08', '09', 
                    '12', '13', '14', '33', '99')
            THEN NULL
        ELSE txn.provider_street_addr_line_2  
    END                                                                                     AS prov_billing_address_2,       
    /* prov_billing_city */   
    CASE
        WHEN RIGHT(CONCAT('00', COALESCE(txn.place_of_service_code, '99')), 2)
                IN ('05', '06', '07', '08', '09', 
                    '12', '13', '14', '33', '99')
            THEN NULL
        ELSE txn.provider_city_name  
    END                                                                                     AS prov_billing_city, 
    /* prov_billing_state */     
    VALIDATE_STATE_CODE
    (
     CASE
        WHEN RIGHT(CONCAT('00', COALESCE(txn.place_of_service_code, '99')), 2)
                IN ('05', '06', '07', '08', '09', 
                    '12', '13', '14', '33', '99')
            THEN NULL
        ELSE txn.provider_state_or_province_code
    END                                                                                        
    )                                                                                       AS prov_billing_state, 
    /* prov_billing_zip */    
    CASE
        WHEN RIGHT(CONCAT('00', COALESCE(txn.place_of_service_code, '99')), 2)
                IN ('05', '06', '07', '08', '09', 
                    '12', '13', '14', '33', '99')
            THEN NULL
        ELSE txn.provider_postal_code  
    END                                                                                     AS prov_billing_zip,      
	'express_scripts'                                                                       AS part_provider, 
    /* part_best_date */
	CASE
	    WHEN 0 = LENGTH(TRIM(COALESCE
	                            (
	                                CAP_DATE
                                        (
                                            CAST(EXTRACT_DATE(txn.first_serviced_date, '%Y%m%d') AS DATE), 
                                            COALESCE(ahdt.gen_ref_1_dt, esdt.gen_ref_1_dt), 
                                            CAST(EXTRACT_DATE('{VDR_FILE_DT}', '%Y-%m-%d') AS DATE)
                                        ), 
                                    ''
                                )))
	        THEN '0_PREDATES_HVM_HISTORY'
        ELSE CONCAT
	            (
                    SUBSTR(txn.first_serviced_date, 1, 4), '-', 
                    SUBSTR(txn.first_serviced_date, 5, 2), '-01'
                )
	END                                                                                 AS part_best_date

FROM txn
LEFT OUTER JOIN matching_payload dx_pay ON txn.hvjoinkey    = dx_pay.hvjoinkey
LEFT OUTER JOIN local_phi rx_pay ON dx_pay.patientid = rx_pay.patient_id

LEFT OUTER JOIN
    (
        SELECT gen_ref_1_dt
         FROM ref_gen_ref
        WHERE hvm_vdr_feed_id = 155
          AND gen_ref_domn_nm = 'EARLIEST_VALID_SERVICE_DATE'
    ) esdt
   ON 1 = 1
 LEFT OUTER JOIN 
    (
        SELECT gen_ref_1_dt
         FROM ref_gen_ref
        WHERE hvm_vdr_feed_id = 155
          AND gen_ref_domn_nm = 'HVM_AVAILABLE_HISTORY_START_DATE'
    ) ahdt
   ON 1 = 1
--------------  Dignosis Code + Proc + Revenue Code
WHERE 
    --LPAD(txn.medical_qualifier_code, 2, 0) IN ('01', '02') 
      txn.medical_code IS NOT NULL 
  AND UPPER(COALESCE(txn.unique_patient_id, 'X')) <> UPPER('unique_patient_id')
