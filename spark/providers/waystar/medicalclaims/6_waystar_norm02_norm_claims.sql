SELECT DISTINCT
    clm.claim_number																		AS claim_id,
	pay.hvid																				AS hvid,
    CURRENT_DATE()                                                                          AS created,
	'08'                                                                                    AS model_version,
--    SPLIT(clm.input_file_name, '/')[SIZE(SPLIT(clm.input_file_name, '/')) - 1]              AS data_set,
    clm.data_set,
	'24'																					AS data_feed,
	'34'																					AS data_vendor,
	/* patient_gender */
	CLEAN_UP_GENDER
	    (
        	CASE
        	    WHEN UPPER(SUBSTR(clm.patient_gender, 1, 1)) IN ('F', 'M')
        	         THEN UPPER(SUBSTR(clm.patient_gender, 1, 1))
        	    WHEN UPPER(SUBSTR(pay.gender, 1, 1)) IN ('F', 'M')
        	         THEN UPPER(SUBSTR(pay.gender, 1, 1)) ELSE 'U' 
        	END
	    )																					AS patient_gender,
	/* patient_age */
	CAP_AGE
	    (
	        VALIDATE_AGE
	            (
	                COALESCE(clm.patient_age, pay.age),
	                CAST(EXTRACT_DATE(COALESCE(sln.service_from, clm.statement_from), '%Y%m%d') AS DATE),
	                COALESCE(clm.patient_yob, pay.yearofbirth)
	            )
	    )																					AS patient_age,
	/* patient_year_of_birth */
	CAP_YEAR_OF_BIRTH
	    (
            COALESCE(clm.patient_age, pay.age),
            CAST(EXTRACT_DATE(COALESCE(sln.service_from, clm.statement_from), '%Y%m%d') AS DATE),
            COALESCE(clm.patient_yob, pay.yearofbirth)
	    )																					AS patient_year_of_birth,
	MASK_ZIP_CODE(SUBSTR(COALESCE(clm.member_adr_zip, pay.threedigitzip), 1, 3))			AS patient_zip3,
	VALIDATE_STATE_CODE(UPPER(COALESCE(clm.member_adr_state, pay.state, '')))				AS patient_state,
	/* As per Reyna, load all claim types, but only expose */
	/* Professional (P) and Institutional (I) claims in the view. */
	clm.claim_type_code																		AS claim_type,
	CAST(EXTRACT_DATE(clm.received_date, '%Y%m%d') AS DATE)								    AS date_received,
	/* date_service */
	CAP_DATE
	    (
            CAST(EXTRACT_DATE(mmd.min_claim_date, '%Y%m%d') AS DATE),
            CAST('{AVAILABLE_START_DATE}' AS DATE),
            CAST('{VDR_FILE_DT}' AS DATE)
	    )                                                                                   AS date_service,
	/* date_service_end */
	CAP_DATE
	    (
            CAST(EXTRACT_DATE(mmd.max_claim_date, '%Y%m%d') AS DATE),
            CAST('{AVAILABLE_START_DATE}' AS DATE),
            CAST('{VDR_FILE_DT}' AS DATE)
	    )                                                                                   AS date_service_end,
	/* inst_date_admitted */
	CAP_DATE
	    (
            CAST(EXTRACT_DATE(clm.admission_date, '%Y%m%d') AS DATE),
            CAST('{AVAILABLE_START_DATE}' AS DATE),
            CAST('{VDR_FILE_DT}' AS DATE)
	    )                                                                                   AS inst_date_admitted,
	clm.admit_type_code																		AS inst_admit_type_std_id,
	clm.admit_src_code																		AS inst_admit_source_std_id,
	clm.patient_status_cd																	AS inst_discharge_status_std_id,
	/* inst_type_of_bill_std_id */
	CASE 
	    WHEN clm.type_bill IS NULL
	         THEN NULL
	    WHEN COALESCE(clm.claim_type_code, 'X') <> 'I'
	         THEN NULL
	    WHEN SUBSTR(clm.type_bill, 1, 1) = '3'
	         THEN CONCAT('X', SUBSTR(clm.type_bill, 2)) 
	    ELSE clm.type_bill
	END 																					AS inst_type_of_bill_std_id,
	/* inst_drg_std_id */
	CASE
	    WHEN clm.drg_code IN ('283', '284', '285', '789')
	         THEN NULL
	    ELSE clm.drg_code
	END 																					AS inst_drg_std_id,
	/* place_of_service_std_id */
	CASE
	    WHEN sln.place_service IS NULL
	         THEN NULL
	    WHEN COALESCE(clm.claim_type_code, 'X') <> 'P'
	         THEN NULL
	    WHEN SUBSTR(CONCAT('00', sln.place_service), -2) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33')
	         THEN '99'
	    ELSE SUBSTR(CONCAT('00', sln.place_service), -2)
	END 																					AS place_of_service_std_id,
	CAST(NULL AS STRING)																    AS service_line_number,
	/* diagnosis_code */
	/* Leave the privacy filtering to the final normalization */
	/* so we can accurately add the claim-level diagnoses.    */
    CASE
        WHEN ARRAY
                (
                    clm.admit_diagnosis,
                    clm.primary_diagnosis,
                    clm.diagnosis_code_2,
                    clm.diagnosis_code_3,
                    clm.diagnosis_code_4,
                    clm.diagnosis_code_5,
                    clm.diagnosis_code_6,
                    clm.diagnosis_code_7,
                    clm.diagnosis_code_8,
                    clm.amb_nurse_to_diag
                )[diag_explode.n] IS NULL 
             THEN NULL
        ELSE ARRAY
                (
                    clm.admit_diagnosis,
                    clm.primary_diagnosis,
                    clm.diagnosis_code_2,
                    clm.diagnosis_code_3,
                    clm.diagnosis_code_4,
                    clm.diagnosis_code_5,
                    clm.diagnosis_code_6,
                    clm.diagnosis_code_7,
                    clm.diagnosis_code_8,
                    clm.amb_nurse_to_diag
                )[diag_explode.n]
    END                                                                                     AS diagnosis_code,
    /* diagnosis_code_qual */
    CASE
        WHEN ARRAY
                (
                    clm.admit_diagnosis,
                    clm.primary_diagnosis,
                    clm.diagnosis_code_2,
                    clm.diagnosis_code_3,
                    clm.diagnosis_code_4,
                    clm.diagnosis_code_5,
                    clm.diagnosis_code_6,
                    clm.diagnosis_code_7,
                    clm.diagnosis_code_8,
                    clm.amb_nurse_to_diag
                )[diag_explode.n] IS NULL 
             THEN NULL
        WHEN clm.coding_type IS NULL
             THEN NULL
        WHEN clm.coding_type = '9'
             THEN '01'
        WHEN UPPER(clm.coding_type) = 'X'
             THEN '02'
        ELSE NULL
	END 																					AS diagnosis_code_qual,
	CAST(NULL AS STRING) 																	AS diagnosis_priority,
	/* admit_diagnosis_ind */
	CASE
        WHEN ARRAY
                (
                    clm.admit_diagnosis,
                    clm.primary_diagnosis,
                    clm.diagnosis_code_2,
                    clm.diagnosis_code_3,
                    clm.diagnosis_code_4,
                    clm.diagnosis_code_5,
                    clm.diagnosis_code_6,
                    clm.diagnosis_code_7,
                    clm.diagnosis_code_8,
                    clm.amb_nurse_to_diag
                )[diag_explode.n] IS NULL 
             THEN NULL
        WHEN clm.admit_diagnosis IS NULL
             THEN NULL
	    WHEN clm.admit_diagnosis = ARRAY
                                    (
                                        clm.admit_diagnosis,
                                        clm.primary_diagnosis,
                                        clm.diagnosis_code_2,
                                        clm.diagnosis_code_3,
                                        clm.diagnosis_code_4,
                                        clm.diagnosis_code_5,
                                        clm.diagnosis_code_6,
                                        clm.diagnosis_code_7,
                                        clm.diagnosis_code_8,
                                        clm.amb_nurse_to_diag
                                    )[diag_explode.n]
             THEN 'Y'
        ELSE 'N'
	END 																					AS admit_diagnosis_ind,
	CAST(NULL AS STRING)											                        AS procedure_code,
	CAST(NULL AS STRING) 																	AS procedure_code_qual,
	CAST(NULL AS STRING) 																	AS principal_proc_ind,
	CAST(NULL AS FLOAT)												    				    AS procedure_units_billed,
	CAST(NULL AS STRING)											                        AS procedure_modifier_1,
	CAST(NULL AS STRING)																	AS procedure_modifier_2,
	CAST(NULL AS STRING)																	AS procedure_modifier_3,
	CAST(NULL AS STRING)																	AS procedure_modifier_4,
	CAST(NULL AS STRING)																    AS revenue_code,
	CAST(NULL AS STRING)												                    AS ndc_code,
	clm.type_coverage																		AS medical_coverage_type,
	CAST(NULL AS FLOAT)											                            AS line_charge,
	CAST(NULL AS FLOAT)													                    AS line_allowed,
	CAST(clm.total_charge AS FLOAT)															AS total_charge,
	CAST(clm.total_allowed AS FLOAT)														AS total_allowed,
	/* prov_rendering_npi */
	CLEAN_UP_NPI_CODE
	    (
        	CASE
        	    WHEN COALESCE(clm.claim_type_code, 'X') NOT IN ('I', 'P')
        	         THEN NULL
                WHEN clm.claim_type_code = 'P'
                 AND SUBSTR(CONCAT('00', sln.place_service), -2) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
                     THEN NULL
                WHEN clm.claim_type_code = 'I'
                 AND SUBSTR(COALESCE(clm.type_bill, 'X'), 1, 1) IN ('3', 'X')
                     THEN NULL
                ELSE clm.attending_pr_npi
        	END
	    )   																				AS prov_rendering_npi,
	/* prov_billing_npi */
	CLEAN_UP_NPI_CODE
	    (
        	CASE
        	    WHEN COALESCE(clm.claim_type_code, 'X') NOT IN ('I', 'P')
        	         THEN NULL
                WHEN clm.claim_type_code = 'P'
                 AND SUBSTR(CONCAT('00', sln.place_service), -2) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
                     THEN NULL
                WHEN clm.claim_type_code = 'I'
                 AND SUBSTR(COALESCE(clm.type_bill, 'X'), 1, 1) IN ('3', 'X')
                     THEN NULL
                ELSE clm.billing_pr_npi
        	END
	    )																					AS prov_billing_npi,
	/* prov_referring_npi */
	CLEAN_UP_NPI_CODE
	    (
        	CASE
        	    WHEN COALESCE(clm.claim_type_code, 'X') NOT IN ('I', 'P')
        	         THEN NULL
                WHEN clm.claim_type_code = 'P'
                 AND SUBSTR(CONCAT('00', sln.place_service), -2) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
                     THEN NULL
                WHEN clm.claim_type_code = 'I'
                 AND SUBSTR(COALESCE(clm.type_bill, 'X'), 1, 1) IN ('3', 'X')
                     THEN NULL
                ELSE clm.referring_pr_npi
        	END
	    )																					AS prov_referring_npi,
	/* prov_facility_npi */
	CLEAN_UP_NPI_CODE
	    (
        	CASE
        	    WHEN COALESCE(clm.claim_type_code, 'X') NOT IN ('I', 'P')
        	         THEN NULL
                WHEN clm.claim_type_code = 'P'
                 AND SUBSTR(CONCAT('00', sln.place_service), -2) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
                     THEN NULL
                WHEN clm.claim_type_code = 'I'
                 AND SUBSTR(COALESCE(clm.type_bill, 'X'), 1, 1) IN ('3', 'X')
                     THEN NULL
                ELSE clm.facility_npi
        	END
	    )																					AS prov_facility_npi,
	clm.payer_name																			AS payer_name,
	clm.payer_id																			AS payer_plan_id,
	/* prov_rendering_state_license */
	CASE
	    WHEN COALESCE(clm.claim_type_code, 'X') NOT IN ('I', 'P')
	         THEN NULL
        WHEN clm.claim_type_code = 'P'
         AND SUBSTR(CONCAT('00', sln.place_service), -2) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
             THEN NULL
        WHEN clm.claim_type_code = 'I'
         AND SUBSTR(COALESCE(clm.type_bill, 'X'), 1, 1) IN ('3', 'X')
             THEN NULL
        ELSE clm.rendering_state_lic
	END																			    		AS prov_rendering_state_license,
	/* prov_rendering_upin */
	CASE
	    WHEN COALESCE(clm.claim_type_code, 'X') NOT IN ('I', 'P')
	         THEN NULL
        WHEN clm.claim_type_code = 'P'
         AND SUBSTR(CONCAT('00', sln.place_service), -2) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
             THEN NULL
        WHEN clm.claim_type_code = 'I'
         AND SUBSTR(COALESCE(clm.type_bill, 'X'), 1, 1) IN ('3', 'X')
             THEN NULL
        ELSE clm.rendering_upin
	END																    					AS prov_rendering_upin,
	/* prov_rendering_name_1 */
	CASE
	    WHEN COALESCE(clm.claim_type_code, 'X') NOT IN ('I', 'P')
	         THEN NULL
        WHEN clm.claim_type_code = 'P'
         AND SUBSTR(CONCAT('00', sln.place_service), -2) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
             THEN NULL
        WHEN clm.claim_type_code = 'I'
         AND SUBSTR(COALESCE(clm.type_bill, 'X'), 1, 1) IN ('3', 'X')
             THEN NULL
        WHEN 0 <> LENGTH(TRIM(CONCAT
                                (
                                    COALESCE(clmnms.attending_name1, ''), 
                                    COALESCE(clmnms.attending_name2, '')
                                ))) 
             THEN TRIM(CONCAT
                        (
                            COALESCE(clmnms.attending_name1, ''), 
                            COALESCE(clmnms.attending_name2, '')
                        )) 
        ELSE NULL
	END																				    	AS prov_rendering_name_1,
	clm.rendering_taxonomy																	AS prov_rendering_std_taxonomy,
	clm.prov_specialty																		AS prov_rendering_vendor_specialty,
	/* prov_billing_tax_id */
	CASE
	    WHEN COALESCE(clm.claim_type_code, 'X') NOT IN ('I', 'P')
	         THEN NULL
        WHEN clm.claim_type_code = 'P'
         AND SUBSTR(CONCAT('00', sln.place_service), -2) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
             THEN NULL
        WHEN clm.claim_type_code = 'I'
         AND SUBSTR(COALESCE(clm.type_bill, 'X'), 1, 1) IN ('3', 'X')
             THEN NULL
        ELSE clm.billing_pr_id
	END						    															AS prov_billing_tax_id,
	/* prov_billing_ssn */
	CASE
	    WHEN COALESCE(clm.claim_type_code, 'X') NOT IN ('I', 'P')
	         THEN NULL
        WHEN clm.claim_type_code = 'P'
         AND SUBSTR(CONCAT('00', sln.place_service), -2) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
             THEN NULL
        WHEN clm.claim_type_code = 'I'
         AND SUBSTR(COALESCE(clm.type_bill, 'X'), 1, 1) IN ('3', 'X')
             THEN NULL
        ELSE clm.billing_ssn
	END						    															AS prov_billing_ssn,
	/* prov_billing_state_license */
	CASE
	    WHEN COALESCE(clm.claim_type_code, 'X') NOT IN ('I', 'P')
	         THEN NULL
        WHEN clm.claim_type_code = 'P'
         AND SUBSTR(CONCAT('00', sln.place_service), -2) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
             THEN NULL
        WHEN clm.claim_type_code = 'I'
         AND SUBSTR(COALESCE(clm.type_bill, 'X'), 1, 1) IN ('3', 'X')
             THEN NULL
        ELSE clm.billing_state_lic
	END						    															AS prov_billing_state_license,
	/* prov_billing_upin */
	CASE
	    WHEN COALESCE(clm.claim_type_code, 'X') NOT IN ('I', 'P')
	         THEN NULL
        WHEN clm.claim_type_code = 'P'
         AND SUBSTR(CONCAT('00', sln.place_service), -2) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
             THEN NULL
        WHEN clm.claim_type_code = 'I'
         AND SUBSTR(COALESCE(clm.type_bill, 'X'), 1, 1) IN ('3', 'X')
             THEN NULL
        ELSE clm.billing_upin
	END						    															AS prov_billing_upin,
	/* prov_billing_name_1 */
	CASE
	    WHEN COALESCE(clm.claim_type_code, 'X') NOT IN ('I', 'P')
	         THEN NULL
        WHEN clm.claim_type_code = 'P'
         AND SUBSTR(CONCAT('00', sln.place_service), -2) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
             THEN NULL
        WHEN clm.claim_type_code = 'I'
         AND SUBSTR(COALESCE(clm.type_bill, 'X'), 1, 1) IN ('3', 'X')
             THEN NULL
        WHEN 0 <> LENGTH(TRIM(CONCAT
                                (
                                    COALESCE(clmnms.billing_name1, ''),
                                    COALESCE(clmnms.billing_name2, '')
                                ))) 
             THEN TRIM(CONCAT
                        (
                            COALESCE(clmnms.billing_name1, ''), 
                            COALESCE(clmnms.billing_name2, '')
                        )) 
        ELSE NULL
	END						    															AS prov_billing_name_1,
	/* prov_billing_address_1 */
	CASE
	    WHEN COALESCE(clm.claim_type_code, 'X') NOT IN ('I', 'P')
	         THEN NULL
        WHEN clm.claim_type_code = 'P'
         AND SUBSTR(CONCAT('00', sln.place_service), -2) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
             THEN NULL
        WHEN clm.claim_type_code = 'I'
         AND SUBSTR(COALESCE(clm.type_bill, 'X'), 1, 1) IN ('3', 'X')
             THEN NULL
        ELSE clm.billing_adr_line1
	END						    															AS prov_billing_address_1,
	/* prov_billing_address_2 */
	CASE
	    WHEN COALESCE(clm.claim_type_code, 'X') NOT IN ('I', 'P')
	         THEN NULL
        WHEN clm.claim_type_code = 'P'
         AND SUBSTR(CONCAT('00', sln.place_service), -2) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
             THEN NULL
        WHEN clm.claim_type_code = 'I'
         AND SUBSTR(COALESCE(clm.type_bill, 'X'), 1, 1) IN ('3', 'X')
             THEN NULL
        ELSE clm.billing_adr_line2
	END						    															AS prov_billing_address_2,
	/* prov_billing_city */
	CASE
	    WHEN COALESCE(clm.claim_type_code, 'X') NOT IN ('I', 'P')
	         THEN NULL
        WHEN clm.claim_type_code = 'P'
         AND SUBSTR(CONCAT('00', sln.place_service), -2) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
             THEN NULL
        WHEN clm.claim_type_code = 'I'
         AND SUBSTR(COALESCE(clm.type_bill, 'X'), 1, 1) IN ('3', 'X')
             THEN NULL
        ELSE clm.billing_adr_city
	END						    															AS prov_billing_city,
	/* prov_billing_state */
	VALIDATE_STATE_CODE
	    (
        	CASE
        	    WHEN COALESCE(clm.claim_type_code, 'X') NOT IN ('I', 'P')
        	         THEN NULL
                WHEN clm.claim_type_code = 'P'
                 AND SUBSTR(CONCAT('00', sln.place_service), -2) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
                     THEN NULL
                WHEN clm.claim_type_code = 'I'
                 AND SUBSTR(COALESCE(clm.type_bill, 'X'), 1, 1) IN ('3', 'X')
                     THEN NULL
                ELSE clm.billing_adr_state
        	END
	    )   																				AS prov_billing_state,
	/* prov_billing_zip */
	CASE
	    WHEN COALESCE(clm.claim_type_code, 'X') NOT IN ('I', 'P')
	         THEN NULL
        WHEN clm.claim_type_code = 'P'
         AND SUBSTR(CONCAT('00', sln.place_service), -2) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
             THEN NULL
        WHEN clm.claim_type_code = 'I'
         AND SUBSTR(COALESCE(clm.type_bill, 'X'), 1, 1) IN ('3', 'X')
             THEN NULL
        ELSE clm.billing_adr_zip
	END						    															AS prov_billing_zip,
	clm.billing_taxonomy																	AS prov_billing_std_taxonomy,
	/* prov_referring_name_1 */
	CASE
	    WHEN COALESCE(clm.claim_type_code, 'X') NOT IN ('I', 'P')
	         THEN NULL
        WHEN clm.claim_type_code = 'P'
         AND SUBSTR(CONCAT('00', sln.place_service), -2) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
             THEN NULL
        WHEN clm.claim_type_code = 'I'
         AND SUBSTR(COALESCE(clm.type_bill, 'X'), 1, 1) IN ('3', 'X')
             THEN NULL
        WHEN 0 <> LENGTH(TRIM(CONCAT
                                (
                                    COALESCE(clmnms.referring_name1, ''), 
                                    COALESCE(clmnms.referring_name2, '')
                                )))
             THEN TRIM(CONCAT
                        (
                            COALESCE(clmnms.referring_name1, ''), 
                            COALESCE(clmnms.referring_name2, '')
                        ))
        ELSE NULL
	END						    															AS prov_referring_name_1,
	/* prov_facility_state_license */
	CASE
	    WHEN COALESCE(clm.claim_type_code, 'X') NOT IN ('I', 'P')
	         THEN NULL
        WHEN clm.claim_type_code = 'P'
         AND SUBSTR(CONCAT('00', sln.place_service), -2) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
             THEN NULL
        WHEN clm.claim_type_code = 'I'
         AND SUBSTR(COALESCE(clm.type_bill, 'X'), 1, 1) IN ('3', 'X')
             THEN NULL
        ELSE clm.facility_state_lic
	END						    															AS prov_facility_state_license,
	/* prov_facility_name_1 */
	CASE
	    WHEN COALESCE(clm.claim_type_code, 'X') NOT IN ('I', 'P')
	         THEN NULL
        WHEN clm.claim_type_code = 'P'
         AND SUBSTR(CONCAT('00', sln.place_service), -2) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
             THEN NULL
        WHEN clm.claim_type_code = 'I'
         AND SUBSTR(COALESCE(clm.type_bill, 'X'), 1, 1) IN ('3', 'X')
             THEN NULL
        WHEN 0 <> LENGTH(TRIM(CONCAT
                                (
                                    COALESCE(clmnms.facility_name1, ''), 
                                    COALESCE(clmnms.facility_name2, '')
                                ))) 
             THEN TRIM(CONCAT
                        (
                            COALESCE(clmnms.facility_name1, ''), 
                            COALESCE(clmnms.facility_name2, '')
                        ))
        ELSE NULL
	END						    															AS prov_facility_name_1,
	/* prov_facility_address_1 */
	CASE
	    WHEN COALESCE(clm.claim_type_code, 'X') NOT IN ('I', 'P')
	         THEN NULL
        WHEN clm.claim_type_code = 'P'
         AND SUBSTR(CONCAT('00', sln.place_service), -2) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
             THEN NULL
        WHEN clm.claim_type_code = 'I'
         AND SUBSTR(COALESCE(clm.type_bill, 'X'), 1, 1) IN ('3', 'X')
             THEN NULL
        ELSE clm.facility_adr_line1
	END						    															AS prov_facility_address_1,
	/* prov_facility_address_2 */
	CASE
	    WHEN COALESCE(clm.claim_type_code, 'X') NOT IN ('I', 'P')
	         THEN NULL
        WHEN clm.claim_type_code = 'P'
         AND SUBSTR(CONCAT('00', sln.place_service), -2) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
             THEN NULL
        WHEN clm.claim_type_code = 'I'
         AND SUBSTR(COALESCE(clm.type_bill, 'X'), 1, 1) IN ('3', 'X')
             THEN NULL
        ELSE clm.facility_adr_line2
	END						    															AS prov_facility_address_2,
	/* prov_facility_city */
	CASE
	    WHEN COALESCE(clm.claim_type_code, 'X') NOT IN ('I', 'P')
	         THEN NULL
        WHEN clm.claim_type_code = 'P'
         AND SUBSTR(CONCAT('00', sln.place_service), -2) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
             THEN NULL
        WHEN clm.claim_type_code = 'I'
         AND SUBSTR(COALESCE(clm.type_bill, 'X'), 1, 1) IN ('3', 'X')
             THEN NULL
        ELSE clm.facility_adr_city
	END						    															AS prov_facility_city,
	/* prov_facility_state */
	VALIDATE_STATE_CODE
	    (
        	CASE
        	    WHEN COALESCE(clm.claim_type_code, 'X') NOT IN ('I', 'P')
        	         THEN NULL
                WHEN clm.claim_type_code = 'P'
                 AND SUBSTR(CONCAT('00', sln.place_service), -2) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
                     THEN NULL
                WHEN clm.claim_type_code = 'I'
                 AND SUBSTR(COALESCE(clm.type_bill, 'X'), 1, 1) IN ('3', 'X')
                     THEN NULL
                ELSE clm.facility_adr_state
        	END
	    )   																				AS prov_facility_state,
	/* prov_facility_zip */
	CASE
	    WHEN COALESCE(clm.claim_type_code, 'X') NOT IN ('I', 'P')
	         THEN NULL
        WHEN clm.claim_type_code = 'P'
         AND SUBSTR(CONCAT('00', sln.place_service), -2) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
             THEN NULL
        WHEN clm.claim_type_code = 'I'
         AND SUBSTR(COALESCE(clm.type_bill, 'X'), 1, 1) IN ('3', 'X')
             THEN NULL
        ELSE clm.facility_adr_zip
	END						    															AS prov_facility_zip,
	/* Changed from pay.pcn to clm.claim_number JKS 12/5/2019 */
	clm.claim_number                                                                        AS medical_claim_link_text,	
	'navicure'																				AS part_provider,
	/* part_best_date */
	CASE
	    WHEN CAP_DATE
	            (
	                CAST(EXTRACT_DATE(mmd.min_claim_date, '%Y%m%d') AS DATE),
                    CAST('{EARLIEST_SERVICE_DATE}' AS DATE),
                    CAST('{VDR_FILE_DT}' AS DATE)
	            ) IS NULL
	         THEN '0_PREDATES_HVM_HISTORY'
	    ELSE CONCAT
	            (
	                SUBSTR(mmd.min_claim_date, 1, 4), '-',
	                SUBSTR(mmd.min_claim_date, 5, 2), '-01'
	            )
	END 																					AS part_best_date
 FROM waystar_dedup_claims clm
 /* Link to the first service line for place of service. */
 LEFT OUTER JOIN waystar_dedup_lines sln
   ON clm.claim_number = sln.claim_number
  AND sln.line_number = '1'
 LEFT OUTER JOIN matching_payload pay 
   ON clm.hvjoinkey = pay.hvjoinkey
 /* Deduplicate the source name columns without trimming and nullifying. */
 /* The source columns sometimes contain trailing blanks (1) and leading */
 /* blanks (2) that are part of the full provider name. */
 LEFT OUTER JOIN
    (
        SELECT DISTINCT
            claim_number,
            attending_name1,
            attending_name2,
            billing_name1,
            billing_name2,
            referring_name1,
            referring_name2,
            facility_name1,
            facility_name2
         FROM claims
    ) clmnms
   ON clm.claim_number = clmnms.claim_number
 LEFT OUTER JOIN
/* Get the min and max dates for each claim_number. */
(
    SELECT
        sub.claim_number,
        MIN(sub.min_dt) AS min_claim_date,
        MAX(sub.max_dt) AS max_claim_date
     FROM
    (
        SELECT
            sln1.claim_number,
            sln1.service_from AS min_dt,
            sln1.service_to AS max_dt
         FROM waystar_dedup_lines sln1
        UNION ALL
        SELECT
            clm1.claim_number,
            clm1.statement_from AS min_dt,
            clm1.statement_to AS max_dt
         FROM waystar_dedup_claims clm1
    ) sub
    GROUP BY 1
) mmd
   ON clm.claim_number = mmd.claim_number

CROSS JOIN (SELECT EXPLODE(ARRAY(0, 1, 2, 3, 4, 5, 6, 7, 8, 9)) AS n) diag_explode
WHERE NOT EXISTS
    (
        SELECT 1
         FROM waystar_dedup_lines sln2
        CROSS JOIN (SELECT EXPLODE(ARRAY(0, 1, 2, 3, 4, 5, 6, 7)) AS n) diag_explode_sln
        WHERE clm.claim_number = sln2.claim_number
          AND 
            COALESCE
                (
                    ARRAY
                        (
                            clm.admit_diagnosis,
                            clm.primary_diagnosis,
                            clm.diagnosis_code_2,
                            clm.diagnosis_code_3,
                            clm.diagnosis_code_4,
                            clm.diagnosis_code_5,
                            clm.diagnosis_code_6,
                            clm.diagnosis_code_7,
                            clm.diagnosis_code_8,
                            clm.amb_nurse_to_diag
                        )[diag_explode.n], 'EMPTY'
                ) = 
            COALESCE
                (
                    ARRAY
                        (
                            clm.primary_diagnosis,
                            clm.diagnosis_code_2,
                            clm.diagnosis_code_3,
                            clm.diagnosis_code_4,
                            clm.diagnosis_code_5,
                            clm.diagnosis_code_6,
                            clm.diagnosis_code_7,
                            clm.diagnosis_code_8
                        )
                            [
                                CAST(-1 AS INTEGER) +
                                CAST
                                    (
                                        COALESCE
                                            (
                                                ARRAY
                                                    (
                                                        sln2.diagnosis_pointer_1,
                                                        sln2.diagnosis_pointer_2,
                                                        sln2.diagnosis_pointer_3,
                                                        sln2.diagnosis_pointer_4,
                                                        sln2.diagnosis_pointer_5,
                                                        sln2.diagnosis_pointer_6,
                                                        sln2.diagnosis_pointer_7,
                                                        sln2.diagnosis_pointer_8
                                                    )[diag_explode_sln.n], '99'
                                            ) AS INTEGER
                                    )
                            ], 'NULL'
                )
    )
---------- Diagnosis code explosion
  AND ARRAY
        (
            clm.admit_diagnosis,
            clm.primary_diagnosis,
            clm.diagnosis_code_2,
            clm.diagnosis_code_3,
            clm.diagnosis_code_4,
            clm.diagnosis_code_5,
            clm.diagnosis_code_6,
            clm.diagnosis_code_7,
            clm.diagnosis_code_8,
            clm.amb_nurse_to_diag
        )[diag_explode.n] IS NOT NULL
/* Eliminate claims loaded from Navicure source data. */
  AND NOT EXISTS
    (
        SELECT 1
         FROM waystar_medicalclaims_augment_comb aug 
        WHERE clm.claim_number = aug.instanceid
    )

-----------------------------------------------------------------------------------
---- UNION 2.5 Insert normalized claim-level data - procedure codes without a diagnosis code
-----------------------------------------------------------------------------------
UNION ALL

SELECT DISTINCT 
    clm.claim_number																		AS claim_id,
	pay.hvid																				AS hvid,
    CURRENT_DATE()                                                                          AS created,
	'08'                                                                                    AS model_version,
--    SPLIT(clm.input_file_name, '/')[SIZE(SPLIT(clm.input_file_name, '/')) - 1]              AS data_set,
    clm.data_set,
	'24'																					AS data_feed,
	'34'																					AS data_vendor,
	/* patient_gender */
	CLEAN_UP_GENDER
	    (
        	CASE
        	    WHEN UPPER(SUBSTR(clm.patient_gender, 1, 1)) IN ('F', 'M')
        	         THEN UPPER(SUBSTR(clm.patient_gender, 1, 1))
        	    WHEN UPPER(SUBSTR(pay.gender, 1, 1)) IN ('F', 'M')
        	         THEN UPPER(SUBSTR(pay.gender, 1, 1)) ELSE 'U' 
        	END
	    )																					AS patient_gender,
	/* patient_age */
	CAP_AGE
	    (
	        VALIDATE_AGE
	            (
	                COALESCE(clm.patient_age, pay.age),
	                CAST(EXTRACT_DATE(COALESCE(sln.service_from, clm.statement_from), '%Y%m%d') AS DATE),
	                COALESCE(clm.patient_yob, pay.yearofbirth)
	            )
	    )																					AS patient_age,
	/* patient_year_of_birth */
	CAP_YEAR_OF_BIRTH
	    (
            COALESCE(clm.patient_age, pay.age),
            CAST(EXTRACT_DATE(COALESCE(sln.service_from, clm.statement_from), '%Y%m%d') AS DATE),
            COALESCE(clm.patient_yob, pay.yearofbirth)
	    )																					AS patient_year_of_birth,
	MASK_ZIP_CODE(SUBSTR(COALESCE(clm.member_adr_zip, pay.threedigitzip), 1, 3))			AS patient_zip3,
	VALIDATE_STATE_CODE(UPPER(COALESCE(clm.member_adr_state, pay.state, '')))				AS patient_state,
	/* As per Reyna, load all claim types, but only expose */
	/* Professional (P) and Institutional (I) claims in the view. */
	clm.claim_type_code																		AS claim_type,
	CAST(EXTRACT_DATE(clm.received_date, '%Y%m%d') AS DATE)								    AS date_received,
	/* date_service */
	CAP_DATE
	    (
            CAST(EXTRACT_DATE(mmd.min_claim_date, '%Y%m%d') AS DATE),
            CAST('{AVAILABLE_START_DATE}' AS DATE),
            CAST('{VDR_FILE_DT}' AS DATE)
	    )                                                                                   AS date_service,
	/* date_service_end */
	CAP_DATE
	    (
            CAST(EXTRACT_DATE(mmd.max_claim_date, '%Y%m%d') AS DATE),
            CAST('{AVAILABLE_START_DATE}' AS DATE),
            CAST('{VDR_FILE_DT}' AS DATE)
	    )                                                                                   AS date_service_end,
	/* inst_date_admitted */
	CAP_DATE
	    (
            CAST(EXTRACT_DATE(clm.admission_date, '%Y%m%d') AS DATE),
            CAST('{AVAILABLE_START_DATE}' AS DATE),
            CAST('{VDR_FILE_DT}' AS DATE)
	    )                                                                                   AS inst_date_admitted,
	clm.admit_type_code																		AS inst_admit_type_std_id,
	clm.admit_src_code																		AS inst_admit_source_std_id,
	clm.patient_status_cd																	AS inst_discharge_status_std_id,
	/* inst_type_of_bill_std_id */
	CASE 
	    WHEN clm.type_bill IS NULL
	         THEN NULL
	    WHEN COALESCE(clm.claim_type_code, 'X') <> 'I'
	         THEN NULL
	    WHEN SUBSTR(clm.type_bill, 1, 1) = '3'
	         THEN CONCAT('X', SUBSTR(clm.type_bill, 2)) 
	    ELSE clm.type_bill
	END 																					AS inst_type_of_bill_std_id,
	/* inst_drg_std_id */
	CASE
	    WHEN clm.drg_code IN ('283', '284', '285', '789')
	         THEN NULL
	    ELSE clm.drg_code
	END 																					AS inst_drg_std_id,
	/* place_of_service_std_id */
	CASE
	    WHEN sln.place_service IS NULL
	         THEN NULL
	    WHEN COALESCE(clm.claim_type_code, 'X') <> 'P'
	         THEN NULL
	    WHEN SUBSTR(CONCAT('00', sln.place_service), -2) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33')
	         THEN '99'
	    ELSE SUBSTR(CONCAT('00', sln.place_service), -2)
	END 																					AS place_of_service_std_id,
	CAST(NULL AS STRING)															        AS service_line_number,
	CAST(NULL AS STRING)                                                                    AS diagnosis_code,
    CAST(NULL AS STRING)																	AS diagnosis_code_qual,
	CAST(NULL AS STRING)																	AS diagnosis_priority,
	CAST(NULL AS STRING)																	AS admit_diagnosis_ind,
	/* procedure_code */
	CLEAN_UP_PROCEDURE_CODE
	    (
            ARRAY
                (
                    clm.principal_procedure,
                    clm.other_proc_code_2,
                    clm.other_proc_code_3,
                    clm.other_proc_code_4,
                    clm.other_proc_code_5,
                    clm.other_proc_code_6,
                    clm.initial_procedure,
                    clm.other_proc_code_7,
                    clm.other_proc_code_8,
                    clm.other_proc_code_9,
                    clm.other_proc_code_10
                )[proc_explode.n]
	    )				                                							    	AS procedure_code,
	CAST(NULL AS STRING)																	AS procedure_code_qual,
	/* principal_proc_ind */
	CASE
	    WHEN ARRAY
                (
                    clm.principal_procedure,
                    clm.other_proc_code_2,
                    clm.other_proc_code_3,
                    clm.other_proc_code_4,
                    clm.other_proc_code_5,
                    clm.other_proc_code_6,
                    clm.initial_procedure,
                    clm.other_proc_code_7,
                    clm.other_proc_code_8,
                    clm.other_proc_code_9,
                    clm.other_proc_code_10
                )[proc_explode.n] IS NULL
	         THEN NULL
	    WHEN clm.principal_procedure IS NULL
	         THEN NULL
	    WHEN CLEAN_UP_PROCEDURE_CODE(clm.principal_procedure) = 
	         CLEAN_UP_PROCEDURE_CODE
	            (
                    ARRAY
                        (
                            clm.principal_procedure,
                            clm.other_proc_code_2,
                            clm.other_proc_code_3,
                            clm.other_proc_code_4,
                            clm.other_proc_code_5,
                            clm.other_proc_code_6,
                            clm.initial_procedure,
                            clm.other_proc_code_7,
                            clm.other_proc_code_8,
                            clm.other_proc_code_9,
                            clm.other_proc_code_10
                        )[proc_explode.n]
	            )
	         THEN 'Y'
	    ELSE 'N'
	END 																					AS principal_proc_ind,
	CAST(NULL AS FLOAT)											    					    AS procedure_units_billed,
	CAST(NULL AS STRING)							                                        AS procedure_modifier_1,
	CAST(NULL AS STRING)																	AS procedure_modifier_2,
	CAST(NULL AS STRING)																	AS procedure_modifier_3,
	CAST(NULL AS STRING)																	AS procedure_modifier_4,
	CAST(NULL AS STRING)												            		AS revenue_code,
	CAST(NULL AS STRING)								                    				AS ndc_code,
	clm.type_coverage																		AS medical_coverage_type,
	CAST(NULL AS FLOAT)							                        				    AS line_charge,
	CAST(NULL AS FLOAT)									                        		    AS line_allowed,
	CAST(clm.total_charge AS FLOAT)															AS total_charge,
	CAST(clm.total_allowed AS FLOAT)														AS total_allowed,
	/* prov_rendering_npi */
	CLEAN_UP_NPI_CODE
	    (
        	CASE
        	    WHEN COALESCE(clm.claim_type_code, 'X') NOT IN ('I', 'P')
        	         THEN NULL
                WHEN clm.claim_type_code = 'P'
                 AND SUBSTR(CONCAT('00', sln.place_service), -2) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
                     THEN NULL
                WHEN clm.claim_type_code = 'I'
                 AND SUBSTR(COALESCE(clm.type_bill, 'X'), 1, 1) IN ('3', 'X')
                     THEN NULL
                ELSE clm.attending_pr_npi
        	END
	    )   																				AS prov_rendering_npi,
	/* prov_billing_npi */
	CLEAN_UP_NPI_CODE
	    (
        	CASE
        	    WHEN COALESCE(clm.claim_type_code, 'X') NOT IN ('I', 'P')
        	         THEN NULL
                WHEN clm.claim_type_code = 'P'
                 AND SUBSTR(CONCAT('00', sln.place_service), -2) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
                     THEN NULL
                WHEN clm.claim_type_code = 'I'
                 AND SUBSTR(COALESCE(clm.type_bill, 'X'), 1, 1) IN ('3', 'X')
                     THEN NULL
                ELSE clm.billing_pr_npi
        	END
	    )																					AS prov_billing_npi,
	/* prov_referring_npi */
	CLEAN_UP_NPI_CODE
	    (
        	CASE
        	    WHEN COALESCE(clm.claim_type_code, 'X') NOT IN ('I', 'P')
        	         THEN NULL
                WHEN clm.claim_type_code = 'P'
                 AND SUBSTR(CONCAT('00', sln.place_service), -2) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
                     THEN NULL
                WHEN clm.claim_type_code = 'I'
                 AND SUBSTR(COALESCE(clm.type_bill, 'X'), 1, 1) IN ('3', 'X')
                     THEN NULL
                ELSE clm.referring_pr_npi
        	END
	    )																					AS prov_referring_npi,
	/* prov_facility_npi */
	CLEAN_UP_NPI_CODE
	    (
        	CASE
        	    WHEN COALESCE(clm.claim_type_code, 'X') NOT IN ('I', 'P')
        	         THEN NULL
                WHEN clm.claim_type_code = 'P'
                 AND SUBSTR(CONCAT('00', sln.place_service), -2) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
                     THEN NULL
                WHEN clm.claim_type_code = 'I'
                 AND SUBSTR(COALESCE(clm.type_bill, 'X'), 1, 1) IN ('3', 'X')
                     THEN NULL
                ELSE clm.facility_npi
        	END
	    )																					AS prov_facility_npi,
	clm.payer_name																			AS payer_name,
	clm.payer_id																			AS payer_plan_id,
	/* prov_rendering_state_license */
	CASE
	    WHEN COALESCE(clm.claim_type_code, 'X') NOT IN ('I', 'P')
	         THEN NULL
        WHEN clm.claim_type_code = 'P'
         AND SUBSTR(CONCAT('00', sln.place_service), -2) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
             THEN NULL
        WHEN clm.claim_type_code = 'I'
         AND SUBSTR(COALESCE(clm.type_bill, 'X'), 1, 1) IN ('3', 'X')
             THEN NULL
        ELSE clm.rendering_state_lic
	END																			    		AS prov_rendering_state_license,
	/* prov_rendering_upin */
	CASE
	    WHEN COALESCE(clm.claim_type_code, 'X') NOT IN ('I', 'P')
	         THEN NULL
        WHEN clm.claim_type_code = 'P'
         AND SUBSTR(CONCAT('00', sln.place_service), -2) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
             THEN NULL
        WHEN clm.claim_type_code = 'I'
         AND SUBSTR(COALESCE(clm.type_bill, 'X'), 1, 1) IN ('3', 'X')
             THEN NULL
        ELSE clm.rendering_upin
	END																    					AS prov_rendering_upin,
	/* prov_rendering_name_1 */
	CASE
	    WHEN COALESCE(clm.claim_type_code, 'X') NOT IN ('I', 'P')
	         THEN NULL
        WHEN clm.claim_type_code = 'P'
         AND SUBSTR(CONCAT('00', sln.place_service), -2) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
             THEN NULL
        WHEN clm.claim_type_code = 'I'
         AND SUBSTR(COALESCE(clm.type_bill, 'X'), 1, 1) IN ('3', 'X')
             THEN NULL
        WHEN 0 <> LENGTH(TRIM(CONCAT
                                (
                                    COALESCE(clmnms.attending_name1, ''), 
                                    COALESCE(clmnms.attending_name2, '')
                                ))) 
             THEN TRIM(CONCAT
                        (
                            COALESCE(clmnms.attending_name1, ''), 
                            COALESCE(clmnms.attending_name2, '')
                        )) 
        ELSE NULL
	END																				    	AS prov_rendering_name_1,
	clm.rendering_taxonomy																	AS prov_rendering_std_taxonomy,
	clm.prov_specialty																		AS prov_rendering_vendor_specialty,
	/* prov_billing_tax_id */
	CASE
	    WHEN COALESCE(clm.claim_type_code, 'X') NOT IN ('I', 'P')
	         THEN NULL
        WHEN clm.claim_type_code = 'P'
         AND SUBSTR(CONCAT('00', sln.place_service), -2) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
             THEN NULL
        WHEN clm.claim_type_code = 'I'
         AND SUBSTR(COALESCE(clm.type_bill, 'X'), 1, 1) IN ('3', 'X')
             THEN NULL
        ELSE clm.billing_pr_id
	END						    															AS prov_billing_tax_id,
	/* prov_billing_ssn */
	CASE
	    WHEN COALESCE(clm.claim_type_code, 'X') NOT IN ('I', 'P')
	         THEN NULL
        WHEN clm.claim_type_code = 'P'
         AND SUBSTR(CONCAT('00', sln.place_service), -2) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
             THEN NULL
        WHEN clm.claim_type_code = 'I'
         AND SUBSTR(COALESCE(clm.type_bill, 'X'), 1, 1) IN ('3', 'X')
             THEN NULL
        ELSE clm.billing_ssn
	END						    															AS prov_billing_ssn,
	/* prov_billing_state_license */
	CASE
	    WHEN COALESCE(clm.claim_type_code, 'X') NOT IN ('I', 'P')
	         THEN NULL
        WHEN clm.claim_type_code = 'P'
         AND SUBSTR(CONCAT('00', sln.place_service), -2) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
             THEN NULL
        WHEN clm.claim_type_code = 'I'
         AND SUBSTR(COALESCE(clm.type_bill, 'X'), 1, 1) IN ('3', 'X')
             THEN NULL
        ELSE clm.billing_state_lic
	END						    															AS prov_billing_state_license,
	/* prov_billing_upin */
	CASE
	    WHEN COALESCE(clm.claim_type_code, 'X') NOT IN ('I', 'P')
	         THEN NULL
        WHEN clm.claim_type_code = 'P'
         AND SUBSTR(CONCAT('00', sln.place_service), -2) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
             THEN NULL
        WHEN clm.claim_type_code = 'I'
         AND SUBSTR(COALESCE(clm.type_bill, 'X'), 1, 1) IN ('3', 'X')
             THEN NULL
        ELSE clm.billing_upin
	END						    															AS prov_billing_upin,
	/* prov_billing_name_1 */
	CASE
	    WHEN COALESCE(clm.claim_type_code, 'X') NOT IN ('I', 'P')
	         THEN NULL
        WHEN clm.claim_type_code = 'P'
         AND SUBSTR(CONCAT('00', sln.place_service), -2) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
             THEN NULL
        WHEN clm.claim_type_code = 'I'
         AND SUBSTR(COALESCE(clm.type_bill, 'X'), 1, 1) IN ('3', 'X')
             THEN NULL
        WHEN 0 <> LENGTH(TRIM(CONCAT
                                (
                                    COALESCE(clmnms.billing_name1, ''),
                                    COALESCE(clmnms.billing_name2, '')
                                ))) 
             THEN TRIM(CONCAT
                        (
                            COALESCE(clmnms.billing_name1, ''), 
                            COALESCE(clmnms.billing_name2, '')
                        )) 
        ELSE NULL
	END						    															AS prov_billing_name_1,
	/* prov_billing_address_1 */
	CASE
	    WHEN COALESCE(clm.claim_type_code, 'X') NOT IN ('I', 'P')
	         THEN NULL
        WHEN clm.claim_type_code = 'P'
         AND SUBSTR(CONCAT('00', sln.place_service), -2) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
             THEN NULL
        WHEN clm.claim_type_code = 'I'
         AND SUBSTR(COALESCE(clm.type_bill, 'X'), 1, 1) IN ('3', 'X')
             THEN NULL
        ELSE clm.billing_adr_line1
	END						    															AS prov_billing_address_1,
	/* prov_billing_address_2 */
	CASE
	    WHEN COALESCE(clm.claim_type_code, 'X') NOT IN ('I', 'P')
	         THEN NULL
        WHEN clm.claim_type_code = 'P'
         AND SUBSTR(CONCAT('00', sln.place_service), -2) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
             THEN NULL
        WHEN clm.claim_type_code = 'I'
         AND SUBSTR(COALESCE(clm.type_bill, 'X'), 1, 1) IN ('3', 'X')
             THEN NULL
        ELSE clm.billing_adr_line2
	END						    															AS prov_billing_address_2,
	/* prov_billing_city */
	CASE
	    WHEN COALESCE(clm.claim_type_code, 'X') NOT IN ('I', 'P')
	         THEN NULL
        WHEN clm.claim_type_code = 'P'
         AND SUBSTR(CONCAT('00', sln.place_service), -2) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
             THEN NULL
        WHEN clm.claim_type_code = 'I'
         AND SUBSTR(COALESCE(clm.type_bill, 'X'), 1, 1) IN ('3', 'X')
             THEN NULL
        ELSE clm.billing_adr_city
	END						    															AS prov_billing_city,
	/* prov_billing_state */
	VALIDATE_STATE_CODE
	    (
        	CASE
        	    WHEN COALESCE(clm.claim_type_code, 'X') NOT IN ('I', 'P')
        	         THEN NULL
                WHEN clm.claim_type_code = 'P'
                 AND SUBSTR(CONCAT('00', sln.place_service), -2) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
                     THEN NULL
                WHEN clm.claim_type_code = 'I'
                 AND SUBSTR(COALESCE(clm.type_bill, 'X'), 1, 1) IN ('3', 'X')
                     THEN NULL
                ELSE clm.billing_adr_state
        	END
	    )   																				AS prov_billing_state,
	/* prov_billing_zip */
	CASE
	    WHEN COALESCE(clm.claim_type_code, 'X') NOT IN ('I', 'P')
	         THEN NULL
        WHEN clm.claim_type_code = 'P'
         AND SUBSTR(CONCAT('00', sln.place_service), -2) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
             THEN NULL
        WHEN clm.claim_type_code = 'I'
         AND SUBSTR(COALESCE(clm.type_bill, 'X'), 1, 1) IN ('3', 'X')
             THEN NULL
        ELSE clm.billing_adr_zip
	END						    															AS prov_billing_zip,
	clm.billing_taxonomy																	AS prov_billing_std_taxonomy,
	/* prov_referring_name_1 */
	CASE
	    WHEN COALESCE(clm.claim_type_code, 'X') NOT IN ('I', 'P')
	         THEN NULL
        WHEN clm.claim_type_code = 'P'
         AND SUBSTR(CONCAT('00', sln.place_service), -2) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
             THEN NULL
        WHEN clm.claim_type_code = 'I'
         AND SUBSTR(COALESCE(clm.type_bill, 'X'), 1, 1) IN ('3', 'X')
             THEN NULL
        WHEN 0 <> LENGTH(TRIM(CONCAT
                                (
                                    COALESCE(clmnms.referring_name1, ''), 
                                    COALESCE(clmnms.referring_name2, '')
                                )))
             THEN TRIM(CONCAT
                        (
                            COALESCE(clmnms.referring_name1, ''), 
                            COALESCE(clmnms.referring_name2, '')
                        ))
        ELSE NULL
	END						    															AS prov_referring_name_1,
	/* prov_facility_state_license */
	CASE
	    WHEN COALESCE(clm.claim_type_code, 'X') NOT IN ('I', 'P')
	         THEN NULL
        WHEN clm.claim_type_code = 'P'
         AND SUBSTR(CONCAT('00', sln.place_service), -2) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
             THEN NULL
        WHEN clm.claim_type_code = 'I'
         AND SUBSTR(COALESCE(clm.type_bill, 'X'), 1, 1) IN ('3', 'X')
             THEN NULL
        ELSE clm.facility_state_lic
	END						    															AS prov_facility_state_license,
	/* prov_facility_name_1 */
	CASE
	    WHEN COALESCE(clm.claim_type_code, 'X') NOT IN ('I', 'P')
	         THEN NULL
        WHEN clm.claim_type_code = 'P'
         AND SUBSTR(CONCAT('00', sln.place_service), -2) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
             THEN NULL
        WHEN clm.claim_type_code = 'I'
         AND SUBSTR(COALESCE(clm.type_bill, 'X'), 1, 1) IN ('3', 'X')
             THEN NULL
        WHEN 0 <> LENGTH(TRIM(CONCAT
                                (
                                    COALESCE(clmnms.facility_name1, ''), 
                                    COALESCE(clmnms.facility_name2, '')
                                ))) 
             THEN TRIM(CONCAT
                        (
                            COALESCE(clmnms.facility_name1, ''), 
                            COALESCE(clmnms.facility_name2, '')
                        ))
        ELSE NULL
	END						    															AS prov_facility_name_1,
	/* prov_facility_address_1 */
	CASE
	    WHEN COALESCE(clm.claim_type_code, 'X') NOT IN ('I', 'P')
	         THEN NULL
        WHEN clm.claim_type_code = 'P'
         AND SUBSTR(CONCAT('00', sln.place_service), -2) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
             THEN NULL
        WHEN clm.claim_type_code = 'I'
         AND SUBSTR(COALESCE(clm.type_bill, 'X'), 1, 1) IN ('3', 'X')
             THEN NULL
        ELSE clm.facility_adr_line1
	END						    															AS prov_facility_address_1,
	/* prov_facility_address_2 */
	CASE
	    WHEN COALESCE(clm.claim_type_code, 'X') NOT IN ('I', 'P')
	         THEN NULL
        WHEN clm.claim_type_code = 'P'
         AND SUBSTR(CONCAT('00', sln.place_service), -2) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
             THEN NULL
        WHEN clm.claim_type_code = 'I'
         AND SUBSTR(COALESCE(clm.type_bill, 'X'), 1, 1) IN ('3', 'X')
             THEN NULL
        ELSE clm.facility_adr_line2
	END						    															AS prov_facility_address_2,
	/* prov_facility_city */
	CASE
	    WHEN COALESCE(clm.claim_type_code, 'X') NOT IN ('I', 'P')
	         THEN NULL
        WHEN clm.claim_type_code = 'P'
         AND SUBSTR(CONCAT('00', sln.place_service), -2) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
             THEN NULL
        WHEN clm.claim_type_code = 'I'
         AND SUBSTR(COALESCE(clm.type_bill, 'X'), 1, 1) IN ('3', 'X')
             THEN NULL
        ELSE clm.facility_adr_city
	END						    															AS prov_facility_city,
	/* prov_facility_state */
	VALIDATE_STATE_CODE
	    (
        	CASE
        	    WHEN COALESCE(clm.claim_type_code, 'X') NOT IN ('I', 'P')
        	         THEN NULL
                WHEN clm.claim_type_code = 'P'
                 AND SUBSTR(CONCAT('00', sln.place_service), -2) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
                     THEN NULL
                WHEN clm.claim_type_code = 'I'
                 AND SUBSTR(COALESCE(clm.type_bill, 'X'), 1, 1) IN ('3', 'X')
                     THEN NULL
                ELSE clm.facility_adr_state
        	END
	    )   																				AS prov_facility_state,
	/* prov_facility_zip */
	CASE
	    WHEN COALESCE(clm.claim_type_code, 'X') NOT IN ('I', 'P')
	         THEN NULL
        WHEN clm.claim_type_code = 'P'
         AND SUBSTR(CONCAT('00', sln.place_service), -2) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
             THEN NULL
        WHEN clm.claim_type_code = 'I'
         AND SUBSTR(COALESCE(clm.type_bill, 'X'), 1, 1) IN ('3', 'X')
             THEN NULL
        ELSE clm.facility_adr_zip
	END						    															AS prov_facility_zip,
	/* Changed from pay.pcn to clm.claim_number JKS 12/5/2019 */
	clm.claim_number                                                                        AS medical_claim_link_text,	
	'navicure'																				AS part_provider,
	/* part_best_date */
	CASE
	    WHEN CAP_DATE
	            (
	                CAST(EXTRACT_DATE(mmd.min_claim_date, '%Y%m%d') AS DATE),
                    CAST('{EARLIEST_SERVICE_DATE}' AS DATE),
                    CAST('{VDR_FILE_DT}' AS DATE)
	            ) IS NULL
	         THEN '0_PREDATES_HVM_HISTORY'
	    ELSE CONCAT
	            (
	                SUBSTR(mmd.min_claim_date, 1, 4), '-',
	                SUBSTR(mmd.min_claim_date, 5, 2), '-01'
	            )
	END 																					AS part_best_date
 FROM waystar_dedup_claims clm
 /* Link to the first service line for place of service. */
 LEFT OUTER JOIN waystar_dedup_lines sln
   ON clm.claim_number = sln.claim_number
  AND sln.line_number = '1'
 LEFT OUTER JOIN matching_payload pay 
   ON clm.hvjoinkey = pay.hvjoinkey
 /* Deduplicate the source name columns without trimming and nullifying. */
 /* The source columns sometimes contain trailing blanks (1) and leading */
 /* blanks (2) that are part of the full provider name. */
 LEFT OUTER JOIN
    (
        SELECT DISTINCT
            claim_number,
            attending_name1,
            attending_name2,
            billing_name1,
            billing_name2,
            referring_name1,
            referring_name2,
            facility_name1,
            facility_name2
         FROM claims
    ) clmnms
   ON clm.claim_number = clmnms.claim_number
 LEFT OUTER JOIN
/* Get the min and max dates for each claim_number. */
(
    SELECT
        sub.claim_number,
        MIN(sub.min_dt) AS min_claim_date,
        MAX(sub.max_dt) AS max_claim_date
     FROM
    (
        SELECT
            sln1.claim_number,
            sln1.service_from AS min_dt,
            sln1.service_to AS max_dt
         FROM waystar_dedup_lines sln1
        UNION ALL
        SELECT
            clm1.claim_number,
            clm1.statement_from AS min_dt,
            clm1.statement_to AS max_dt
         FROM waystar_dedup_claims clm1
    ) sub
    GROUP BY 1
) mmd
   ON clm.claim_number = mmd.claim_number

CROSS JOIN (SELECT EXPLODE(ARRAY(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)) AS n) proc_explode
WHERE NOT EXISTS
    (
        SELECT 1
         FROM waystar_norm01_norm_lines nml
        WHERE clm.claim_number = nml.claim_id
          AND COALESCE(nml.procedure_code, 'x') = 
              COALESCE
                (
                    CLEAN_UP_PROCEDURE_CODE
                        (
                            ARRAY
                                (
                                    clm.principal_procedure,
                                    clm.other_proc_code_2,
                                    clm.other_proc_code_3,
                                    clm.other_proc_code_4,
                                    clm.other_proc_code_5,
                                    clm.other_proc_code_6,
                                    clm.initial_procedure,
                                    clm.other_proc_code_7,
                                    clm.other_proc_code_8,
                                    clm.other_proc_code_9,
                                    clm.other_proc_code_10
                                )[proc_explode.n]
                        ), 'EMPTY'
                )

    )
---------- Procedure code explosion
  AND ARRAY
        (
            clm.principal_procedure,
            clm.other_proc_code_2,
            clm.other_proc_code_3,
            clm.other_proc_code_4,
            clm.other_proc_code_5,
            clm.other_proc_code_6,
            clm.initial_procedure,
            clm.other_proc_code_7,
            clm.other_proc_code_8,
            clm.other_proc_code_9,
            clm.other_proc_code_10
        )[proc_explode.n] IS NOT NULL
/* Eliminate claims loaded from Navicure source data. */
  AND NOT EXISTS
    (
        SELECT 1
         FROM waystar_medicalclaims_augment_comb aug
        WHERE clm.claim_number = aug.instanceid
    )
