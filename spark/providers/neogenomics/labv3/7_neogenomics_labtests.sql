SELECT DISTINCT
    MONOTONICALLY_INCREASING_ID()                                                           AS record_id,
    CONCAT
        (
            txn.accession_number_hashed, '_',
            txn.case_number_hashed
        )                                                                                   AS claim_id,
    pay.hvid                                                                                AS hvid,
    CURRENT_DATE()                                                                          AS created,
	'07'                                                                                    AS model_version,
    txn.input_file_name                                                                     AS data_set,
	'32'                                                                                    AS data_feed,
	'78'                                                                                    AS data_vendor,
	/* patient_gender */
    CASE
        WHEN UPPER(pay.gender) IN ('F', 'M')
            THEN UPPER(pay.gender)
        ELSE 'U'
    END                                                                                     AS patient_gender,
	/* patient_age */
	CAP_AGE
	    (
        	VALIDATE_AGE
        	    (
                    pay.age,
                      CAST(
                        COALESCE
                            (
                                EXTRACT_DATE(txn.test_created_date, '%m/%d/%Y'),
                                EXTRACT_DATE(txn.vendor_file_date, '%Y-%m-%d'),
                                EXTRACT_DATE('1900-01-01', '%Y-%m-%d')
                            ) AS DATE),
                    pay.yearofbirth
        	    )
        )                                                                                   AS patient_age,
	/* patient_year_of_birth */
	CAP_YEAR_OF_BIRTH
        (
            pay.age,
            CAST(
                COALESCE
                    (
                        EXTRACT_DATE(txn.test_created_date, '%m/%d/%Y'),
                        EXTRACT_DATE(txn.vendor_file_date, '%Y-%m-%d'),
                        EXTRACT_DATE('1900-01-01', '%Y-%m-%d')
                    ) AS DATE),
            pay.yearofbirth
        )                                                                                   AS patient_year_of_birth,
    /* patient_zip3 */
    MASK_ZIP_CODE
        (
            SUBSTR(pay.threedigitzip, 1, 3)
        )                                                                                   AS patient_zip3,
    /* patient_state */
    VALIDATE_STATE_CODE
        (
            UPPER(COALESCE(pay.state, ''))
        )                                                                                   AS patient_state,
    /* date_service */
	CAP_DATE
        (
            CAST(EXTRACT_DATE(txn.test_created_date, '%m/%d/%Y') AS DATE),
            CAST(esdt.gen_ref_1_dt AS DATE),
            CAST(EXTRACT_DATE(txn.vendor_file_date, '%Y-%m-%d') AS DATE)
        )                                                                                   AS date_service,
    /* date_specimen */
 	CAP_DATE
        (
            CAST(EXTRACT_DATE(txn.specimen_collection_date, '%m/%d/%Y') AS DATE),
            CAST(esdt.gen_ref_1_dt AS DATE),
            CAST(EXTRACT_DATE(txn.vendor_file_date, '%Y-%m-%d') AS DATE)
        )                                                                                   AS date_specimen,
    /* date_report */
 	CAP_DATE
        (
            CAST(EXTRACT_DATE(COALESCE(txn.test_cancellation_date, txn.test_report_date), '%m/%d/%Y') AS DATE),
            CAST(esdt.gen_ref_1_dt AS DATE),
            CAST(EXTRACT_DATE(txn.vendor_file_date, '%Y-%m-%d') AS DATE)
        )                                                                                   AS date_report,
    /* test_id */
    txn.test_orderid_hashed                                                                 AS test_id,
    /* test_battery_local_id */
    txn.panel_code                                                                          AS test_battery_local_id,
    /* test_battery_name   */
    txn.panel_name                                                                          AS test_battery_name,
    /* test_ordered_local_id */
    txn.test_code                                                                           AS test_ordered_local_id,
    /* test_ordered_name */
    CASE
    	WHEN txn.body_site IS NOT NULL AND txn.specimen_type IS NOT NULL
         AND txn.test_name IS NOT NULL AND txn.methodology IS NOT NULL
            THEN CONCAT('Body Site: ', UPPER(txn.body_site), ' | Specimen Type: ', UPPER(txn.specimen_type), ' | Test Name: ', UPPER(txn.test_name), ' | Methodology: ', UPPER(txn.methodology))
    	WHEN txn.body_site IS NOT NULL
         AND txn.test_name IS NOT NULL AND txn.methodology IS NOT NULL
            THEN CONCAT('Body Site: ', UPPER(txn.body_site), ' | Test Name: ', UPPER(txn.test_name), ' | Methodology: ', UPPER(txn.methodology))
    	WHEN txn.specimen_type IS NOT NULL
         AND txn.test_name IS NOT NULL AND txn.methodology IS NOT NULL
            THEN CONCAT('Specimen Type: ', UPPER(txn.specimen_type), ' | Test Name: ', UPPER(txn.test_name), ' | Methodology: ', UPPER(txn.methodology))
    	WHEN txn.test_name IS NOT NULL AND txn.methodology IS NOT NULL
            THEN CONCAT('Test Name: ', UPPER(txn.test_name), ' | Methodology: ', UPPER(txn.methodology))
    	WHEN txn.test_name IS NOT NULL
            THEN CONCAT('Test Name: ', UPPER(txn.test_name))
    	WHEN txn.methodology IS NOT NULL
            THEN CONCAT('Methodology: ', UPPER(txn.methodology))
    ELSE NULL
    END                                                                                     AS test_ordered_name,
     /* result */
    res.result_value                                                                        AS result,
     /* result_name */
    res.result_code                                                                         AS result_name,
     /* result_unit_of_measure */
    res.result_units_of_measure                                                             AS result_unit_of_measure,
     /* result_desc */
    res.result_name                                                                         AS result_desc,
     /* result_comments */
    CASE
        WHEN res.result_comments IS NOT NULL AND txn.level_of_service IS NOT NULL
            THEN CONCAT('Comments: ', res.result_comments, ' | Level of Service: ', txn.level_of_service)
        WHEN res.result_comments IS NOT NULL
            THEN CONCAT('Comments: ', res.result_comments)
        WHEN txn.level_of_service IS NOT NULL
            THEN CONCAT('Level of Service: ', txn.level_of_service)
    ELSE NULL
    END                                                                                     AS result_comments,
     /* ref_range_alpha */
    res.result_reference_range                                                              AS ref_range_alpha,
     /* diagnosis_code */
    CLEAN_UP_DIAGNOSIS_CODE(
                                txn.billing_icd_codes,
                                NULL,
                                CAP_DATE
                                (
                                    CAST(EXTRACT_DATE(txn.test_created_date, '%m/%d/%Y') AS DATE),
                                    CAST(esdt.gen_ref_1_dt AS DATE),
                                    CAST(EXTRACT_DATE(txn.vendor_file_date, '%Y-%m-%d') AS DATE)
                                )
                            )                                                               AS diagnosis_code,
    /* diagnosis_code_priority */
    CASE WHEN txn.diagnosis_priority IS NOT NULL
            THEN 1 + txn.diagnosis_priority
    ELSE NULL
    END                                                                                     AS diagnosis_code_priority,
    /* procedure_code */
    CLEAN_UP_PROCEDURE_CODE(txn.cpt_code)                                                   AS procedure_code,
     /* procedure_code_qual */
    CASE
        WHEN 0 <> LENGTH(COALESCE(txn.cpt_code,''))
            THEN 'HC'
    ELSE NULL
    END                                                                                     AS procedure_code_qual,
     /* procedure_modifier_1 */
    SUBSTR(UPPER(CLEAN_UP_ALPHANUMERIC_CODE(txn.cpt_modifier_id)), 1, 2)                    AS procedure_modifier_1,
     /* ordering_npi */
    CLEAN_UP_NPI_CODE(txn.ordering_provider_npi_number )                                    AS ordering_npi,
     /* payer_id */
    txn.insurance_company_id                                                                AS payer_id,
     /* payer_id_qual */
    CASE
        WHEN txn.insurance_company_id IS NOT NULL
            THEN 'INSURANCE_COMPANY_ID'
    ELSE NULL
    END                                                                                     AS payer_id_qual,
     /* payer_name */
    txn.insurance_company_name                                                              AS payer_name,
     /* payer_type */
    txn.bill_type		                                                                    AS payer_type,
     /* lab_other_id */
    txn.performing_organization_name                                                        AS lab_other_id,
      /* lab_other_qual */
    CASE
        WHEN txn.performing_organization_name IS NOT NULL
            THEN 'PERFORMING_ORGANIZATION_NAME'
    ELSE NULL
    END                                                                                     AS lab_other_qual,
     /* ordering_other_id */
    txn.ordering_practice_name                                                              AS ordering_other_id,
     /* ordering_other_qual */
    CASE
        WHEN txn.ordering_practice_name IS NOT NULL
            THEN 'ORDERING_PRACTICE_NAME'
    ELSE NULL
    END                                                                                   AS ordering_other_qual,
     /* ordering_name */
    CASE
        WHEN txn.ordering_provider_last_name IS NOT NULL
         AND txn.ordering_provider_first_name IS NOT NULL
            THEN CONCAT(txn.ordering_provider_last_name, ', ', txn.ordering_provider_first_name)
    ELSE txn.ordering_provider_last_name
    END                                                                                   AS ordering_name,
     /* ordering_address_1 */
    txn.ordering_practice_address_line_1                                                  AS ordering_address_1,
     /* ordering_city */
    txn.ordering_practice_city                                                            AS ordering_city,
     /* ordering_state */
    VALIDATE_STATE_CODE(txn.ordering_practice_state)                                      AS ordering_state,
     /* ordering_zip */
    txn.ordering_practice_zip_code                                                        AS ordering_zip,
     /* logical_delete_reason */
    CASE
        WHEN UPPER(txn.test_status) IN ('CANCELLED', 'CANCELED')
            THEN 'CANCELED'
    ELSE NULL
    END                                                                                   AS logical_delete_reason,
     /* part_provider */
	'neogenomics'                                                                          AS part_provider,
    /* part_best_date */
	CASE
	    WHEN 0 = LENGTH(TRIM(COALESCE
	                            (
	                                CAP_DATE
                                        (
                                            CAST(EXTRACT_DATE(txn.test_created_date, '%m/%d/%Y')  AS DATE),
                                            CAST(COALESCE(ahdt.gen_ref_1_dt, esdt.gen_ref_1_dt) AS DATE),
                                            CAST(EXTRACT_DATE(txn.vendor_file_date, '%Y-%m-%d') AS DATE)
                                        ),
                                    ''
                                )))
	        THEN '0_PREDATES_HVM_HISTORY'
	    ELSE CONCAT
	            (
                    SUBSTR(txn.test_created_date, 7, 4), '-',
                    SUBSTR(txn.test_created_date, 1, 2), '-01'
                )
	END                                                                                 AS part_best_date
 FROM neogenomics_meta_dedup txn
 LEFT OUTER JOIN neogenomics_results_dedup res
        ON txn.test_orderid_hashed = res.test_orderid_hashed
 LEFT OUTER JOIN neogenomics_payload_lag pay
        ON TRIM(UPPER(txn.patientid_hashed)) = TRIM(UPPER(pay.personid))
        AND txn.vendor_file_date > pay.prev_vendor_file_date
        AND txn.vendor_file_date <= pay.vendor_file_date
 LEFT OUTER JOIN
    (
        SELECT gen_ref_1_dt
         FROM ref_gen_ref
        WHERE hvm_vdr_feed_id = 32
          AND gen_ref_domn_nm = 'EARLIEST_VALID_SERVICE_DATE'
        LIMIT 1
    ) esdt
   ON 1 = 1
 LEFT OUTER JOIN
    (
        SELECT gen_ref_1_dt
         FROM ref_gen_ref
        WHERE hvm_vdr_feed_id = 32
          AND gen_ref_domn_nm = 'HVM_AVAILABLE_HISTORY_START_DATE'
        LIMIT 1
    ) ahdt
   ON 1 = 1
