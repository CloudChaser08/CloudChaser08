SELECT
    CAST(MONOTONICALLY_INCREASING_ID() AS STRING)                                           AS record_id,
    /* MD5 added on 5/16/19 */
    MD5(txn.med_phm_fill_fid)                                                               AS claim_id,
    obfuscate_hvid(pay.hvid, {salt})                                                        AS hvid,
    CURRENT_DATE()                                                                          AS created,
	'09'                                                                                    AS model_version,
    SPLIT(txn.input_file_name, '/')[SIZE(SPLIT(txn.input_file_name, '/')) - 1]              AS data_set,
	'86'                                                                                    AS data_feed,
	'337'                                                                                   AS data_vendor,
	/* patient_gender */
	CLEAN_UP_GENDER
    	(
        	CASE
        	    WHEN SUBSTR(UPPER(TRIM(COALESCE(txn.gender_code, 'U'))), 1, 1) IN ('F', 'M')
        	        THEN SUBSTR(UPPER(TRIM(COALESCE(txn.gender_code, 'U'))), 1, 1)
        	    WHEN SUBSTR(UPPER(TRIM(COALESCE(pay.gender, 'U'))), 1, 1) IN ('F', 'M')
        	        THEN SUBSTR(UPPER(TRIM(COALESCE(pay.gender, 'U'))), 1, 1)
        	    ELSE 'U'
        	END
	    )                                                                                   AS patient_gender,
	/* patient_age */
	VALIDATE_AGE
        (
            COALESCE(txn.patient_age, pay.age),
            CAST(EXTRACT_DATE(COALESCE(txn.date_sold, txn.transaction_date), '%Y%m%d') AS DATE),
            COALESCE(txn.year_of_birth, pay.yearofbirth)
        )                                                                                   AS patient_age,
	/* patient_year_of_birth */
	CAP_YEAR_OF_BIRTH
        (
            COALESCE(txn.patient_age, pay.age),
            CAST(EXTRACT_DATE(COALESCE(txn.date_sold, txn.transaction_date), '%Y%m%d') AS DATE),
            COALESCE(txn.year_of_birth, pay.yearofbirth)
        )                                                                                   AS patient_year_of_birth,
    /* patient_zip3 */
    MASK_ZIP_CODE
        (
            SUBSTR(COALESCE(txn.address_postal_code, pay.threedigitzip), 1, 3)
        )                                                                                   AS patient_zip3,
    /* patient_state */
    VALIDATE_STATE_CODE
        (
            UPPER(COALESCE(txn.address_state_prov_code, pay.state, ''))
        )                                                                                   AS patient_state,
    /* Laurie 6/12/19: Eliminated min- and max-capping for these dates. */
    /* Changed date_service to map directly from date_sold, instead of  */
    /* being a COALESCE of date_sold and transaction_date.              */
	CAST(EXTRACT_DATE(txn.date_sold, '%Y%m%d') AS DATE)                                     AS date_service,
    CAST(EXTRACT_DATE(txn.date_ordered, '%Y%m%d') AS DATE)                                  AS date_written,
	CAST(EXTRACT_DATE(txn.transaction_date, '%Y%m%d') AS DATE)                              AS date_authorized,
    txn.fill_status_code                                                                    AS transaction_code_vendor,
    /* ndc_code */
    CLEAN_UP_NDC_CODE
        (
            COALESCE(txn.dispensed_product_ndc, txn.prescribed_product_ndc)
        )                                                                                   AS ndc_code,
    MD5(txn.rx_number)                                                                      AS rx_number,
	/* rx_number_qual */
	CASE
	    WHEN 0 <> LENGTH(TRIM(COALESCE(txn.rx_number, '')))
            THEN 'RX_NUMBER'
        ELSE NULL
    END                                                                                     AS rx_number_qual,
	/* bin_number */
	ARRAY
        (
            txn.primary_bank_industry_number_code,
            txn.secondary_bank_industry_number_code
        )[expl_pcn.n]                                                                       AS bin_number,
	/* processor_control_number */
	ARRAY
        (
            txn.primary_processor_control_number_code,
            txn.secondary_processor_control_number_code
        )[expl_pcn.n]                                                                       AS processor_control_number,
    CAST(txn.refill_sequence_number AS INTEGER)                                             AS fill_number,
    txn.refill_authorization_amount                                                         AS refill_auth_amount,
    CAST(txn.dispensed_quantity AS FLOAT)                                                   AS dispensed_quantity,
    CAST(txn.actual_duration AS INTEGER)                                                    AS days_supply,
    CLEAN_UP_NPI_CODE(txn.pharmacy_code)                                                    AS pharmacy_npi,
    txn.payer_type                                                                          AS payer_type,
    /* compound_code */
    CASE
        WHEN SUBSTR(UPPER(COALESCE
                            (
                                txn.dispensed_product_compound_code,
                                txn.prescribed_product_compound_code,
                                ''
                            )), 1, 1) = 'N'
            THEN '1'
        WHEN SUBSTR(UPPER(COALESCE
                            (
                                txn.dispensed_product_compound_code,
                                txn.prescribed_product_compound_code,
                                ''
                            )), 1, 1) = 'Y'
            THEN '2'
        ELSE NULL
    END                                                                                     AS compound_code,
    /* dispensed_as_written */
    CASE
        WHEN CLEAN_UP_ALPHANUMERIC_CODE(txn.dispense_as_written_description) =
             CLEAN_UP_ALPHANUMERIC_CODE('NO PRODUCT SELECTION INDICATED')
            THEN '0'
        WHEN CLEAN_UP_ALPHANUMERIC_CODE(txn.dispense_as_written_description) =
             CLEAN_UP_ALPHANUMERIC_CODE('SUBSTITUTION NOT ALLOWED BY PRESCRIBER')
            THEN '1'
        WHEN CLEAN_UP_ALPHANUMERIC_CODE(txn.dispense_as_written_description) =
             CLEAN_UP_ALPHANUMERIC_CODE('SUBSTITUTION ALLOWED-PATIENT REQUESTED PRODUCT DISPENSED')
            THEN '2'
        WHEN CLEAN_UP_ALPHANUMERIC_CODE(txn.dispense_as_written_description) =
             CLEAN_UP_ALPHANUMERIC_CODE('SUBSTITUTION ALLOWED-PHARMACIST SELECTED PRODUCT DISPENSED')
            THEN '3'
        WHEN CLEAN_UP_ALPHANUMERIC_CODE(txn.dispense_as_written_description) =
             CLEAN_UP_ALPHANUMERIC_CODE('SUBSTITUTION ALLOWED-GENERIC DRUG NOT IN STOCK')
            THEN '4'
        WHEN CLEAN_UP_ALPHANUMERIC_CODE(txn.dispense_as_written_description) =
             CLEAN_UP_ALPHANUMERIC_CODE('SUBSTITUTION ALLOWED-BRAND DRUG DISPENSED AS A GENERIC')
            THEN '5'
        WHEN CLEAN_UP_ALPHANUMERIC_CODE(txn.dispense_as_written_description) =
             CLEAN_UP_ALPHANUMERIC_CODE('OVERRIDE')
            THEN '6'
        WHEN CLEAN_UP_ALPHANUMERIC_CODE(txn.dispense_as_written_description) =
             CLEAN_UP_ALPHANUMERIC_CODE('SUBSTITUTION NOT ALLOWED-BRAND DRUG MANDATED BY LAW')
            THEN '7'
        /* Non-standard text value received from 84.51. */
        WHEN CLEAN_UP_ALPHANUMERIC_CODE(txn.dispense_as_written_description) =
             CLEAN_UP_ALPHANUMERIC_CODE('SUBSTITUTION ALLOWED-GENERIC DRUG NOT AVAILABLE IN THE MARKETPLACE')
            THEN '8'
        /* Standard NCPDP text value, just in case. */
        WHEN CLEAN_UP_ALPHANUMERIC_CODE(txn.dispense_as_written_description) =
             CLEAN_UP_ALPHANUMERIC_CODE('SUBSTITUTION ALLOWED-GENERIC DRUG NOT AVAILABLE IN MARKETPLACE')
            THEN '8'
        WHEN CLEAN_UP_ALPHANUMERIC_CODE(txn.dispense_as_written_description) =
             CLEAN_UP_ALPHANUMERIC_CODE
                (
                    "SUBSTITUTION ALLOWED BY PRESCRIBER BUT PLAN REQUESTS BRAND - PATIENT'S PLAN REQUESTED BRAND PRODUCT TO BE DISPENSED"
                )
            THEN '9'
        ELSE NULL
    END                                                                                     AS dispensed_as_written,
    /* orig_prescribed_product_service_code */
    /* code added per mapping v02 5/16/19 */
    CLEAN_UP_NDC_CODE(txn.prescribed_product_ndc)                                           AS orig_prescribed_product_service_code,
	/* orig_prescribed_product_service_code_qual */
    /* code added per mapping v02 5/16/19 */
	CASE
	    WHEN 0 <> LENGTH(TRIM(COALESCE(txn.prescribed_product_ndc, '')))
            THEN '03'
        ELSE NULL
    END                                                                                     AS orig_prescribed_product_service_code_qual,
    txn.prescribed_quantity                                                                 AS orig_prescribed_quantity,
    CAST(NULL AS STRING)                                                                    AS prov_prescribing_npi,
    /* cob_count */
    CASE
        WHEN txn.primary_processor_control_number_code IS NULL
         AND txn.secondary_processor_control_number_code IS NULL
            THEN NULL
        WHEN txn.primary_processor_control_number_code IS NOT NULL
         AND txn.secondary_processor_control_number_code IS NOT NULL
            THEN '1'
        WHEN txn.primary_processor_control_number_code IS NOT NULL
          OR txn.secondary_processor_control_number_code IS NOT NULL
            THEN '0'
        ELSE NULL
    END                                                                                     AS cob_count,
    CAST(txn.total_pay_amt AS FLOAT)                                                        AS paid_gross_due,
    CAST(txn.total_patient_pay_amt AS FLOAT)                                                AS paid_patient_pay,
    txn.pharmacy_zip                                                                        AS pharmacy_postal_code,
	/* other_payer_coverage_type */
    /* code added per mapping v02 5/16/19 */
    CASE
        WHEN ARRAY
                (
                    COALESCE
                        (
                            txn.primary_processor_control_number_code,
                            txn.primary_bank_industry_number_code
                        ),
                    COALESCE
                        (
                            txn.secondary_processor_control_number_code,
                            txn.secondary_bank_industry_number_code
                        )
                )[expl_pcn.n] IS NOT NULL
            THEN ARRAY('01', '02')[expl_pcn.n]
        ELSE NULL
    END                                                                                     AS other_payer_coverage_type,
    /* -------------------------------------------- */
    /* Below fields are for the data provider only. */
    /* These fields are not uploaded in the DW.     */
    /* -------------------------------------------- */
    /* Laurie 6/12/19: Added a hash for this source column. */
    MD5(txn.med_phm_fill_code)                                                              AS med_phm_fill_code,
    txn.date_cancelled                                                                      AS date_cancelled,
    txn.product_prescribed		                                                            AS product_prescribed,
    txn.product_dispensed		                                                            AS product_dispensed,
    txn.generic_substitution_description		                                            AS generic_substitution_description,
    txn.intended_duration		                                                            AS intended_duration,
    txn.return_to_stock_flag		                                                        AS return_to_stock_flag,
    txn.partial_fill_code		                                                            AS partial_fill_code,
    txn.government_funded_flag		                                                        AS government_funded_flag,
    txn.cash_override_amt		                                                            AS cash_override_amt,
    txn.division_id		                                                                    AS division_id,
    txn.prescription_code		                                                            AS prescription_code,
    txn.prescribed_product_commodity		                                                AS prescribed_product_commodity,
    txn.prescribed_product_subcommodity		                                                AS prescribed_product_subcommodity,
    txn.prescribed_product_therapeutic_class_code		                                    AS prescribed_product_therapeutic_class_code,
    txn.prescribed_product_therapeutic_class_description		                            AS prescribed_product_therapeutic_class_description,
    txn.prescribed_product_generic_flag		                                                AS prescribed_product_generic_flag,
    txn.prescribed_product_generic_name		                                                AS prescribed_product_generic_name,
    txn.prescribed_product_maintenance_drug_flag		                                    AS prescribed_product_maintenance_drug_flag,
   /* code added per mapping v02 5/16/19 */
    txn.prescribed_product_compound_code                                                    AS prescribed_product_compound_code,
    txn.prescribed_product_strength_code		                                            AS prescribed_product_strength_code,
    txn.prescribed_product_strength_quantity		                                        AS prescribed_product_strength_quantity,
    txn.dispensed_product_description		                                                AS dispensed_product_description,
    txn.dispensed_product_commodity		                                                    AS dispensed_product_commodity,
    txn.dispensed_product_subcommodity		                                                AS dispensed_product_subcommodity,
    txn.dispensed_product_therapeutic_class_code		                                    AS dispensed_product_therapeutic_class_code,
    txn.dispensed_product_therapeutic_class_description		                                AS dispensed_product_therapeutic_class_description,
    txn.dispensed_product_generic_flag		                                                AS dispensed_product_generic_flag,
    txn.dispensed_product_generic_name		                                                AS dispensed_product_generic_name,
    txn.dispensed_product_maintenance_drug_flag		                                        AS dispensed_product_maintenance_drug_flag,
    txn.dispensed_product_compound_code                                                     AS dispensed_product_compound_code,
    txn.dispensed_product_strength_code		                                                AS dispensed_product_strength_code,
    txn.dispensed_product_strength_quantity	    	                                        AS dispensed_product_strength_quantity,
    txn.pdc_diabetes		                                                                AS pdc_diabetes,
    txn.pdc_rasa		                                                                    AS pdc_rasa,
    txn.pdc_statins		                                                                    AS pdc_statins,
    MD5(txn.med_identity_id)                                                                AS med_identity_id
 FROM transaction_pdc_fix txn
 LEFT OUTER JOIN matching_payload pay
   ON txn.hvjoinkey = pay.hvjoinkey
 LEFT OUTER JOIN
    (
        SELECT gen_ref_1_dt
         FROM ref_gen_ref
        WHERE hvm_vdr_feed_id = 86
          AND gen_ref_domn_nm = 'EARLIEST_VALID_SERVICE_DATE'
        LIMIT 1
    ) esdt
   ON 1 = 1
 LEFT OUTER JOIN
    (
        SELECT gen_ref_1_dt
         FROM ref_gen_ref
        WHERE hvm_vdr_feed_id = 86
          AND gen_ref_domn_nm = 'HVM_AVAILABLE_HISTORY_START_DATE'
        LIMIT 1
    ) ahdt
   ON 1 = 1
CROSS JOIN (SELECT EXPLODE(ARRAY(0, 1)) AS n) expl_pcn
/* Processor Control Number explosion */
WHERE
    (
        (
            ARRAY
                (
                    COALESCE(txn.primary_bank_industry_number_code, txn.primary_processor_control_number_code),
                    COALESCE(txn.secondary_bank_industry_number_code, txn.secondary_processor_control_number_code)
                )[expl_pcn.n] IS NOT NULL
        )
    OR
        (
            COALESCE
                (
                    txn.primary_bank_industry_number_code,
                    txn.primary_processor_control_number_code,
                    txn.secondary_bank_industry_number_code,
                    txn.secondary_processor_control_number_code
                ) IS NULL
            AND expl_pcn.n =  0
        )
    )
/* Laurie 6/14/19: Removed the code that filters out rows where the patient state code is non-U.S. */
