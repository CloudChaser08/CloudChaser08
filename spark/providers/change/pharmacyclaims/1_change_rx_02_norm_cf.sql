SELECT
    --MONOTONICALLY_INCREASING_ID()                                                           AS record_id,
    txn.claim_id                                                                            AS claim_id,
    payload.hvid                                                                            AS hvid,
    CURRENT_DATE()                                                                          AS created,
	'11'                                                                                    AS model_version,
    SPLIT(txn.input_file_name, '/')[SIZE(SPLIT(txn.input_file_name, '/')) - 1]              AS data_set,
	'11'                                                                                    AS data_feed,
	'35'                                                                                    AS data_vendor,

    /* source_version */
    CAST(NULL AS STRING)                                                                    AS source_version,

	/* patient_gender */
	CLEAN_UP_GENDER
    	(
        	CASE
        	    WHEN SUBSTR(UPPER(COALESCE(txn.gender_code,'')), 1, 1) IN ('1', 'M') THEN 'M'
        	    WHEN SUBSTR(UPPER(COALESCE(txn.gender_code,'')), 1, 1) IN ('2', 'F') THEN 'F'
        	    WHEN SUBSTR(UPPER(COALESCE(payload.gender    ,'')), 1, 1) IN ('F', 'M')
        	        THEN SUBSTR(UPPER(COALESCE(payload.gender    , '')), 1, 1)
        	    ELSE 'U'
        	END
	    )                                                                                   AS patient_gender,

	/* patient_age */
	VALIDATE_AGE
        (
            payload.age,
            CAST(EXTRACT_DATE(txn.date_service, '%Y%m%d') AS DATE),
            COALESCE(YEAR(CAST(EXTRACT_DATE(txn.year_of_birth , '%Y%m%d') AS DATE)), payload.yearofbirth)
        )                                                                                   AS patient_age,

	/* patient_year_of_birth */
	CAP_YEAR_OF_BIRTH
        (
            payload.age,
            CAST(EXTRACT_DATE(txn.date_service, '%Y%m%d') AS DATE),
            COALESCE(YEAR(CAST(EXTRACT_DATE(txn.year_of_birth , '%Y%m%d') AS DATE)), payload.yearofbirth, YEAR(CAST(EXTRACT_DATE('1900-01-01', '%Y-%m-%d') AS DATE)))
        )                                                                                   AS patient_year_of_birth,

    /* patient_zip3 */
    MASK_ZIP_CODE(
        COALESCE(SUBSTR(payload.threedigitzip, 1, 3), SUBSTR(txn.patient_zip3, 1, 3))
        )                                                                                   AS patient_zip3,

    /* patient_state */
    VALIDATE_STATE_CODE(UPPER(COALESCE(txn.patient_state_province, payload.state, '')))     AS patient_state,

    /* date_service */
	CAP_DATE
        (
            CAST(EXTRACT_DATE(txn.date_service, '%Y%m%d') AS DATE),
            CAST('{EARLIEST_SERVICE_DATE}'  AS DATE),
            CAST('{VDR_FILE_DT}' AS DATE)
        )                                                                                   AS date_service,

    /* date_written */
	CAP_DATE
        (
            CAST(EXTRACT_DATE(txn.date_written, '%Y%m%d') AS DATE),
            CAST('{EARLIEST_SERVICE_DATE}'  AS DATE),
            CAST('{VDR_FILE_DT}' AS DATE)
        )                                                                                   AS date_written,

    /* year_of_injury */
	CAP_DATE
        (
            CAST(EXTRACT_DATE(txn.date_injury, '%Y%m%d') AS DATE),
            CAST('{EARLIEST_SERVICE_DATE}'  AS DATE),
            CAST('{VDR_FILE_DT}' AS DATE)
        )                                                                                   AS year_of_injury,

    /* date_authorized */
	CAP_DATE
        (
            CAST(EXTRACT_DATE(txn.date_authorized, '%Y%m%d') AS DATE),
            CAST('{EARLIEST_SERVICE_DATE}'  AS DATE),
            CAST('{VDR_FILE_DT}' AS DATE)
        )                                                                                   AS date_authorized,

    /* time_authorized */
    CASE WHEN length(txn.time_authorized) >= 4
            THEN concat_ws(':', substring(txn.time_authorized, 1, 2), substring(txn.time_authorized, 3, 2))
        ELSE NULL
    END                                                                                     AS time_authorized,

    /* discharge_date */
    CAST(NULL AS DATE)                                                                      AS discharge_date,

    /* transaction_code_std */
    txn.transaction_code_std                                                                AS transaction_code_std,

    /* transaction_code_vendor */
    CAST(NULL AS STRING)                                                                    AS transaction_code_vendor,

    /* response_code_std */
    txn.response_code_std                                                                   AS response_code_std,

    /* response_code_vendor */
    CAST(NULL AS STRING)                                                                    AS response_code_vendor,

    /* reject_reason_code_1 */
    txn.reject_reason_code_1                                                                AS reject_reason_code_1,

    /* reject_reason_code_2 */
    txn.reject_reason_code_2                                                                AS reject_reason_code_2,

    /* reject_reason_code_3 */
    txn.reject_reason_code_3                                                                AS reject_reason_code_3,

    /* reject_reason_code_4 */
    txn.reject_reason_code_4                                                                AS reject_reason_code_4,

    /* reject_reason_code_5 */
    txn.reject_reason_code_5                                                                AS reject_reason_code_5,

    /* diagnosis_code */
    CLEAN_UP_DIAGNOSIS_CODE
        (
              UPPER(regexp_replace(regexp_replace(regexp_replace(txn.diagnosis_code, '\\.', ''), ',', ''), ' ', '')),
              txn.diagnosis_code_qual,
              CAST(EXTRACT_DATE(txn.date_service, '%Y%m%d') AS DATE)
        )                                                                                   AS diagnosis_code,

    /* diagnosis_code_qual */
	CASE WHEN LENGTH(COALESCE(txn.diagnosis_code,'')) > 0 AND COALESCE(txn.diagnosis_code_qual ,'XX') IN ('01', '02')
	       THEN txn.diagnosis_code_qual
	    ELSE NULL
	END                                                                                     AS diagnosis_code_qual,

    /* procedure_code */
    CLEAN_UP_PROCEDURE_CODE
        (
            CASE WHEN txn.product_service_id_qualifier IN ('7','8','9','07','08','09')
                    THEN txn.product_service_id
                ELSE NULL
            END
        )                                                                                   AS procedure_code,

    /* procedure_code_qual */
	CASE WHEN txn.product_service_id_qualifier IN ('7','8','9','07','08','09')
	            THEN txn.product_service_id_qualifier
	    ELSE NULL
    END                                                                                     AS procedure_code_qual,

    /* ndc_code */
    CLEAN_UP_NDC_CODE
        (
            CASE WHEN txn.product_service_id_qualifier IN ('3','03')
                    THEN txn.product_service_id
                ELSE NULL
            END
        )                                                                                   AS ndc_code,

    /* product_service_id */
    CASE WHEN txn.product_service_id_qualifier NOT IN ('7','8','9','07','08','09','3','03')
            THEN txn.product_service_id
        ELSE NULL
    END                                                                                     AS product_service_id,

    /* product_service_id_qual */
    CASE WHEN txn.product_service_id_qualifier NOT IN ('7','8','9','07','08','09','3','03')
            THEN txn.product_service_id_qualifier
        ELSE NULL
    END                                                                                     AS product_service_id_qual,

    /* rx_number */
    MD5(txn.rx_number)                                                                      AS rx_number,

    /* rx_number_qual */
    CASE WHEN LENGTH(COALESCE(txn.rx_number,'')) > 0
            THEN '1'
	    ELSE NULL
	END                                                                                     AS rx_number_qual,

    /* bin_number */
    txn.bin_number                                                                          AS bin_number,

    /* processor_control_number */
    txn.processor_control_number                                                            AS processor_control_number,

    /* fill_number */
    txn.fill_number                                                                         AS fill_number,

    /* refill_auth_amount */
    txn.refill_auth_amount                                                                  AS refill_auth_amount,

    /* dispensed_quantity */
    EXTRACT_NUMBER(txn.dispensed_quantity)                                                  AS dispensed_quantity,

    /* unit_of_measure */
    txn.unit_of_measure                                                                     AS unit_of_measure,

    /* days_supply */
    txn.days_supply	                                                                        AS days_supply,

    /* pharmacy_npi */
    CLEAN_UP_NPI_CODE(txn.pharmacy_npi)                                                     AS pharmacy_npi,

    /* prov_dispensing_npi */
    CLEAN_UP_NPI_CODE
        (
            CASE WHEN (txn.prov_dispensing_qual IN ('1','01')
                    OR (prov_dispensing_qual IN ('5', '05') AND regexp_extract(provider_id, '^[0-9]{{10}}$', 0) <> '')
                ) THEN provider_id
                ELSE NULL
            END
        )                                                                                   AS prov_dispensing_npi,

    /* payer_id */
    txn.payer_id                                                                            AS payer_id,

   /* payer_id_qual */
    CASE WHEN LENGTH(COALESCE(txn.payer_id,'')) > 0
	            THEN txn.payer_id_qual
	    ELSE NULL
    END                                                                                     AS payer_id_qual,

    /* payer_name */
    CAST(NULL AS STRING)                                                                    AS payer_name,

    /* payer_parent_name */
    CAST(NULL AS STRING)                                                                    AS payer_parent_name,

    /* payer_org_name */
    CAST(NULL AS STRING)                                                                    AS payer_org_name,

    /* payer_plan_id */
    txn.payer_plan_id                                                                       AS payer_plan_id,

    /* payer_plan_name */
    txn.payer_plan_name                                                                     AS payer_plan_name,

    /* payer_type */
    txn.payer_type                                                                          AS payer_type,

    /* compound_code */
    CASE
        WHEN txn.compound_code IN ('0', '1', '2')
            THEN txn.compound_code
        ELSE NULL
    END                                                                                     AS compound_code,

    /* unit_dose_indicator */
    txn.unit_dose_indicator                                                                 AS unit_dose_indicator,

    /* dispensed_as_written */
    CLEAN_UP_NUMERIC_CODE(txn.dispensed_as_written)                                         AS dispensed_as_written,

    /* prescription_origin */
    txn.prescription_origin                                                                 AS prescription_origin,

    /* submission_clarification */
    CASE
        WHEN CLEAN_UP_NUMERIC_CODE(txn.submission_clarification) BETWEEN 1 AND 99
            THEN CLEAN_UP_NUMERIC_CODE(txn.submission_clarification)
        ELSE NULL
    END                                                                                     AS submission_clarification,

    /* orig_prescribed_product_service_code */
    CASE
        WHEN txn.orig_prescribed_product_service_code ='00000000000'
            THEN NULL
        ELSE txn.orig_prescribed_product_service_code
    END                                                                                     AS orig_prescribed_product_service_code,

    /* orig_prescribed_product_service_code_qual */
    CASE WHEN LENGTH(COALESCE(txn.orig_prescribed_product_service_code,'')) > 0
            AND txn.orig_prescribed_product_service_code <> '00000000000'
	            THEN txn.orig_prescribed_product_service_code_qual
	    ELSE NULL
	END                                                                                     AS orig_prescribed_product_service_code_qual,

    /* orig_prescribed_quantity */
    EXTRACT_NUMBER(txn.orig_prescribed_quantity)                                            AS orig_prescribed_quantity,

    /* prior_auth_type_code */
    txn.prior_auth_type_code                                                                AS prior_auth_type_code,

    /* level_of_service */
    txn.level_of_service                                                                    AS level_of_service,

    /* reason_for_service */
    txn.reason_for_service                                                                  AS reason_for_service,

    /* professional_service_code */
    txn.professional_service_code                                                           AS professional_service_code,

    /* result_of_service_code */
    txn.result_of_service_code                                                              AS result_of_service_code,

    /* place_of_service_std_id */
    CAST(NULL AS STRING) AS place_of_service_std_id,

    /* place_of_service_vendor_id */
    CAST(NULL AS STRING) AS place_of_service_vendor_id,

    /* prov_prescribing_npi */
    CLEAN_UP_NPI_CODE
        (
            CASE WHEN (txn.prov_prescribing_qual IN ('1','01')
                    OR (txn.prov_prescribing_qual IN ('5', '05') AND regexp_extract(txn.prescriber_id, '^[0-9]{{10}}$', 0) <> '')
                ) THEN txn.prescriber_id
                ELSE NULL
            END
        )                                                                                   AS prov_prescribing_npi,

    /* prov_prescribing_tax_id */
    CAST(NULL AS STRING) AS prov_prescribing_tax_id,

    /* prov_prescribing_dea_id */
    CAST(NULL AS STRING) AS prov_prescribing_dea_id,

    /* prov_prescribing_ssn */
    CAST(NULL AS STRING) AS prov_prescribing_ssn,

    /* prov_prescribing_state_license */
    CAST(NULL AS STRING) AS prov_prescribing_state_license,

    /* prov_prescribing_upin */
    CAST(NULL AS STRING) AS prov_prescribing_upin,

    /* prov_prescribing_commercial_id */
    CAST(NULL AS STRING) AS prov_prescribing_commercial_id,

    /* prov_prescribing_name_1 */
    txn.prescriber_last_name                                                                AS prov_prescribing_name_1,

    /* prov_prescribing_name_2 */
    CAST(NULL AS STRING) AS prov_prescribing_name_2,

    /* prov_prescribing_address_1 */
    CAST(NULL AS STRING) AS prov_prescribing_address_1,

    /* prov_prescribing_address_2 */
    CAST(NULL AS STRING) AS prov_prescribing_address_2,

    /* prov_prescribing_city */
    CAST(NULL AS STRING) AS prov_prescribing_city,

    /* prov_prescribing_state */
    CAST(NULL AS STRING) AS prov_prescribing_state,

    /* prov_prescribing_zip */
    CAST(NULL AS STRING) AS prov_prescribing_zip,

    /* prov_prescribing_std_taxonomy */
    CAST(NULL AS STRING) AS prov_prescribing_std_taxonomy,

    /* prov_prescribing_vendor_specialty */
    CAST(NULL AS STRING) AS prov_prescribing_vendor_specialty,

    /* prov_primary_care_npi */
    CLEAN_UP_NPI_CODE
        (
            CASE WHEN (txn.prov_primary_care_qual IN ('1','01')
                    OR (txn.prov_primary_care_qual IN ('5', '05') AND regexp_extract(txn.primary_care_provider_id, '^[0-9]{{10}}$', 0) <> '')
                ) THEN txn.primary_care_provider_id
                ELSE NULL
            END
        )                                                                                   AS prov_primary_care_npi,

    /* cob_count */
    txn.cob_count                                                                           AS cob_count,

    /* usual_and_customary_charge */
    EXTRACT_NUMBER(txn.usual_and_customary_charge)                                          AS usual_and_customary_charge,

    /* product_selection_attributed */
    EXTRACT_NUMBER(txn.product_selection_attributed)                                        AS product_selection_attributed,

    /* other_payer_recognized */
    EXTRACT_NUMBER(txn.other_payer_recognized)                                              AS other_payer_recognized,

    /* periodic_deductible_applied */
    EXTRACT_NUMBER(txn.periodic_deductible_applied)                                         AS periodic_deductible_applied,

    /* periodic_benefit_exceed */
    EXTRACT_NUMBER(txn.periodic_benefit_exceed)                                             AS periodic_benefit_exceed,

    /* accumulated_deductible */
    EXTRACT_NUMBER(txn.accumulated_deductible)                                              AS accumulated_deductible,

    /* remaining_deductible */
    EXTRACT_NUMBER(txn.remaining_deductible)                                                AS remaining_deductible,

    /* remaining_benefit */
    EXTRACT_NUMBER(txn.remaining_benefit)                                                   AS remaining_benefit,

    /* copay_coinsurance */
    EXTRACT_NUMBER(txn.copay_coinsurance)                                                   AS copay_coinsurance,

    /* basis_of_cost_determination */
    txn.basis_of_cost_determination                                                         AS basis_of_cost_determination,

    /* submitted_ingredient_cost */
    EXTRACT_NUMBER(txn.submitted_ingredient_cost)                                           AS submitted_ingredient_cost,

    /* submitted_dispensing_fee */
    EXTRACT_NUMBER(txn.submitted_dispensing_fee)                                            AS submitted_dispensing_fee,

    /* submitted_incentive */
    EXTRACT_NUMBER(txn.submitted_incentive)                                                 AS submitted_incentive,

    /* submitted_gross_due */
    EXTRACT_NUMBER(txn.submitted_gross_due)                                                 AS submitted_gross_due,

    /* submitted_professional_service_fee */
    EXTRACT_NUMBER(txn.submitted_professional_service_fee)                                  AS submitted_professional_service_fee,

    /* submitted_patient_pay */
    EXTRACT_NUMBER(txn.submitted_patient_pay)                                               AS submitted_patient_pay,

    /* submitted_other_claimed_qual */
    EXTRACT_NUMBER
        (
            CASE WHEN LENGTH(COALESCE(txn.submitted_other_claimed,'')) > 0
                    THEN txn.submitted_other_claimed_qual
                ELSE NULL
            END
        )                                                                                   AS submitted_other_claimed_qual,

    /* submitted_other_claimed */
    EXTRACT_NUMBER(txn.submitted_other_claimed)                                             AS submitted_other_claimed,

    /* basis_of_reimbursement_determination */
    txn.basis_of_reimbursement_determination                                                AS basis_of_reimbursement_determination,

    /* paid_ingredient_cost */
    EXTRACT_NUMBER(txn.paid_ingredient_cost)                                                AS paid_ingredient_cost,

    /* paid_dispensing_fee */
    EXTRACT_NUMBER(txn.paid_dispensing_fee)                                                 AS paid_dispensing_fee,

    /* paid_incentive */
    txn.incentive_paid                                                                      AS paid_incentive,

    /* paid_gross_due */
    EXTRACT_NUMBER(txn.paid_gross_due)                                                      AS paid_gross_due,

    /* paid_professional_service_fee */
    EXTRACT_NUMBER(txn.paid_professional_service_fee)                                       AS paid_professional_service_fee,

    /* paid_patient_pay */
    EXTRACT_NUMBER(txn.paid_patient_pay)                                                    AS paid_patient_pay,

    /* paid_other_claimed_qual */
    EXTRACT_NUMBER
        (
            CASE WHEN LENGTH(COALESCE(txn.paid_other_claimed,'')) > 0
                        THEN txn.paid_other_claimed_qual
                ELSE NULL
            END
        )                                                                                   AS paid_other_claimed_qual,

    /* paid_other_claimed */
    EXTRACT_NUMBER(txn.paid_other_claimed)                                                  AS paid_other_claimed,

    /* tax_exempt_indicator */
    txn.tax_exempt_indicator                                                                AS tax_exempt_indicator,

    /* coupon_type */
    txn.coupon_type                                                                         AS coupon_type,

    /* coupon_number */
    txn.coupon_number                                                                       AS coupon_number,

    /* coupon_value */
    txn.coupon_value                                                                        AS coupon_value,

    /* pharmacy_other_id */
    CASE WHEN txn.ncpdp_number <> '' AND txn.ncpdp_number IS NOT NULL
            THEN txn.ncpdp_number
        ELSE txn.service_provider_id
    END                                                                                     AS pharmacy_other_id,

    /* pharmacy_other_qual */
    CASE WHEN txn.ncpdp_number <> '' AND txn.ncpdp_number IS NOT NULL
            THEN '07'
        ELSE txn.service_provider_id_qualifier
    END                                                                                     AS pharmacy_other_qual,

    /* pharmacy_postal_code */
    txn.pharmacy_postal_code                                                                AS pharmacy_postal_code,

    /* pharmacy_group */
    CAST(NULL AS STRING)                                                                    AS pharmacy_group,

    /* prov_dispensing_id */
    CASE WHEN (txn.prov_dispensing_qual NOT IN ('1','01')
            AND (txn.prov_dispensing_qual NOT IN ('5', '05') OR regexp_extract(txn.provider_id, '^[0-9]{{10}}$', 0) = '')
        ) THEN txn.provider_id
        ELSE NULL
    END                                                                                     AS prov_dispensing_id,

    /* prov_dispensing_qual */
    CASE WHEN (txn.prov_dispensing_qual NOT IN ('1','01')
            AND (txn.prov_dispensing_qual NOT IN ('5', '05') OR regexp_extract(txn.provider_id, '^[0-9]{{10}}$', 0) = '')
        ) THEN txn.prov_dispensing_qual
        ELSE NULL
    END                                                                                     AS prov_dispensing_qual,

    /* prov_prescribing_id */
    CASE WHEN (txn.prov_prescribing_qual NOT IN ('1','01')
            AND (txn.prov_prescribing_qual NOT IN ('5', '05') OR regexp_extract(txn.prescriber_id, '^[0-9]{{10}}$', 0) = '')
        ) THEN txn.prescriber_id
        ELSE NULL
    END                                                                                     AS prov_prescribing_id,

    /* prov_prescribing_qual */
    CASE WHEN (txn.prov_prescribing_qual NOT IN ('1','01')
            AND (txn.prov_prescribing_qual NOT IN ('5', '05') OR regexp_extract(txn.prescriber_id, '^[0-9]{{10}}$', 0) = '')
        ) THEN txn.prov_prescribing_qual
        ELSE NULL
    END                                                                                     AS prov_prescribing_qual,

    /* prov_primary_care_id */
    CASE WHEN (txn.prov_primary_care_qual NOT IN ('1','01')
            AND (txn.prov_primary_care_qual NOT IN ('5', '05') OR regexp_extract(txn.primary_care_provider_id, '^[0-9]{{10}}$', 0) = '')
        ) THEN txn.primary_care_provider_id
        ELSE NULL
    END                                                                                     AS prov_primary_care_id,

    /* prov_primary_care_qual */
    CASE WHEN (txn.prov_primary_care_qual NOT IN ('1','01')
            AND (txn.prov_primary_care_qual NOT IN ('5', '05') OR regexp_extract(txn.primary_care_provider_id, '^[0-9]{{10}}$', 0) = '')
        ) THEN txn.prov_primary_care_qual
        ELSE NULL
    END                                                                                     AS prov_primary_care_qual,

    /* other_payer_coverage_type */
    txn.other_payer_coverage_type                                                           AS other_payer_coverage_type,

    /* other_payer_coverage_id */
    txn.other_payer_coverage_id                                                             AS other_payer_coverage_id,

    /* other_payer_coverage_qual */
    CASE WHEN LENGTH(COALESCE(txn.other_payer_coverage_id,'')) > 0
	            THEN txn.other_payer_coverage_qual
	    ELSE NULL
	END                                                                                     AS other_payer_coverage_qual,

    /* other_payer_date */
	CAP_DATE
        (
            CAST(EXTRACT_DATE(txn.other_payer_date, '%Y%m%d') AS DATE),
            CAST('{EARLIEST_SERVICE_DATE}'  AS DATE),
            CAST('{VDR_FILE_DT}' AS DATE)
        )                                                                                   AS other_payer_date,

    /* other_payer_coverage_code */
    txn.other_payer_coverage_code                                                           AS other_payer_coverage_code,

    /* pharmacy_claim_link_text */
    CAST(NULL AS STRING) AS pharmacy_claim_link_text,

    /* logical_delete_reason_date */
    CAST(NULL AS DATE) AS logical_delete_reason_date,

    /* logical_delete_reason */
    CASE WHEN txn.transaction_code_std = 'B2'
            THEN 'Reversal'
        WHEN txn.transaction_code_std = 'B1' AND txn.response_code_std = 'R'
            THEN 'Claim Rejected'
        ELSE NULL
    END                                                                                     AS  logical_delete_reason,

    /* part_provider */
	'emdeon'                                                                                AS part_provider,
    /* part_best_date */
	CASE
	    WHEN 0 = LENGTH(COALESCE
	                            (
	                                CAP_DATE
                                        (
                                            CAST(EXTRACT_DATE(txn.date_service, '%Y%m%d') AS DATE),
                                            COALESCE(CAST('{AVAILABLE_START_DATE}'  AS DATE), CAST('{EARLIEST_SERVICE_DATE}'  AS DATE)),
                                            CAST('{VDR_FILE_DT}' AS DATE)
                                        ),
                                    ''
                                ))
	        THEN '0_PREDATES_HVM_HISTORY'
	    ELSE CONCAT
	            (
                    SUBSTR(txn.date_service, 1, 4), '-',
                    SUBSTR(txn.date_service, 5, 2), '-01'
                )
	END                                                                                     AS part_best_date

FROM claim txn
LEFT OUTER JOIN matching_payload payload ON txn.claim_id = payload.claimid AND payload.hvid IS NOT NULL
WHERE TRIM(UPPER(COALESCE(txn.claim_id, 'empty'))) <> 'CLAIM_ID'
