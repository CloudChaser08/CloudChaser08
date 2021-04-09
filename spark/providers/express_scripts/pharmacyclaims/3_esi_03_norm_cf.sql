SELECT
    --    MONOTONICALLY_INCREASING_ID()                                                           AS record_id,
    TRIM(txn.pharmacy_claim_id)                                                             AS claim_id,
    txn.hvid                                                                                AS hvid,
    CURRENT_DATE()                                                                          AS created,
    '3'                                                                                     AS model_version,
    SPLIT(txn.input_file_name, '/')[SIZE(SPLIT(txn.input_file_name, '/')) - 1]              AS data_set,
	'16'                                                       AS data_feed, --16
	'17'                                                                       AS data_vendor, --express_scripts
	CAST(NULL AS STRING) AS source_version,

	CLEAN_UP_GENDER
    	(
        	CASE
        	    WHEN SUBSTR(UPPER(COALESCE(txn.gender,'')), 1, 1) IN ('F', 'M')
        	        THEN SUBSTR(UPPER(COALESCE(txn.gender , '')), 1, 1)
        	    ELSE 'U'
        	END
	    )   AS patient_gender,

	CAST(NULL AS STRING) AS patient_age,

	/* patient_year_of_birth */
	CAP_YEAR_OF_BIRTH
        (
            txn.age,
            CAST(EXTRACT_DATE(txn.date_of_service, '%Y%m%d') AS DATE),
            COALESCE(txn.yearOfBirth, YEAR(CAST(EXTRACT_DATE('1900-01-01', '%Y-%m-%d') AS DATE)))
        )                                                                                   AS patient_year_of_birth,

    /* patient_zip3 */
    MASK_ZIP_CODE(SUBSTR(txn.threeDigitZip, 1, 3))                                      AS patient_zip3,

    /* patient_state */
    VALIDATE_STATE_CODE(UPPER(COALESCE(txn.patient_state, txn.state, '')))              AS patient_state,

    /* date_service */
	CAP_DATE
        (
            CAST(EXTRACT_DATE(txn.date_of_service, '%Y%m%d') AS DATE),
            CAST('{EARLIEST_SERVICE_DATE}'  AS DATE),
            CAST(EXTRACT_DATE('{VDR_FILE_DT}', '%Y-%m-%d') AS DATE)
        )                                                                                   AS date_service,

    /* date_written */
	CAP_DATE
        (
            CAST(EXTRACT_DATE(txn.date_prescription_written, '%Y%m%d') AS DATE),
            CAST('{EARLIEST_SERVICE_DATE}'  AS DATE),
            CAST(EXTRACT_DATE('{VDR_FILE_DT}', '%Y-%m-%d') AS DATE)
        )                                                                                   AS date_written,

	CAST(NULL AS STRING) AS year_of_injury,
	CAST(NULL AS DATE) AS date_authorized,
	CAST(NULL AS STRING) AS time_authorized,
	CAST(NULL AS DATE) AS discharge_date,
	CAST(NULL AS STRING) AS transaction_code_std,

    CASE WHEN LOWER(txn.transaction_code) = 'd' THEN 'D' ELSE NULL END                      AS transaction_code_vendor,

	CAST(NULL AS STRING) AS response_code_std,

    CASE WHEN LOWER(txn.transaction_code) = 'r' THEN 'R' ELSE NULL END                      AS response_code_vendor,

    txn.reject_code_1                                                                       AS reject_reason_code_1,
    txn.reject_code_2                                                                       AS reject_reason_code_2,
    txn.reject_code_3                                                                       AS reject_reason_code_3,
    txn.reject_code_4                                                                       AS reject_reason_code_4,
    txn.reject_code_5                                                                       AS reject_reason_code_5,

    /* diagnosis_code */
    CLEAN_UP_DIAGNOSIS_CODE
       (
          txn.diagnosis_code,
    	  txn.diagnosis_code_qualifier,
    	  CAST(EXTRACT_DATE(txn.date_of_service, '%Y%m%d') AS DATE)
    	)                                                                                   AS diagnosis_code,

    /* diagnosis_code_qual */
	CASE WHEN LENGTH(COALESCE(txn.diagnosis_code,'')) > 0
	            AND COALESCE(txn.diagnosis_code_qualifier,'XX') IN ('01', '02')
	       THEN txn.diagnosis_code_qualifier
	ELSE NULL END                                                                           AS diagnosis_code_qual,

    CLEAN_UP_PROCEDURE_CODE(
    CASE WHEN txn.product_service_id_qualifier IN ('7','8','9','07','08','09')
                THEN txn.product_service_id ELSE NULL END
        )                                                                                   AS procedure_code,

    /* procedure_code_qual */
	CASE WHEN txn.product_service_id_qualifier IN ('7','8','9','07','08','09')
	            THEN txn.product_service_id_qualifier
	ELSE NULL END                                                                           AS procedure_code_qual,

    CLEAN_UP_NDC_CODE(CASE WHEN product_service_id_qualifier IN ('3','03')
                THEN product_service_id ELSE NULL END)                                      AS ndc_code,

    CASE WHEN product_service_id_qualifier NOT IN ('3','7','8','9','03','07','08','09')
                THEN product_service_id
    ELSE NULL END                                                                           AS product_service_id,

   /* product_service_id_qual */
	CASE WHEN product_service_id_qualifier NOT IN ('3','7','8','9','03','07','08','09')
	    THEN product_service_id_qualifier
	ELSE NULL END                                                                           AS product_service_id_qual,

    MD5(txn.rxnumber)                                                                       AS rx_number,
   /* rx_number_qual */
    txn.prescription_service_reference_number_qualifier                                     AS rx_number_qual,

	CAST(NULL AS STRING) AS bin_number,

    txn.processor_control_number                                                            AS processor_control_number,
    CAST(txn.fill_number AS INTEGER)                                                        AS fill_number,
    txn.number_of_refills_authorized                                                        AS refill_auth_amount,

    CAST(txn.quantity_dispensed AS FLOAT)                                                 AS dispensed_quantity,

    txn.unit_of_measure                                                                     AS unit_of_measure,

    CAST(txn.days_supply AS INTEGER)                                                        AS days_supply,

    CASE WHEN (txn.service_provider_id_qualifier in ('1','01'))
            OR (txn.service_provider_id_qualifier in ('5', '05')
                AND regexp_replace(txn.service_provider_id, '^[0-9]{{10}}$', '') = '')
            THEN txn.service_provider_id
    ELSE NULL END                                                                           AS pharmacy_npi,

	CAST(NULL AS STRING) AS prov_dispensing_npi,
	CAST(NULL AS STRING) AS payer_id,
	CAST(NULL AS STRING) AS payer_id_qual,
	CAST(NULL AS STRING) AS payer_name,
	CAST(NULL AS STRING) AS payer_parent_name,
	CAST(NULL AS STRING) AS payer_org_name,
	CAST(NULL AS STRING) AS payer_plan_id,
	CAST(NULL AS STRING) AS payer_plan_name,
	CAST(NULL AS STRING) AS payer_type,

    /* compound_code */
    CASE
        WHEN txn.compound_code IN ('0','1', '2')
            THEN txn.compound_code
    ELSE NULL END                                                                           AS compound_code,

    txn.unit_dose_indicator                                                                 AS unit_dose_indicator,

    CLEAN_UP_NUMERIC_CODE(txn.dispense_as_written)                                          AS dispensed_as_written,

	CAST(NULL AS STRING) AS prescription_origin,
	CAST(NULL AS STRING) AS submission_clarification,
	CAST(NULL AS STRING) AS orig_prescribed_product_service_code,
	CAST(NULL AS STRING) AS orig_prescribed_product_service_code_qual,
	CAST(NULL AS STRING) AS orig_prescribed_quantity,
	CAST(NULL AS STRING) AS prior_auth_type_code,

    txn.level_of_service                                                                    AS level_of_service,

	CAST(NULL AS STRING) AS reason_for_service,
	CAST(NULL AS STRING) AS professional_service_code,
	CAST(NULL AS STRING) AS result_of_service_code,

    CASE WHEN (txn.prescriber_id_qualifier in ('1','01'))
                OR (txn.prescriber_id_qualifier in ('5', '05')
                    AND regexp_replace(txn.prescriber_id_qualifier, '^[0-9]{{10}}$', '') = '')
        THEN txn.prescriber_id
    ELSE NULL END AS prov_prescribing_npi,

    CAST(NULL AS STRING) AS prov_prescribing_tax_id,
    CAST(NULL AS STRING) AS prov_prescribing_dea_id,
    CAST(NULL AS STRING) AS prov_prescribing_ssn,
    CAST(NULL AS STRING) AS prov_prescribing_state_license,
    CAST(NULL AS STRING) AS prov_prescribing_upin,
    CAST(NULL AS STRING) AS prov_prescribing_commercial_id,
    CAST(NULL AS STRING) AS prov_prescribing_name_1,
    CAST(NULL AS STRING) AS prov_prescribing_name_2,
    CAST(NULL AS STRING) AS prov_prescribing_address_1,
    CAST(NULL AS STRING) AS prov_prescribing_address_2,
    CAST(NULL AS STRING) AS prov_prescribing_city,
    CAST(NULL AS STRING) AS prov_prescribing_state,
    CAST(NULL AS STRING) AS prov_prescribing_zip,
    CAST(NULL AS STRING) AS prov_prescribing_std_taxonomy,
    CAST(NULL AS STRING) AS prov_prescribing_vendor_specialty,
	CAST(NULL AS STRING) AS prov_primary_care_npi,
	CAST(NULL AS STRING) AS cob_count,
	CAST(NULL AS FLOAT) AS usual_and_customary_charge,
	CAST(NULL AS FLOAT) AS product_selection_attributed,
	CAST(NULL AS FLOAT) AS other_payer_recognized,
	CAST(NULL AS FLOAT) AS periodic_deductible_applied,
	CAST(NULL AS FLOAT) AS periodic_benefit_exceed,
	CAST(NULL AS FLOAT) AS accumulated_deductible,
	CAST(NULL AS FLOAT) AS remaining_deductible,
	CAST(NULL AS FLOAT) AS remaining_benefit,
	CAST(NULL AS FLOAT) AS copay_coinsurance,
	CAST(NULL AS FLOAT) AS basis_of_cost_determination,
	CAST(NULL AS FLOAT) AS submitted_ingredient_cost,
	CAST(NULL AS FLOAT) AS submitted_dispensing_fee,
	CAST(NULL AS FLOAT) AS submitted_incentive,
	CAST(NULL AS FLOAT) AS submitted_gross_due,
	CAST(NULL AS FLOAT) AS submitted_professional_service_fee,
	CAST(NULL AS FLOAT) AS submitted_patient_pay,
	CAST(NULL AS FLOAT) AS submitted_other_claimed_qual,
	CAST(NULL AS FLOAT) AS submitted_other_claimed,
	CAST(NULL AS FLOAT) AS basis_of_reimbursement_determination,
	CAST(NULL AS FLOAT) AS paid_ingredient_cost,
	CAST(NULL AS FLOAT) AS paid_dispensing_fee,
	CAST(NULL AS FLOAT) AS paid_incentive,
	CAST(NULL AS FLOAT) AS paid_gross_due,
	CAST(NULL AS FLOAT) AS paid_professional_service_fee,
	CAST(NULL AS FLOAT) AS paid_patient_pay,
	CAST(NULL AS FLOAT) AS paid_other_claimed_qual,
	CAST(NULL AS FLOAT) AS paid_other_claimed,
	CAST(NULL AS STRING) AS tax_exempt_indicator,
	CAST(NULL AS STRING) AS coupon_type,
	CAST(NULL AS STRING) AS coupon_number,
	CAST(NULL AS FLOAT) AS coupon_value,

    CASE WHEN (txn.service_provider_id_qualifier not in ('1','01'))
                AND (txn.service_provider_id_qualifier not in ('5', '05')
                    OR regexp_replace(txn.service_provider_id, '^[0-9]{{10}}$', '') != '')
        THEN txn.service_provider_id
    ELSE NULL END AS pharmacy_other_id,

    CASE WHEN (txn.service_provider_id_qualifier not in ('1','01'))
                AND (txn.service_provider_id_qualifier not in ('5', '05')
                    OR regexp_replace(txn.service_provider_id, '^[0-9]{{10}}$', '') != '')
        THEN txn.service_provider_id_qualifier
    ELSE NULL END AS pharmacy_other_qual,

	CAST(NULL AS STRING) AS pharmacy_postal_code,
	CAST(NULL AS STRING) AS prov_dispensing_id,
	CAST(NULL AS STRING) AS prov_dispensing_qual,

    CASE WHEN (txn.prescriber_id_qualifier not in ('1','01'))
                AND (txn.prescriber_id_qualifier not in ('5', '05')
                    OR regexp_replace(txn.prescriber_id_qualifier, '^[0-9]{{10}}$', '') != '')
        THEN txn.prescriber_id
    ELSE NULL END AS prov_prescribing_id,

    CASE WHEN (txn.prescriber_id_qualifier not in ('1','01'))
                AND (txn.prescriber_id_qualifier not in ('5', '05')
                    OR regexp_replace(txn.prescriber_id_qualifier, '^[0-9]{{10}}$', '') != '')
        THEN txn.prescriber_id_qualifier
    ELSE NULL END                                                                               AS prov_prescribing_qual,
    /* logical_delete_reason */

	CAST(NULL AS STRING) AS prov_primary_care_id,
	CAST(NULL AS STRING) AS prov_primary_care_qual,
	CAST(NULL AS STRING) AS other_payer_coverage_type,
	CAST(NULL AS STRING) AS other_payer_coverage_id,
	CAST(NULL AS STRING) AS other_payer_coverage_qual,
	CAST(NULL AS DATE) AS other_payer_date,
	CAST(NULL AS STRING) AS other_payer_coverage_code,

    CAST(NULL AS STRING) AS logical_delete_reason,

	'express_scripts' AS part_provider,
    /* part_best_date */
	CASE
	    WHEN 0 = LENGTH(COALESCE
	                            (
	                                CAP_DATE
                                        (
                                            CAST(EXTRACT_DATE(txn.date_of_service, '%Y%m%d') AS DATE),
                                            COALESCE(CAST('{AVAILABLE_START_DATE}'  AS DATE), CAST('{EARLIEST_SERVICE_DATE}'  AS DATE)),
                                            CAST(EXTRACT_DATE('{VDR_FILE_DT}', '%Y-%m-%d') AS DATE)
                                        ),
                                    ''
                                ))
	        THEN '0_PREDATES_HVM_HISTORY'
	    ELSE CONCAT
	            (
                    SUBSTR(txn.date_of_service, 1, 4), '-',
                    SUBSTR(txn.date_of_service, 5, 2)
                )
	END                                                                                         AS part_best_date
FROM esi_00_dedup txn
