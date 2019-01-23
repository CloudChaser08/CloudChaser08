SELECT
    monotonically_increasing_id()                                                           AS record_id,
	pay.claimid																				AS claim_id,
	pay.hvid																				AS hvid,
    current_date()                                                                          AS created,
	'07'																					AS model_version,
    data_set,
	'65'																					AS data_feed,
	'262'																					AS data_vendor,
	/* patient_gender */
	CASE
	    WHEN COALESCE(txn.patient_gender_code, '3') IN ('1', 'M')
	         THEN 'M'
	    WHEN COALESCE(txn.patient_gender_code, '3') IN ('2', 'F')
	         THEN 'F'
	    WHEN COALESCE(pay.gender, 'U') = 'F'
	         THEN 'F'
	    WHEN COALESCE(pay.gender, 'U') = 'M'
	         THEN 'M'
	    ELSE 'U'
	END 																					AS patient_gender,
	/* patient_age */
	VALIDATE_AGE
	    (
	        pay.age,
            CAST
                (
                    EXTRACT_DATE
                        (
                            CASE
                                WHEN date_filled >= date_delivered_fulfilled
                                 AND date_filled <= '{VENDOR_FILE_DATE_STR}'
                                     THEN date_filled
                                WHEN date_delivered_fulfilled >= date_filled
                                 AND date_delivered_fulfilled <= '{VENDOR_FILE_DATE_STR}'
                                     THEN date_delivered_fulfilled
                                WHEN date_filled >= date_delivered_fulfilled
                                 AND date_filled > '{VENDOR_FILE_DATE_STR}'
                                 AND date_delivered_fulfilled <= '{VENDOR_FILE_DATE_STR}'
                                     THEN date_delivered_fulfilled
                                WHEN date_delivered_fulfilled >= date_filled
                                 AND date_delivered_fulfilled > '{VENDOR_FILE_DATE_STR}'
                                 AND date_filled <= '{VENDOR_FILE_DATE_STR}'
                                     THEN date_filled
                                WHEN date_delivered_fulfilled > '{VENDOR_FILE_DATE_STR}'
                                 AND date_filled > '{VENDOR_FILE_DATE_STR}'
                                     THEN date_delivered_fulfilled
                                ELSE NULL
                            END,
                            '%Y%m%d'
                        ) AS DATE
                ),
            CAST(COALESCE
                    (
                        CAST(txn.patient_birth_year AS INTEGER),
                        CAST(pay.yearofbirth AS INTEGER)
                    ) AS STRING)
	    )																					AS patient_age,
	/* patient_year_of_birth */
	CAP_YEAR_OF_BIRTH
	    (
	        pay.age,
            CAST
                (
                    EXTRACT_DATE
                        (
                            CASE
                                WHEN date_filled >= date_delivered_fulfilled
                                 AND date_filled <= '{VENDOR_FILE_DATE_STR}'
                                     THEN date_filled
                                WHEN date_delivered_fulfilled >= date_filled
                                 AND date_delivered_fulfilled <= '{VENDOR_FILE_DATE_STR}'
                                     THEN date_delivered_fulfilled
                                WHEN date_filled >= date_delivered_fulfilled
                                 AND date_filled > '{VENDOR_FILE_DATE_STR}'
                                 AND date_delivered_fulfilled <= '{VENDOR_FILE_DATE_STR}'
                                     THEN date_delivered_fulfilled
                                WHEN date_delivered_fulfilled >= date_filled
                                 AND date_delivered_fulfilled > '{VENDOR_FILE_DATE_STR}'
                                 AND date_filled <= '{VENDOR_FILE_DATE_STR}'
                                     THEN date_filled
                                WHEN date_delivered_fulfilled > '{VENDOR_FILE_DATE_STR}'
                                 AND date_filled > '{VENDOR_FILE_DATE_STR}'
                                     THEN date_delivered_fulfilled
                                ELSE NULL
                            END,
                            '%Y%m%d'
                        ) AS DATE
                ),
            CAST(COALESCE
                    (
                        CAST(txn.patient_birth_year AS INTEGER),
                        CAST(pay.yearofbirth AS INTEGER)
                    ) AS STRING)
	    )																					AS patient_year_of_birth,
	/* patient_zip3 */
	MASK_ZIP_CODE
	    (
	        CASE
	            WHEN COALESCE(txn.patient_zip_code, 'XXXXX') <> 'XXXXX'
	                 THEN SUBSTR(txn.patient_zip_code, 1, 3)
	            WHEN pay.threedigitzip IS NOT NULL
	                 THEN SUBSTR(pay.threedigitzip, 1, 3)
	            ELSE NULL
	        END
	    )                                                                                   AS patient_zip3,
	/* patient_state */
	VALIDATE_STATE_CODE(UPPER(COALESCE(pay.state, ''))) 									AS patient_state,
	/* date_service */
	CAP_DATE
	    (
            CAST
                (
                    EXTRACT_DATE
                        (
                            CASE
                                WHEN date_filled >= date_delivered_fulfilled
                                 AND date_filled <= '{VENDOR_FILE_DATE_STR}'
                                     THEN date_filled
                                WHEN date_delivered_fulfilled >= date_filled
                                 AND date_delivered_fulfilled <= '{VENDOR_FILE_DATE_STR}'
                                     THEN date_delivered_fulfilled
                                WHEN date_filled >= date_delivered_fulfilled
                                 AND date_filled > '{VENDOR_FILE_DATE_STR}'
                                 AND date_delivered_fulfilled <= '{VENDOR_FILE_DATE_STR}'
                                     THEN date_delivered_fulfilled
                                WHEN date_delivered_fulfilled >= date_filled
                                 AND date_delivered_fulfilled > '{VENDOR_FILE_DATE_STR}'
                                 AND date_filled <= '{VENDOR_FILE_DATE_STR}'
                                     THEN date_filled
                                WHEN date_delivered_fulfilled > '{VENDOR_FILE_DATE_STR}'
                                 AND date_filled > '{VENDOR_FILE_DATE_STR}'
                                     THEN date_delivered_fulfilled
                                ELSE NULL
                            END,
                            '%Y%m%d'
                        ) AS DATE
                ),
            esdt.gen_ref_1_dt,
            CAST('{VENDOR_FILE_DATE_FMT}' AS DATE)
	    )																					AS date_service,
	/* date_written */
	CAP_DATE
	    (
            CAST(EXTRACT_DATE(txn.date_written, '%Y%m%d') AS DATE),
            esdt.gen_ref_1_dt,
            CAST('{VENDOR_FILE_DATE_FMT}' AS DATE)
	    )																					AS date_written,
	UPPER(txn.claim_indicator)																AS response_code_vendor,
	/* diagnosis_code */
	CLEAN_UP_DIAGNOSIS_CODE
	    (
	        txn.diagnosis_code,
	        '01',
            CAST
                (
                    EXTRACT_DATE
                        (
                            CASE
                                WHEN date_filled >= date_delivered_fulfilled
                                 AND date_filled <= '{VENDOR_FILE_DATE_STR}'
                                     THEN date_filled
                                WHEN date_delivered_fulfilled >= date_filled
                                 AND date_delivered_fulfilled <= '{VENDOR_FILE_DATE_STR}'
                                     THEN date_delivered_fulfilled
                                WHEN date_filled >= date_delivered_fulfilled
                                 AND date_filled > '{VENDOR_FILE_DATE_STR}'
                                 AND date_delivered_fulfilled <= '{VENDOR_FILE_DATE_STR}'
                                     THEN date_delivered_fulfilled
                                WHEN date_delivered_fulfilled >= date_filled
                                 AND date_delivered_fulfilled > '{VENDOR_FILE_DATE_STR}'
                                 AND date_filled <= '{VENDOR_FILE_DATE_STR}'
                                     THEN date_filled
                                WHEN date_delivered_fulfilled > '{VENDOR_FILE_DATE_STR}'
                                 AND date_filled > '{VENDOR_FILE_DATE_STR}'
                                     THEN date_delivered_fulfilled
                                ELSE NULL
                            END,
                            '%Y%m%d'
                        ) AS DATE
                )
	    )																					AS diagnosis_code,
	/* diagnosis_code_qual */
	CASE
	    WHEN 0 <> LENGTH(TRIM(COALESCE(txn.diagnosis_code)))
	         THEN '01'
	    ELSE NULL
	END 																					AS diagnosis_code_qual,
	CLEAN_UP_NDC_CODE(txn.dispensed_ndc_number)												AS ndc_code,
	MD5(txn.prescription_number_filler)														AS rx_number,
	/* rx_number_qual */
	CASE
	    WHEN 0 <> LENGTH(TRIM(COALESCE(txn.prescription_number_filler)))
	         THEN '1'
	    ELSE NULL
	END 																					AS rx_number_qual,
	txn.bank_identification_number															AS bin_number,
	txn.processor_control_number															AS processor_control_number,
	CAST(txn.new_refill_counter AS INTEGER)													AS fill_number,
	txn.number_of_refills_authorized														AS refill_auth_amount,
	CAST(0.001 * CAST(txn.quantity_dispensed AS INTEGER) AS FLOAT)							AS dispensed_quantity,
	txn.unit_of_measure																		AS unit_of_measure,
	CAST(txn.days_supply AS INTEGER)														AS days_supply,
	CLEAN_UP_NPI_CODE(txn.pharmacy_npi)														AS pharmacy_npi,
	txn.plan_code																			AS payer_name,
	/* payer_type */
	CASE
	    WHEN txn.payment_type = '1' THEN 'Cash'
	    WHEN txn.payment_type = '2' THEN 'Medicaid'
	    WHEN txn.payment_type = '3' THEN 'Third Party'
	    ELSE NULL
	END 																					AS payer_type,
	/* compound_code */
	CASE
	    WHEN COALESCE(txn.compound_code, 'XXX') IN ('0', '1', '2')
	         THEN txn.compound_code
	    ELSE NULL
	END 																					AS compound_code,
	/* dispensed_as_written */
	CASE
	    WHEN COALESCE(txn.dispensed_as_written, 'XXX') IN ('0', '1', '2', '3', '4', '5', '6', '7', '8', '9')
	         THEN txn.dispensed_as_written
	    ELSE NULL
	END 																					AS dispensed_as_written,
	/* prescription_origin */
	CASE
	    WHEN txn.origin_of_rx = '1' THEN 'WRITTEN'
	    WHEN txn.origin_of_rx = '2' THEN 'PHONE'
	    WHEN txn.origin_of_rx = '3' THEN 'ESCRIPT'
	    WHEN txn.origin_of_rx = '4' THEN 'FAX'
	    ELSE NULL
	END 																					AS prescription_origin,
	CLEAN_UP_NDC_CODE(txn.prescribed_ndc_number)											AS orig_prescribed_product_service_code,
	/* orig_prescribed_product_service_code_qual */
	CASE
	    WHEN 0 <> LENGTH(TRIM(COALESCE(CLEAN_UP_NDC_CODE(txn.prescribed_ndc_number), '')))
	         THEN '03'
	    ELSE NULL
	END 																					AS orig_prescribed_product_service_code_qual,
	txn.level_of_service																	AS level_of_service,
	CLEAN_UP_NPI_CODE(txn.prescriber_npi)													AS prov_prescribing_npi,
	/* prov_prescribing_dea_id */
	CASE
	    WHEN COALESCE(txn.level_of_service, 'XXX') <> '6'
	         THEN txn.prescriber_dea_number
	    ELSE NULL
	END 																					AS prov_prescribing_dea_id,
	/* prov_prescribing_state_license */
	CASE
	    WHEN COALESCE(txn.level_of_service, 'XXX') <> '6'
	         THEN txn.state_license_number_for_prescriber
	    ELSE NULL
	END 																					AS prov_prescribing_state_license,
	/* prov_prescribing_name_1 */
	CASE
	    WHEN COALESCE(txn.level_of_service, 'XXX') <> '6'
	         THEN txn.prescriber_last_name
	    ELSE NULL
	END 																					AS prov_prescribing_name_1,
	/* prov_prescribing_name_2 */
	CASE
	    WHEN COALESCE(txn.level_of_service, 'XXX') <> '6'
	         THEN txn.prescriber_first_name
	    ELSE NULL
	END 																					AS prov_prescribing_name_2,
	/* prov_prescribing_city */
	CASE
	    WHEN COALESCE(txn.level_of_service, 'XXX') <> '6'
	         THEN txn.prescriber_city
	    ELSE NULL
	END 																					AS prov_prescribing_city,
	/* prov_prescribing_state */
	CASE
	    WHEN COALESCE(txn.level_of_service, 'XXX') <> '6'
	         THEN txn.prescriber_state
	    ELSE NULL
	END 																					AS prov_prescribing_state,
	/* prov_prescribing_zip */
	CASE
	    WHEN COALESCE(txn.level_of_service, 'XXX') <> '6'
	         THEN txn.prescriber_zip_code
	    ELSE NULL
	END 																					AS prov_prescribing_zip,
	txn.coordination_of_benefits_counter													AS cob_count,
	CAST(0.01 * CAST(txn.copay_coinsurance_amount AS INTEGER) AS FLOAT)						AS copay_coinsurance,
	/* basis_of_cost_determination */
	CASE
	    WHEN COALESCE(txn.basis_of_ingredient_cost_submitted, 'XXX') = 'M' THEN '06'
	    WHEN COALESCE(txn.basis_of_ingredient_cost_submitted, 'XXX') = 'A' THEN '01'
	    WHEN COALESCE(txn.basis_of_ingredient_cost_submitted, 'XXX') = 'U' THEN '07'
	    WHEN COALESCE(txn.basis_of_ingredient_cost_submitted, 'XXX') = 'O' THEN '09'
	    ELSE NULL
	END 																					AS basis_of_cost_determination,
	CAST(0.01 * CAST(txn.ingredient_cost AS INTEGER) AS FLOAT)								AS submitted_ingredient_cost,
	CAST(0.01 * CAST(txn.dispensing_fee AS INTEGER) AS FLOAT)								AS submitted_dispensing_fee,
	/* basis_of_reimbursement_determination */
	CASE
	    WHEN COALESCE(txn.basis_of_ingredient_cost_reimbursed, 'XXX') = 'M' THEN '06'
	    WHEN COALESCE(txn.basis_of_ingredient_cost_reimbursed, 'XXX') = 'A' THEN '02'
	    WHEN COALESCE(txn.basis_of_ingredient_cost_reimbursed, 'XXX') = 'U' THEN '04'
	    WHEN COALESCE(txn.basis_of_ingredient_cost_reimbursed, 'XXX') = 'O' THEN '07'
	    ELSE NULL
	END 																					AS basis_of_reimbursement_determination,
	CAST(0.01 * CAST(txn.ingredient_cost_paid AS INTEGER) AS FLOAT) 						AS paid_ingredient_cost,
	CAST(0.01 * CAST(txn.dispensing_fee_paid AS INTEGER) AS FLOAT)							AS paid_dispensing_fee,
	CAST(0.01 * CAST(txn.reimbursed_amount AS INTEGER) AS FLOAT)							AS paid_gross_due,
	CAST(0.01 * CAST(txn.total_amount_paid_by_patient AS INTEGER) AS FLOAT)					AS paid_patient_pay,
	txn.indicator_for_coupon_type															AS coupon_type,
	txn.coupon_id   																		AS coupon_number,
	CAST(0.01 * CAST(txn.coupon_face_value AS INTEGER) AS FLOAT)							AS coupon_value,
	txn.pharmacy_ncpdp_number																AS pharmacy_other_id,
	/* pharmacy_other_qual */
	CASE
	    WHEN 0 <> LENGTH(TRIM(COALESCE(txn.pharmacy_ncpdp_number, '')))
	         THEN '07'
        ELSE NULL
	END 																					AS pharmacy_other_qual,
	txn.pharmacy_zip_code																	AS pharmacy_postal_code,
	CAST(EXTRACT_DATE(txn.date_filled, '%Y%m%d') AS DATE)									AS logical_delete_reason_date,
	/* logical_delete_reason */
	CASE
	    WHEN COALESCE(txn.claim_indicator, 'XXX') = 'R'
	         THEN 'Reversal'
	    ELSE NULL
	END 																					AS logical_delete_reason,
	'pdx'   																				AS part_provider,
	/* part_best_date */
	(CASE WHEN 0 = LENGTH
	                (
	                    TRIM
	                        (
	                            COALESCE
	                                (
	                                    CAP_DATE
	                                        (
                                                CAST
                                                    (
                                                        EXTRACT_DATE
                                                            (
                                                                CASE
                                                                    WHEN date_filled >= date_delivered_fulfilled
                                                                     AND date_filled <= '{VENDOR_FILE_DATE_STR}'
                                                                         THEN date_filled
                                                                    WHEN date_delivered_fulfilled >= date_filled
                                                                     AND date_delivered_fulfilled <= '{VENDOR_FILE_DATE_STR}'
                                                                         THEN date_delivered_fulfilled
                                                                    WHEN date_filled >= date_delivered_fulfilled
                                                                     AND date_filled > '{VENDOR_FILE_DATE_STR}'
                                                                     AND date_delivered_fulfilled <= '{VENDOR_FILE_DATE_STR}'
                                                                         THEN date_delivered_fulfilled
                                                                    WHEN date_delivered_fulfilled >= date_filled
                                                                     AND date_delivered_fulfilled > '{VENDOR_FILE_DATE_STR}'
                                                                     AND date_filled <= '{VENDOR_FILE_DATE_STR}'
                                                                         THEN date_filled
                                                                    WHEN date_delivered_fulfilled > '{VENDOR_FILE_DATE_STR}'
                                                                     AND date_filled > '{VENDOR_FILE_DATE_STR}'
                                                                         THEN date_delivered_fulfilled
                                                                    ELSE NULL
                                                                END,
                                                                '%Y%m%d'
                                                            ) AS DATE
                                                    ),
                                                ahdt.gen_ref_1_dt,
                                                CAST('{VENDOR_FILE_DATE_FMT}' AS DATE)
                                            )
	                                            , ''
	                               )
	                       )
	               )
	           THEN '0_PREDATES_HVM_HISTORY'
	      ELSE CONCAT
	            (
	                SUBSTR
	                    (
	                        CASE
                                WHEN date_filled >= date_delivered_fulfilled
                                 AND date_filled <= '{VENDOR_FILE_DATE_STR}'
                                     THEN date_filled
                                WHEN date_delivered_fulfilled >= date_filled
                                 AND date_delivered_fulfilled <= '{VENDOR_FILE_DATE_STR}'
                                     THEN date_delivered_fulfilled
                                WHEN date_filled >= date_delivered_fulfilled
                                 AND date_filled > '{VENDOR_FILE_DATE_STR}'
                                 AND date_delivered_fulfilled <= '{VENDOR_FILE_DATE_STR}'
                                     THEN date_delivered_fulfilled
                                WHEN date_delivered_fulfilled >= date_filled
                                 AND date_delivered_fulfilled > '{VENDOR_FILE_DATE_STR}'
                                 AND date_filled <= '{VENDOR_FILE_DATE_STR}'
                                     THEN date_filled
                                WHEN date_delivered_fulfilled > '{VENDOR_FILE_DATE_STR}'
                                 AND date_filled > '{VENDOR_FILE_DATE_STR}'
                                     THEN date_delivered_fulfilled
                                ELSE NULL
                            END, 1, 4
	                    ), '-',
	                SUBSTR
	                    (
	                        CASE
                                WHEN date_filled >= date_delivered_fulfilled
                                 AND date_filled <= '{VENDOR_FILE_DATE_STR}'
                                     THEN date_filled
                                WHEN date_delivered_fulfilled >= date_filled
                                 AND date_delivered_fulfilled <= '{VENDOR_FILE_DATE_STR}'
                                     THEN date_delivered_fulfilled
                                WHEN date_filled >= date_delivered_fulfilled
                                 AND date_filled > '{VENDOR_FILE_DATE_STR}'
                                 AND date_delivered_fulfilled <= '{VENDOR_FILE_DATE_STR}'
                                     THEN date_delivered_fulfilled
                                WHEN date_delivered_fulfilled >= date_filled
                                 AND date_delivered_fulfilled > '{VENDOR_FILE_DATE_STR}'
                                 AND date_filled <= '{VENDOR_FILE_DATE_STR}'
                                     THEN date_filled
                                WHEN date_delivered_fulfilled > '{VENDOR_FILE_DATE_STR}'
                                 AND date_filled > '{VENDOR_FILE_DATE_STR}'
                                     THEN date_delivered_fulfilled
                                ELSE NULL
                            END, 5, 2
	                    /* Add the day portion of the date, to match the historical part_best_date. */
	                    ), '-01'
                )
	  END)                                                                              AS part_best_date
 FROM pdx_norm_txn_dedup txn
 LEFT OUTER JOIN pdx_payload pay
   ON txn.hvjoinkey = pay.hvjoinkey
 LEFT OUTER JOIN ref_gen_ref esdt
   ON 1 = 1
  AND esdt.hvm_vdr_feed_id = 65
  AND esdt.gen_ref_domn_nm = 'EARLIEST_VALID_SERVICE_DATE'
 LEFT OUTER JOIN ref_gen_ref ahdt
   ON 1 = 1
  AND ahdt.hvm_vdr_feed_id = 65
  AND ahdt.gen_ref_domn_nm = 'HVM_AVAILABLE_HISTORY_START_DATE'
WHERE SUBSTR(txn.pharmacy_ncpdp_number, 1, 2) <> 'DT'
   OR 0 <> LENGTH(TRIM(COALESCE(txn.pharmacy_npi, '')))
