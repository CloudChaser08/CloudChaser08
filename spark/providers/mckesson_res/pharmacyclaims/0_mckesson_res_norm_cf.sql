SELECT
    MONOTONICALLY_INCREASING_ID()            AS record_id,
    t.prescriptionkey                        AS claim_id,
    mp.hvid                                  AS hvid,
    CURRENT_DATE()                           AS created,
    6                                        AS model_version,
    SPLIT(t.input_file_name, '/')[SIZE(SPLIT(t.input_file_name, '/')) - 1]
                                             AS data_set,
    '36'                                     AS data_feed,
    '119'                                    AS data_vendor,
    mp.gender                                AS patient_gender,
    mp.age                                   AS patient_age,
    mp.yearOfBirth                           AS patient_year_of_birth,
    mp.threeDigitZip                         AS patient_zip3,
    TRIM(UPPER(mp.state))                    AS patient_state,
    extract_date(
        t.datefilled, '%m/%d/%Y', CAST('{EARLIEST_SERVICE_DATE}' AS DATE), CAST('{AVAILABLE_START_DATE}' AS DATE)
        )                                    AS date_service,
    extract_date(
        t.datewritten, '%m/%d/%Y', CAST('{EARLIEST_SERVICE_DATE}' AS DATE), CAST('{AVAILABLE_START_DATE}' AS DATE)
        )                                    AS date_written,
    extract_date(
        t.claimtransactiondate, '%Y/%m/%d', CAST('1999-01-01' AS DATE), CAST('{AVAILABLE_START_DATE}' AS DATE)
        )                                    AS date_authorized,
    t.fillertransactiontime                  AS time_authorized,
    CASE
    WHEN t.rebillindicator = 'O' OR t.rebillindicator = 'N'
    THEN 'Original'
    WHEN t.rebillindicator = 'R' OR t.rebillindicator = 'Y'
    THEN 'Rebilled'
    END                                      AS transaction_code_vendor,
    t.camstatuscode                          AS response_code_std,
    t.rejectcode1                            AS reject_reason_code_1,
    t.rejectcode2                            AS reject_reason_code_2,
    t.rejectcode3                            AS reject_reason_code_3,
    t.rejectcode4                            AS reject_reason_code_4,
    t.rejectcode5                            AS reject_reason_code_5,
    t.diagnosiscode                          AS diagnosis_code,
    t.procedurecode                          AS procedure_code,
    t.procedurecodequalifier                 AS procedure_code_qual,
    t.dispensedndcnumber                     AS ndc_code,
    t.fillerproductserviceid                 AS product_service_id,
    t.productserviceidqualifier              AS product_service_id_qual,
    MD5(t.prescriptionnumber)                AS rx_number,
    t.prescriptionnumberqualifier            AS rx_number_qual,
    t.binnumber                              AS bin_number,
    t.processcontrolnumber                   AS processor_control_number,
    t.refillcode                             AS fill_number,
    t.numberofrefillsauthorized              AS refill_auth_amount,
    t.metricquantity                         AS dispensed_quantity,
    t.unitofmeasure                          AS unit_of_measure,
    t.dayssupply                             AS days_supply,
    CLEAN_UP_NPI_CODE(t.customernpinumber)   AS pharmacy_npi,
    CLEAN_UP_NPI_CODE(t.pharmacistnpi)       AS prov_dispensing_npi,
    t.payerid                                AS payer_id,
    t.payeridqualifier                       AS payer_id_qual,
    t.payername                              AS payer_name,
    t.payerorgname                           AS payer_org_name,
    t.payerplanid                            AS payer_plan_id,
    t.planname                               AS payer_plan_name,
    t.payertype                              AS payer_type,
    t.compoundindicator                      AS compound_code,
    t.unitdoseindicator                      AS unit_dose_indicator,
    t.dispensedaswritten                     AS dispensed_as_written,
    t.originofrx                             AS prescription_origin,
    t.submissionclarificationcode            AS submission_clarification,
    t.prescribedndcnumber                    AS orig_prescribed_product_service_code,
    t.prescribedproductservicecodequalifier  AS orig_prescribed_product_service_code_qual,
    t.prescribedquantity                     AS orig_prescribed_quantity,
    t.priorauthtypecode                      AS prior_auth_type_code,
    t.levelofservice                         AS level_of_service,
    t.reasonforservice                       AS reason_for_service,
    t.professionalservicecode                AS professional_service_code,
    t.resultofservicecode                    AS result_of_service_code,
    CLEAN_UP_NPI_CODE(t.prescribernpi)       AS prov_prescribing_npi,
    CLEAN_UP_NPI_CODE(t.primarycareproviderid)
                                             AS prov_primary_care_npi,
    t.cobcount                               AS cob_count,
    t.usualandcustomary                      AS usual_and_customary_charge,
    t.productselectionattributed             AS product_selection_attributed,
    t.otherpayeramountpaid                   AS other_payer_recognized,
    t.periodicdeductibleapplied              AS periodic_deductible_applied,
    t.periodicbenefitexceed                  AS periodic_benefit_exceed,
    t.accumulateddeductible                  AS accumulated_deductible,
    t.remainingdeductible                    AS remaining_deductible,
    t.remainingbenefit                       AS remaining_benefit,
    t.copayamount                            AS copay_coinsurance,
    t.basisofingredientcostsubmitted         AS basis_of_cost_determination,
    t.ingredientcostsubmitted                AS submitted_ingredient_cost,
    t.dispensingfeesubmitted                 AS submitted_dispensing_fee,
    t.incentiveamountsubmitted               AS submitted_incentive,
    t.grossamountdue                         AS submitted_gross_due,
    t.professionalservicefeesubmitted        AS submitted_professional_service_fee,
    t.patientpayamountsubmitted              AS submitted_patient_pay,
    t.otheramountsubmittedqualfier           AS submitted_other_claimed_qual,
    t.basisofingredientcostpaid              AS basis_of_reimbursement_determination,
    t.ingredientcostpaid                     AS paid_ingredient_cost,
    t.dispensingfeepaid                      AS paid_dispensing_fee,
    t.IncentiveAmountPaid                    AS paid_incentive,
    t.grossamountduepaid                     AS paid_gross_due,
    t.professionalservicefeepaid             AS paid_professional_service_fee,
    t.patientpayamountpaid                   AS paid_patient_pay,
    t.otheramountpaidqualifier               AS paid_other_claimed_qual,
    t.paidotherclaimed                       AS paid_other_claimed,
    t.taxexemptindicator                     AS tax_exempt_indicator,
    t.coupontype                             AS coupon_type,
    t.couponidnumber                         AS coupon_number,
    t.couponfacevalue                        AS coupon_value,
    t.pharmacyotherid                        AS pharmacy_other_id,
    t.pharmacyotheridqualifier               AS pharmacy_other_qual,
    t.customerzipcode                        AS pharmacy_postal_code,
    t.providerdispensingnpi                  AS prov_dispensing_id,
    t.provdispensingqual                     AS prov_dispensing_qual,
    t.primarycareproviderid                  AS prov_primary_care_id,
    t.primarycareprovideridqualifier         AS prov_primary_care_qual,
    t.otherpayercoveragetype                 AS other_payer_coverage_type,
    t.otherpayercoverageid                   AS other_payer_coverage_id,
    t.otherpayercoverageidqualifier          AS other_payer_coverage_qual,
    t.otherpayerdate                         AS other_payer_date,
    t.otherpayercoveragecode                 AS other_payer_coverage_code,
    CASE
    WHEN COALESCE(t.rejectcode1, '') != ''
    OR COALESCE(t.rejectcode2, '') != ''
    OR COALESCE(t.rejectcode3, '') != ''
    OR COALESCE(t.rejectcode4, '') != ''
    OR COALESCE(t.rejectcode5, '') != ''
    OR COALESCE(t.camstatuscode, 'X') = 'R'
    THEN 'Claim Rejected'
    END                                      AS logical_delete_reason,
    'mckesson_res'                           AS part_provider,
    
    /* part_best_date */
	CASE
	    WHEN 0 = LENGTH(TRIM(COALESCE
	                            (
	                                CAP_DATE
                                        (
                                            CAST(EXTRACT_DATE(t.datefilled, '%m/%d/%Y') AS DATE),
                                            COALESCE(CAST('{AVAILABLE_START_DATE}' AS DATE), CAST('{EARLIEST_SERVICE_DATE}' AS DATE)),
                                            CAST(EXTRACT_DATE('{VDR_FILE_DT}', '%Y-%m-%d') AS DATE)
                                        ),
                                    ''
                                )))
	        THEN '0_PREDATES_HVM_HISTORY'
	    ELSE SUBSTR(CAST(EXTRACT_DATE(t.datefilled, '%m/%d/%Y') AS DATE), 1, 7)
	END                                                                                 AS part_best_date
FROM txn t
    LEFT JOIN matching_payload mp ON t.hvjoinkey = mp.hvjoinkey

-- exclude blank hvJoinKey values
WHERE TRIM(COALESCE(t.hvjoinkey, '')) <> ''
