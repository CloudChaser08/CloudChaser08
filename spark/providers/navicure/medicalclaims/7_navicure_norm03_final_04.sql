SELECT
	claim_id,
	hvid,
	model_version,
	data_feed,
	data_vendor,
	patient_gender,
	patient_age,
	patient_year_of_birth,
	patient_zip3,
	patient_state,
	claim_type,
	date_received,
	DATE_ADD(date_service, dei.d)                                                           AS date_service,
	DATE_ADD(date_service, dei.d)                                                           AS date_service_end,
	place_of_service_std_id,
	service_line_number,
	diagnosis_code,
	diagnosis_code_qual,
	diagnosis_priority,
	procedure_code,
	procedure_code_qual,
	procedure_units_billed,
	procedure_modifier_1,
	procedure_modifier_2,
	procedure_modifier_3,
	procedure_modifier_4,
	ndc_code,
	medical_coverage_type,
	line_charge,
	total_charge,
	prov_rendering_npi,
	prov_billing_npi,
	prov_referring_npi,
	prov_facility_npi,
	payer_name,
	payer_plan_id,
	prov_rendering_state_license,
	prov_rendering_upin,
	prov_rendering_commercial_id,
	prov_rendering_name_1,
	prov_rendering_std_taxonomy,
	prov_billing_tax_id,
	prov_billing_ssn,
	prov_billing_state_license,
	prov_billing_commercial_id,
	prov_billing_name_1,
	prov_billing_address_1,
	prov_billing_address_2,
	prov_billing_city,
	prov_billing_state,
	prov_billing_zip,
	prov_billing_std_taxonomy,
	prov_referring_state_license,
	prov_referring_upin,
	prov_referring_commercial_id,
	prov_referring_name_1,
	prov_facility_name_1,
	prov_facility_address_1,
	prov_facility_address_2,
	prov_facility_city,
	prov_facility_state,
	prov_facility_zip,
	cob_payer_vendor_id_1,
	cob_payer_claim_filing_ind_code_1,
	cob_payer_vendor_id_2,
	cob_payer_claim_filing_ind_code_2,
	part_provider,
	/* part_best_date */
	CASE
	    WHEN CAP_DATE
	            (
	                DATE_ADD(date_service, dei.d),
                    ahdt.gen_ref_1_dt,
                    CAST(EXTRACT_DATE({VDR_FILE_DT}, '%Y%m%d') AS DATE)
	            ) IS NULL
	         THEN '0_PREDATES_HVM_HISTORY'
	    ELSE CONCAT
	            (
	                SUBSTR(CAST(DATE_ADD(date_service, dei.d) AS STRING), 1, 7), '-01'
	            )
	END 																					AS part_best_date
 FROM navicure_norm02_claims clm
CROSS JOIN
    (
        SELECT gen_ref_1_dt
         FROM ref_gen_ref 
        WHERE hvm_vdr_feed_id = 24
          AND gen_ref_domn_nm = 'HVM_AVAILABLE_HISTORY_START_DATE'
    ) ahdt
CROSS JOIN date_explode_indices dei
   ON clm.date_service IS NOT NULL
  AND clm.date_service_end IS NOT NULL
  AND DATEDIFF
        (
            COALESCE(clm.date_service_end, CAST('1900-01-01' AS DATE)),
            COALESCE(clm.date_service, CAST('1900-01-01' AS DATE))
        ) <> 0
  AND DATEDIFF
        (
            COALESCE(clm.date_service_end, CAST('1900-01-01' AS DATE)),
            COALESCE(clm.date_service, CAST('1900-01-01' AS DATE))
        ) <= 365
  AND DATE_ADD(clm.date_service, dei.d) <= COALESCE(clm.date_service_end, CAST('1900-01-01' AS DATE))
/* Select where both dates are populated, the two dates are */
/* not the same, and the two dates are a year apart or less. */
WHERE clm.date_service IS NOT NULL
  AND clm.date_service_end IS NOT NULL
  AND DATEDIFF
        (
            COALESCE(clm.date_service_end, CAST('1900-01-01' AS DATE)),
            COALESCE(clm.date_service, CAST('1900-01-01' AS DATE))
        ) <> 0
  AND DATEDIFF
        (
            COALESCE(clm.date_service_end, CAST('1900-01-01' AS DATE)),
            COALESCE(clm.date_service, CAST('1900-01-01' AS DATE))
        ) <= 365
  AND DATE_ADD(clm.date_service, dei.d) <= COALESCE(clm.date_service_end, CAST('1900-01-01' AS DATE))
