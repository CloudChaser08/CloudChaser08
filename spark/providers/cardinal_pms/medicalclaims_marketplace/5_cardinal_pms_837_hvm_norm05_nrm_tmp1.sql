SELECT
	txn.claim_id,
	txn.hvid,
	txn.created,
	txn.model_version,
	txn.data_set,
	txn.data_feed,
	txn.data_vendor,
	txn.vendor_org_id,
	txn.patient_gender,
	txn.patient_year_of_birth,
	txn.claim_type,
	txn.date_service,
	txn.date_service_end,
	txn.place_of_service_std_id,
	txn.service_line_number,
	txn.service_line_id,
	txn.diagnosis_code,
	txn.diagnosis_priority,
	txn.procedure_code,
	txn.procedure_code_qual,
	txn.procedure_units_billed,
	txn.procedure_modifier_1,
	txn.procedure_modifier_2,
	txn.procedure_modifier_3,
	txn.procedure_modifier_4,
	txn.ndc_code,
	txn.line_charge,
	txn.total_charge,
	txn.prov_rendering_npi,
	txn.prov_billing_npi,
	txn.prov_referring_npi,
	txn.prov_facility_npi,
	txn.payer_vendor_id,
	txn.payer_name,
	txn.prov_rendering_vendor_id,
	txn.prov_rendering_name_1,
	txn.prov_rendering_std_taxonomy,
	txn.prov_billing_vendor_id,
	txn.prov_billing_name_1,
	txn.prov_billing_std_taxonomy,
	txn.prov_referring_vendor_id,
	txn.prov_referring_name_1,
	txn.prov_facility_vendor_id,
	txn.prov_facility_name_1,
	txn.prov_facility_address_1,
	txn.prov_facility_city,
	txn.prov_facility_state,
	txn.prov_facility_zip,
	txn.part_provider,
	txn.part_best_date
 FROM cardinal_pms_837_hvm_norm02_nrm_lns txn
UNION ALL
/* Select claim-level rows that don't need to be exploded. */
SELECT
	txn.claim_id,
	txn.hvid,
	txn.created,
	txn.model_version,
	txn.data_set,
	txn.data_feed,
	txn.data_vendor,
	txn.vendor_org_id,
	txn.patient_gender,
	txn.patient_year_of_birth,
	txn.claim_type,
	txn.date_service,
	txn.date_service_end,
	txn.place_of_service_std_id,
	txn.service_line_number,
	txn.service_line_id,
	txn.diagnosis_code,
	txn.diagnosis_priority,
	txn.procedure_code,
	txn.procedure_code_qual,
	txn.procedure_units_billed,
	txn.procedure_modifier_1,
	txn.procedure_modifier_2,
	txn.procedure_modifier_3,
	txn.procedure_modifier_4,
	txn.ndc_code,
	txn.line_charge,
	txn.total_charge,
	txn.prov_rendering_npi,
	txn.prov_billing_npi,
	txn.prov_referring_npi,
	txn.prov_facility_npi,
	txn.payer_vendor_id,
	txn.payer_name,
	txn.prov_rendering_vendor_id,
	txn.prov_rendering_name_1,
	txn.prov_rendering_std_taxonomy,
	txn.prov_billing_vendor_id,
	txn.prov_billing_name_1,
	txn.prov_billing_std_taxonomy,
	txn.prov_referring_vendor_id,
	txn.prov_referring_name_1,
	txn.prov_facility_vendor_id,
	txn.prov_facility_name_1,
	txn.prov_facility_address_1,
	txn.prov_facility_city,
	txn.prov_facility_state,
	txn.prov_facility_zip,
	txn.part_provider,
	/* part_best_date */
	CASE
	    WHEN CAP_DATE
	            (
	                txn.date_service,
                    ahdt.gen_ref_1_dt,
                    CAST('{VDR_FILE_DT}' AS DATE)
	            ) IS NULL
	         THEN '0_PREDATES_HVM_HISTORY'
	    ELSE CONCAT
	            (
	                SUBSTR(CAST(txn.date_service AS STRING), 1, 7), '-01'
	            )
	END 																					AS part_best_date
 FROM cardinal_pms_837_hvm_norm04_nrm_clm txn
CROSS JOIN
    (
        SELECT gen_ref_1_dt
         FROM ref_gen_ref
        WHERE hvm_vdr_feed_id = 41
          AND gen_ref_domn_nm = 'HVM_AVAILABLE_HISTORY_START_DATE'
        LIMIT 1
    ) ahdt
/* Select where there is no date_service, or there is no date_service_end,
   or the two dates are the same, or the two dates are more than a year apart. */
WHERE txn.date_service IS NULL
   OR txn.date_service_end IS NULL
   OR COALESCE(txn.date_service_end, CAST('1900-01-01' AS DATE)) =
      COALESCE(txn.date_service, CAST('1900-01-01' AS DATE))
   OR DATEDIFF
        (
            COALESCE(txn.date_service_end, CAST('1900-01-01' AS DATE)),
            COALESCE(txn.date_service, CAST('1900-01-01' AS DATE))
        ) > 365
UNION ALL
/* Select claim-level rows that do need to be exploded. */
SELECT
	txn.claim_id,
	txn.hvid,
	txn.created,
	txn.model_version,
	txn.data_set,
	txn.data_feed,
	txn.data_vendor,
	txn.vendor_org_id,
	txn.patient_gender,
	txn.patient_year_of_birth,
	txn.claim_type,
	DATE_ADD(date_service, dei.d)                                                           AS date_service,
	DATE_ADD(date_service, dei.d)                                                           AS date_service_end,
	txn.place_of_service_std_id,
	txn.service_line_number,
	txn.service_line_id,
	txn.diagnosis_code,
	txn.diagnosis_priority,
	txn.procedure_code,
	txn.procedure_code_qual,
	txn.procedure_units_billed,
	txn.procedure_modifier_1,
	txn.procedure_modifier_2,
	txn.procedure_modifier_3,
	txn.procedure_modifier_4,
	txn.ndc_code,
	txn.line_charge,
	txn.total_charge,
	txn.prov_rendering_npi,
	txn.prov_billing_npi,
	txn.prov_referring_npi,
	txn.prov_facility_npi,
	txn.payer_vendor_id,
	txn.payer_name,
	txn.prov_rendering_vendor_id,
	txn.prov_rendering_name_1,
	txn.prov_rendering_std_taxonomy,
	txn.prov_billing_vendor_id,
	txn.prov_billing_name_1,
	txn.prov_billing_std_taxonomy,
	txn.prov_referring_vendor_id,
	txn.prov_referring_name_1,
	txn.prov_facility_vendor_id,
	txn.prov_facility_name_1,
	txn.prov_facility_address_1,
	txn.prov_facility_city,
	txn.prov_facility_state,
	txn.prov_facility_zip,
	txn.part_provider,
	CASE
	    WHEN CAP_DATE
	            (
	                DATE_ADD(txn.date_service, dei.d),
                    txn.gen_ref_1_dt,
                    CAST('{VDR_FILE_DT}' AS DATE)
	            ) IS NULL
	         THEN '0_PREDATES_HVM_HISTORY'
	    ELSE CONCAT
	            (
	                SUBSTR(CAST(DATE_ADD(txn.date_service, dei.d) AS STRING), 1, 7), '-01'
	            )
	END 																					AS part_best_date
 FROM cardinal_pms_837_hvm_norm06_nrm_tmp2 txn
CROSS JOIN date_explode_indices dei
   ON DATE_ADD(txn.date_service, dei.d) <= COALESCE(txn.date_service_end, CAST('1900-01-01' AS DATE))
/* Select where there is a date_service, and there is a date_service_end, and the
   two dates are not the same, and the two dates are not more than a year apart. */
WHERE DATE_ADD(txn.date_service, dei.d) <= COALESCE(txn.date_service_end, CAST('1900-01-01' AS DATE))