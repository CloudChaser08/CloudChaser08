SELECT
    claim_id,
    hvid,
    model_version,
    data_set,
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
    inst_date_admitted,
    inst_admit_type_std_id,
    inst_admit_source_std_id,
    inst_discharge_status_std_id,
    inst_type_of_bill_std_id,
    inst_drg_std_id,
    place_of_service_std_id,
    service_line_number,
    /* diagnosis_code */
    /* We left the privacy filtering to the final normalization */
    /* so we can accurately add the claim-level diagnoses.      */
    CLEAN_UP_DIAGNOSIS_CODE
        (
            diagnosis_code,
            diagnosis_code_qual,
            date_service
        )                                                                                   AS diagnosis_code,
    diagnosis_code_qual,
    diagnosis_priority,
    admit_diagnosis_ind,
    procedure_code,
    procedure_code_qual,
    principal_proc_ind,
    procedure_units_billed,
    procedure_modifier_1,
    procedure_modifier_2,
    procedure_modifier_3,
    procedure_modifier_4,
    revenue_code,
    ndc_code,
    medical_coverage_type,
    line_charge,
    line_allowed,
    total_charge,
    total_allowed,
    prov_rendering_npi,
    prov_billing_npi,
    prov_referring_npi,
    prov_facility_npi,
    payer_name,
    payer_plan_id,
    prov_rendering_state_license,
    prov_rendering_upin,
    prov_rendering_name_1,
    prov_rendering_std_taxonomy,
    prov_rendering_vendor_specialty,
    prov_billing_tax_id,
    prov_billing_ssn,
    prov_billing_state_license,
    prov_billing_upin,
    prov_billing_name_1,
    prov_billing_address_1,
    prov_billing_address_2,
    prov_billing_city,
    prov_billing_state,
    prov_billing_zip,
    prov_billing_std_taxonomy,
    prov_referring_name_1,
    prov_facility_state_license,
    prov_facility_name_1,
    prov_facility_address_1,
    prov_facility_address_2,
    prov_facility_city,
    prov_facility_state,
    prov_facility_zip,
    medical_claim_link_text,
    part_provider,
    /* part_best_date */
    CASE
        WHEN CAP_DATE
                (
                    DATE_ADD(date_service, dei.d),
                    ahdt.gen_ref_1_dt,
                    CAST('${VDR_FILE_DT}' AS DATE)
                ) IS NULL
             THEN '0_PREDATES_HVM_HISTORY'
        ELSE CONCAT
                (
                    SUBSTR(CAST(DATE_ADD(date_service, dei.d) AS STRING), 1, 7), '-01'
                )
    END                                                                                     AS part_best_date
 FROM waystar_norm01_norm_lines sln
CROSS JOIN
    (
        SELECT gen_ref_1_dt
         FROM ref_gen_ref 
        WHERE hvm_vdr_feed_id = 24
          AND gen_ref_domn_nm = 'HVM_AVAILABLE_HISTORY_START_DATE'
        LIMIT 1
    ) ahdt
CROSS JOIN date_explode_indices dei
   ON COALESCE(sln.claim_type, 'X') = 'P'
  AND sln.date_service IS NOT NULL
  AND sln.date_service_end IS NOT NULL
  AND DATEDIFF
        (
            COALESCE(sln.date_service_end, CAST('1900-01-01' AS DATE)),
            COALESCE(sln.date_service, CAST('1900-01-01' AS DATE))
        ) <> 0
  AND DATEDIFF
        (
            COALESCE(sln.date_service_end, CAST('1900-01-01' AS DATE)),
            COALESCE(sln.date_service, CAST('1900-01-01' AS DATE))
        ) <= 365
  AND DATE_ADD(sln.date_service, dei.d) <= COALESCE(sln.date_service_end, CAST('1900-01-01' AS DATE))
/* Select only professional claims, where both dates are populated, the */
/* two dates are not the same, and the two dates are a year apart or less. */
WHERE COALESCE(sln.claim_type, 'X') = 'P'
  AND sln.date_service IS NOT NULL
  AND sln.date_service_end IS NOT NULL
  AND DATEDIFF
        (
            COALESCE(sln.date_service_end, CAST('1900-01-01' AS DATE)),
            COALESCE(sln.date_service, CAST('1900-01-01' AS DATE))
        ) <> 0
  AND DATEDIFF
        (
            COALESCE(sln.date_service_end, CAST('1900-01-01' AS DATE)),
            COALESCE(sln.date_service, CAST('1900-01-01' AS DATE))
        ) <= 365
  AND DATE_ADD(sln.date_service, dei.d) <= COALESCE(sln.date_service_end, CAST('1900-01-01' AS DATE))
