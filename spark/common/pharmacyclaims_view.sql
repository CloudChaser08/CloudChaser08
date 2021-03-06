DROP VIEW IF EXISTS default.pharmacyclaims;
CREATE VIEW default.pharmacyclaims (
        record_id,
        claim_id,
        hvid,
        created,
        model_version,
        data_set,
        data_feed,
        data_vendor,
        source_version,
        patient_gender,
        patient_age,
        patient_year_of_birth,
        patient_zip3,
        patient_state,
        date_service,
        date_written,
        year_of_injury,
        date_authorized,
        time_authorized,
        discharge_date,
        transaction_code_std,
        transaction_code_vendor,
        response_code_std,
        response_code_vendor,
        reject_reason_code_1,
        reject_reason_code_2,
        reject_reason_code_3,
        reject_reason_code_4,
        reject_reason_code_5,
        diagnosis_code,
        diagnosis_code_qual,
        procedure_code,
        procedure_code_qual,
        ndc_code,
        product_service_id,
        product_service_id_qual,
        rx_number,
        rx_number_qual,
        bin_number,
        processor_control_number,
        fill_number,
        refill_auth_amount,
        dispensed_quantity,
        unit_of_measure,
        days_supply,
        pharmacy_npi,
        prov_dispensing_npi,
        payer_id,
        payer_id_qual,
        payer_name,
        payer_parent_name,
        payer_org_name,
        payer_plan_id,
        payer_plan_name,
        payer_type,
        compound_code,
        unit_dose_indicator,
        dispensed_as_written,
        prescription_origin,
        submission_clarification,
        orig_prescribed_product_service_code,
        orig_prescribed_product_service_code_qual,
        orig_prescribed_quantity,
        prior_auth_type_code,
        level_of_service,
        reason_for_service,
        professional_service_code,
        result_of_service_code,
        prov_prescribing_npi,
        prov_prescribing_tax_id,
        prov_prescribing_dea_id,
        prov_prescribing_ssn,
        prov_prescribing_state_license,
        prov_prescribing_upin,
        prov_prescribing_commercial_id,
        prov_prescribing_name_1,
        prov_prescribing_name_2,
        prov_prescribing_address_1,
        prov_prescribing_address_2,
        prov_prescribing_city,
        prov_prescribing_state,
        prov_prescribing_zip,
        prov_prescribing_std_taxonomy,
        prov_prescribing_vendor_specialty,
        prov_primary_care_npi,
        cob_count,
        usual_and_customary_charge,
        product_selection_attributed,
        other_payer_recognized,
        periodic_deductible_applied,
        periodic_benefit_exceed,
        accumulated_deductible,
        remaining_deductible,
        remaining_benefit,
        copay_coinsurance,
        basis_of_cost_determination,
        submitted_ingredient_cost,
        submitted_dispensing_fee,
        submitted_incentive,
        submitted_gross_due,
        submitted_professional_service_fee,
        submitted_patient_pay,
        submitted_other_claimed_qual,
        submitted_other_claimed,
        basis_of_reimbursement_determination,
        paid_ingredient_cost,
        paid_dispensing_fee,
        paid_incentive,
        paid_gross_due,
        paid_professional_service_fee,
        paid_patient_pay,
        paid_other_claimed_qual,
        paid_other_claimed,
        tax_exempt_indicator,
        coupon_type,
        coupon_number,
        coupon_value,
        pharmacy_other_id,
        pharmacy_other_qual,
        pharmacy_postal_code,
        prov_dispensing_id,
        prov_dispensing_qual,
        prov_prescribing_id,
        prov_prescribing_qual,
        prov_primary_care_id,
        prov_primary_care_qual,
        other_payer_coverage_type,
        other_payer_coverage_id,
        other_payer_coverage_qual,
        other_payer_date,
        other_payer_coverage_code,
        logical_delete_reason,
        part_provider,
        part_processdate
        )
    AS SELECT record_id,
    claim_id,
    hvid,
    created,
    model_version,
    data_set,
    data_feed,
    data_vendor,
    source_version,
    patient_gender,
    patient_age,
    patient_year_of_birth,
    patient_zip3,
    patient_state,
    date_service,
    date_written,
    year_of_injury,
    date_authorized,
    time_authorized,
    discharge_date,
    transaction_code_std,
    transaction_code_vendor,
    response_code_std,
    response_code_vendor,
    reject_reason_code_1,
    reject_reason_code_2,
    reject_reason_code_3,
    reject_reason_code_4,
    reject_reason_code_5,
    diagnosis_code,
    diagnosis_code_qual,
    procedure_code,
    procedure_code_qual,
    CASE
      WHEN LENGTH(REGEXP_REPLACE(ndc_code, '[^0-9]', '')) >= 8
      THEN REGEXP_REPLACE(ndc_code, '[^0-9]', '')
    END AS ndc_code,
    product_service_id,
    product_service_id_qual,
    rx_number,
    rx_number_qual,
    bin_number,
    processor_control_number,
    fill_number,
    refill_auth_amount,
    dispensed_quantity,
    unit_of_measure,
    days_supply,
    pharmacy_npi,
    prov_dispensing_npi,
    payer_id,
    payer_id_qual,
    payer_name,
    payer_parent_name,
    payer_org_name,
    payer_plan_id,
    payer_plan_name,
    payer_type,
    compound_code,
    unit_dose_indicator,
    dispensed_as_written,
    prescription_origin,
    submission_clarification,
    orig_prescribed_product_service_code,
    orig_prescribed_product_service_code_qual,
    orig_prescribed_quantity,
    prior_auth_type_code,
    level_of_service,
    reason_for_service,
    professional_service_code,
    result_of_service_code,
    prov_prescribing_npi,
    prov_prescribing_tax_id,
    prov_prescribing_dea_id,
    prov_prescribing_ssn,
    prov_prescribing_state_license,
    prov_prescribing_upin,
    prov_prescribing_commercial_id,
    prov_prescribing_name_1,
    prov_prescribing_name_2,
    prov_prescribing_address_1,
    prov_prescribing_address_2,
    prov_prescribing_city,
    prov_prescribing_state,
    prov_prescribing_zip,
    prov_prescribing_std_taxonomy,
    prov_prescribing_vendor_specialty,
    prov_primary_care_npi,
    cob_count,
    usual_and_customary_charge,
    product_selection_attributed,
    other_payer_recognized,
    periodic_deductible_applied,
    periodic_benefit_exceed,
    accumulated_deductible,
    remaining_deductible,
    remaining_benefit,
    copay_coinsurance,
    SUBSTRING(
        CONCAT('0', CAST(basis_of_cost_determination AS int)),
        LENGTH(CONCAT('0', CAST(basis_of_cost_determination AS int))) - 1
        ) AS basis_of_cost_determination,
    submitted_ingredient_cost,
    submitted_dispensing_fee,
    submitted_incentive,
    submitted_gross_due,
    submitted_professional_service_fee,
    submitted_patient_pay,
    submitted_other_claimed_qual,
    submitted_other_claimed,
    CAST(
        CAST(basis_of_reimbursement_determination AS int) AS string
        ) AS basis_of_reimbursement_determination,
    paid_ingredient_cost,
    paid_dispensing_fee,
    paid_incentive,
    paid_gross_due,
    paid_professional_service_fee,
    paid_patient_pay,
    paid_other_claimed_qual,
    paid_other_claimed,
    tax_exempt_indicator,
    coupon_type,
    coupon_number,
    coupon_value,
    pharmacy_other_id,
    pharmacy_other_qual,
    pharmacy_postal_code,
    prov_dispensing_id,
    prov_dispensing_qual,
    prov_prescribing_id,
    prov_prescribing_qual,
    prov_primary_care_id,
    prov_primary_care_qual,
    other_payer_coverage_type,
    other_payer_coverage_id,
    other_payer_coverage_qual,
    other_payer_date,
    other_payer_coverage_code,
    logical_delete_reason,
    part_provider,
    CASE WHEN part_best_date NOT IN ('NULL', '0_PREDATES_HVM_HISTORY')
    THEN CONCAT(part_best_date, '-01')
    ELSE '0_PREDATES_HVM_HISTORY'
    END AS part_processdate
FROM default.pharmacyclaims_20180205
WHERE part_provider IN ('apothecary_by_design', 'genoa', 'mckesson_macro_helix', 'mckesson', 'mckesson_res', 'cardinal_pds', 'pdx')
UNION ALL
    SELECT record_id,
    claim_id,
    hvid,
    created,
    model_version,
    data_set,
    data_feed,
    data_vendor,
    source_version,
    patient_gender,
    patient_age,
    patient_year_of_birth,
    patient_zip3,
    patient_state,
    date_service,
    date_written,
    year_of_injury,
    date_authorized,
    time_authorized,
    discharge_date,
    transaction_code_std,
    transaction_code_vendor,
    response_code_std,
    response_code_vendor,
    reject_reason_code_1,
    reject_reason_code_2,
    reject_reason_code_3,
    reject_reason_code_4,
    reject_reason_code_5,
    diagnosis_code,
    diagnosis_code_qual,
    procedure_code,
    procedure_code_qual,
    CASE
      WHEN LENGTH(REGEXP_REPLACE(ndc_code, '[^0-9]', '')) >= 8
      THEN REGEXP_REPLACE(ndc_code, '[^0-9]', '')
    END AS ndc_code,
    product_service_id,
    product_service_id_qual,
    rx_number,
    rx_number_qual,
    bin_number,
    processor_control_number,
    fill_number,
    refill_auth_amount,
    CAST(dispensed_quantity as float),
    unit_of_measure,
    days_supply,
    pharmacy_npi,
    prov_dispensing_npi,
    payer_id,
    payer_id_qual,
    payer_name,
    payer_parent_name,
    payer_org_name,
    payer_plan_id,
    payer_plan_name,
    payer_type,
    compound_code,
    unit_dose_indicator,
    dispensed_as_written,
    prescription_origin,
    submission_clarification,
    orig_prescribed_product_service_code,
    orig_prescribed_product_service_code_qual,
    orig_prescribed_quantity,
    prior_auth_type_code,
    level_of_service,
    reason_for_service,
    professional_service_code,
    result_of_service_code,
    prov_prescribing_npi,
    prov_prescribing_tax_id,
    prov_prescribing_dea_id,
    prov_prescribing_ssn,
    prov_prescribing_state_license,
    prov_prescribing_upin,
    prov_prescribing_commercial_id,
    prov_prescribing_name_1,
    prov_prescribing_name_2,
    prov_prescribing_address_1,
    prov_prescribing_address_2,
    prov_prescribing_city,
    prov_prescribing_state,
    prov_prescribing_zip,
    prov_prescribing_std_taxonomy,
    prov_prescribing_vendor_specialty,
    prov_primary_care_npi,
    cob_count,
    usual_and_customary_charge,
    product_selection_attributed,
    other_payer_recognized,
    periodic_deductible_applied,
    periodic_benefit_exceed,
    accumulated_deductible,
    remaining_deductible,
    remaining_benefit,
    copay_coinsurance,
    SUBSTRING(
        CONCAT('0', CAST(basis_of_cost_determination AS int)),
        LENGTH(CONCAT('0', CAST(basis_of_cost_determination AS int))) - 1
        ) AS basis_of_cost_determination,
    submitted_ingredient_cost,
    submitted_dispensing_fee,
    submitted_incentive,
    submitted_gross_due,
    submitted_professional_service_fee,
    submitted_patient_pay,
    submitted_other_claimed_qual,
    submitted_other_claimed,
    CAST(
        CAST(basis_of_reimbursement_determination AS int) AS string
        ) AS basis_of_reimbursement_determination,
    paid_ingredient_cost,
    paid_dispensing_fee,
    paid_incentive,
    paid_gross_due,
    paid_professional_service_fee,
    paid_patient_pay,
    paid_other_claimed_qual,
    paid_other_claimed,
    tax_exempt_indicator,
    coupon_type,
    coupon_number,
    coupon_value,
    pharmacy_other_id,
    pharmacy_other_qual,
    pharmacy_postal_code,
    prov_dispensing_id,
    prov_dispensing_qual,
    prov_prescribing_id,
    prov_prescribing_qual,
    prov_primary_care_id,
    prov_primary_care_qual,
    other_payer_coverage_type,
    other_payer_coverage_id,
    other_payer_coverage_qual,
    other_payer_date,
    other_payer_coverage_code,
    logical_delete_reason,
    part_provider,
    CASE WHEN part_best_date NOT IN ('NULL', '0_PREDATES_HVM_HISTORY')
    THEN CONCAT(part_best_date, '-01')
    ELSE '0_PREDATES_HVM_HISTORY'
    END AS part_processdate
FROM default.pharmacyclaims_20170602
WHERE part_provider IN ('diplomat', 'cardinal_vitalpath')
UNION ALL
SELECT CAST(record_id AS bigint),
    claim_id,
    CASE WHEN TRIM(hvid) = '' THEN NULL ELSE hvid END,
    created,
    model_version,
    data_set,
    CASE WHEN data_feed = 'accredo pharmacy claims' THEN '16'
         WHEN data_feed = 'express scripts pharmacy claims' THEN '16'
         WHEN data_feed = 'webmd pharmacy claims' THEN '11'
         ELSE data_feed END AS data_feed,
    CASE WHEN data_vendor = 'accredo' THEN '17'
         WHEN data_vendor = 'express scripts' THEN '17'
         WHEN data_vendor = 'webmd' THEN '11'
         ELSE data_vendor END AS data_vendor,
    source_version,
    patient_gender,
    patient_age,
    patient_year_of_birth,
    patient_zip3,
    patient_state,
    CAST(date_service AS date),
    CAST(date_written AS date),
    SUBSTRING(date_injury, 0, 4),
    CAST(date_authorized AS date),
    time_authorized,
    NULL AS discharge_date,
    transaction_code_std,
    transaction_code_vendor,
    response_code_std,
    response_code_vendor,
    reject_reason_code_1,
    reject_reason_code_2,
    reject_reason_code_3,
    reject_reason_code_4,
    reject_reason_code_5,
    diagnosis_code,
    diagnosis_code_qual,
    procedure_code,
    procedure_code_qual,
    CASE
      WHEN LENGTH(REGEXP_REPLACE(ndc_code, '[^0-9]', '')) >= 8
      THEN REGEXP_REPLACE(ndc_code, '[^0-9]', '')
    END AS ndc_code,
    product_service_id,
    product_service_id_qual,
    rx_number,
    rx_number_qual,
    bin_number,
    processor_control_number,
    CAST(fill_number AS int),
    refill_auth_amount,
    CAST(dispensed_quantity AS float),
    unit_of_measure,
    CAST(days_supply AS int),
    CASE WHEN TRIM(pharmacy_npi) = '' THEN NULL ELSE pharmacy_npi END,
    CASE WHEN TRIM(prov_dispensing_npi) = '' THEN NULL ELSE prov_dispensing_npi END,
    payer_id,
    payer_id_qual,
    payer_name,
    payer_parent_name,
    payer_org_name,
    payer_plan_id,
    payer_plan_name,
    payer_type,
    compound_code,
    unit_dose_indicator,
    dispensed_as_written,
    prescription_origin,
    submission_clarification,
    orig_prescribed_product_service_code,
    orig_prescribed_product_service_code_qual,
    orig_prescribed_quantity,
    prior_auth_type_code,
    level_of_service,
    reason_for_service,
    professional_service_code,
    result_of_service_code,
    CASE WHEN TRIM(prov_prescribing_npi) = '' THEN NULL ELSE prov_prescribing_npi END,
    NULL AS prov_prescribing_tax_id,
    NULL AS prov_prescribing_dea_id,
    NULL AS prov_prescribing_ssn,
    NULL AS prov_prescribing_state_license,
    NULL AS prov_prescribing_upin,
    NULL AS prov_prescribing_commercial_id,
    NULL AS prov_prescribing_name_1,
    NULL AS prov_prescribing_name_2,
    NULL AS prov_prescribing_address_1,
    NULL AS prov_prescribing_address_2,
    NULL AS prov_prescribing_city,
    NULL AS prov_prescribing_state,
    NULL AS prov_prescribing_zip,
    NULL AS prov_prescribing_std_taxonomy,
    NULL AS prov_prescribing_vendor_specialty,
    CASE WHEN TRIM(prov_primary_care_npi) = '' THEN NULL ELSE prov_primary_care_npi END,
    cob_count,
    CAST(usual_and_customary_charge AS double),
    CAST(product_selection_attributed AS double),
    CAST(other_payer_recognized AS double),
    CAST(periodic_deductible_applied AS double),
    CAST(periodic_benefit_exceed AS double),
    CAST(accumulated_deductible AS double),
    CAST(remaining_deductible AS double),
    CAST(remaining_benefit AS double),
    CAST(copay_coinsurance AS double),
    SUBSTRING(
        CONCAT('0', CAST(basis_of_cost_determination AS int)),
        LENGTH(CONCAT('0', CAST(basis_of_cost_determination AS int))) - 1
        ) AS basis_of_cost_determination,
    CAST(submitted_ingredient_cost AS double),
    CAST(submitted_dispensing_fee AS double),
    CAST(submitted_incentive AS double),
    CAST(submitted_gross_due AS double),
    CAST(submitted_professional_service_fee AS double),
    CAST(submitted_patient_pay AS double),
    CAST(submitted_other_claimed_qual AS double),
    CAST(submitted_other_claimed AS double),
    basis_of_reimbursement_determination,
    CAST(paid_ingredient_cost AS double),
    CAST(paid_dispensing_fee AS double),
    CAST(paid_incentive AS double),
    CAST(paid_gross_due AS double),
    CAST(paid_professional_service_fee AS double),
    CAST(paid_patient_pay AS double),
    CAST(paid_other_claimed_qual AS double),
    CAST(paid_other_claimed AS double),
    tax_exempt_indicator,
    coupon_type,
    coupon_number,
    CAST(coupon_value AS double),
    pharmacy_other_id,
    pharmacy_other_qual,
    pharmacy_postal_code,
    prov_dispensing_id,
    prov_dispensing_qual,
    prov_prescribing_id,
    prov_prescribing_qual,
    prov_primary_care_id,
    prov_primary_care_qual,
    other_payer_coverage_type,
    other_payer_coverage_id,
    other_payer_coverage_qual,
    CAST(other_payer_date AS date),
    other_payer_coverage_code,
    CASE WHEN TRIM(logical_delete_reason) = '' THEN NULL ELSE logical_delete_reason END,
    CASE
    WHEN part_provider = 'accredo' THEN 'express_scripts'
    ELSE part_provider
    END AS part_provider,
    CASE WHEN part_processdate IN ('NULL', '0_PREDATES_HVM_HISTORY')
      THEN '0_PREDATES_HVM_HISTORY'
      WHEN LENGTH(part_processdate) = 4
      THEN CONCAT(part_processdate, '-01-01')
      ELSE REGEXP_REPLACE(part_processdate, '/', '-')
    END AS part_processdate
FROM default.pharmacyclaims_old
WHERE part_provider IN ('express_scripts', 'accredo')
;
