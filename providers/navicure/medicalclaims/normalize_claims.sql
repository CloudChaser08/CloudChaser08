DROP TABLE IF EXISTS tmp1;
CREATE TABLE tmp1 AS
SELECT *,
CASE WHEN icd9_code_1 IS NOT NULL AND icd9_code_1 NOT IN ('', ' ') THEN (COALESCE(icd9_code_1, '') || ':' || COALESCE(icd9_code_2, '') || ':' || COALESCE(icd9_code_3, '') || ':' || COALESCE(icd9_code_4, '') || ':' || COALESCE(icd9_code_5, '') || ':' || COALESCE(icd9_code_6, '') || ':' || COALESCE(icd9_code_7, '') || ':' || COALESCE(icd9_code_8, '') || ':' || COALESCE(icd9_code_9, '') || ':' || COALESCE(icd9_code_10, '') || ':' || COALESCE(icd9_code_11, '') || ':' || COALESCE(icd9_code_12, '')) 
     WHEN icd10_code_1 IS NOT NULL AND icd10_code_1 NOT IN ('', ' ') THEN (COALESCE(icd10_code_1, '') || ':' || COALESCE(icd10_code_2, '') || ':' || COALESCE(icd10_code_3, '') || ':' || COALESCE(icd10_code_4, '') || ':' || COALESCE(icd10_code_5, '') || ':' || COALESCE(icd10_code_6, '') || ':' || COALESCE(icd10_code_7, '') || ':' || COALESCE(icd10_code_8, '') || ':' || COALESCE(icd10_code_9, '') || ':' || COALESCE(icd10_code_10, '') || ':' || COALESCE(icd10_code_11, '') || ':' || COALESCE(icd10_code_12, ''))
     ELSE NULL END as diag_concat,
(COALESCE(diagnosis_code_1, '') || ':' || COALESCE(diagnosis_code_2, '') || ':' || COALESCE(diagnosis_code_3, '') || ':' || COALESCE(diagnosis_code_4, '')) as priority_concat
FROM navicure_raw;

DROP TABLE IF EXISTS tmp2;
CREATE TABLE tmp2 AS
SELECT *, get_diagnosis_with_priority(diag_concat, priority_concat) as diag_with_priority
FROM tmp1;

UPDATE tmp2
SET claim_submit_date = dates.formatted
FROM dates
WHERE claim_submit_date = dates.date;

UPDATE tmp2 SET
claim_submit_date = NULL
WHERE length(claim_submit_date) <> 10;

UPDATE tmp2
SET service_from_date = dates.formatted
FROM dates
WHERE service_from_date = dates.date;

UPDATE tmp2 SET
service_from_date = NULL
WHERE length(service_from_date) <> 10;

UPDATE tmp2
SET service_to_date = dates.formatted
FROM dates
WHERE service_to_date = dates.date;

UPDATE tmp2 SET
service_to_date = NULL
WHERE length(service_to_date) <> 10;

INSERT INTO medicalclaims_common_model (
    claim_id,
    hvid,
    patient_gender,
    patient_year_of_birth,
    patient_zip3,
    patient_state,
    claim_type,
    date_received, -- date
    date_service, -- date
    date_service_end, -- date
    place_of_service_std_id,
    service_line_number,
    diagnosis_code,
    diagnosis_code_qual,
    diagnosis_priority,
    procedure_code,
    procedure_code_qual,
    procedure_units,
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
    payer_vendor_id,
    payer_name,
    payer_type,
    prov_rendering_state_license,
    prov_rendering_upin,
    prov_rendering_commercial_id,
    prov_rendering_name_2,
    prov_rendering_std_taxonomy,
    prov_billing_tax_id,
    prov_billing_ssn,
    prov_billing_commercial_id,
    prov_billing_name_1,
    prov_billing_name_2,
    prov_billing_address_1,
    prov_billing_address_2,
    prov_billing_city,
    prov_billing_state,
    prov_billing_zip,
    prov_billing_std_taxonomy,
    prov_referring_state_license,
    prov_referring_upin,
    prov_referring_commercial_id,
    prov_referring_name_2,
    prov_facility_name_1,
    prov_facility_address_1,
    prov_facility_address_2,
    prov_facility_city,
    prov_facility_state,
    prov_facility_zip,
    cob_payer_vendor_id_1,
    cob_payer_claim_filing_ind_code_1,
    cob_ins_type_code_1,
    cob_payer_vendor_id_2,
    cob_payer_claim_filing_ind_code_2,
    cob_ins_type_code_2
)
SELECT
    COALESCE(navicure_client_id, '') || COALESCE(unique_claim_id, '') || COALESCE(claim_revision_no, ''),
    payload.hvid,
    payload.gender,
    payload.yearofbirth,
    payload.threedigitzip,
    state.state,
    'P',
    claim_submit_date,
    (service_from_date::date + date_explode_indices.d)::text,
    (service_from_date::date + date_explode_indices.d)::text,
    place_of_service_code,
    service_line,
    UPPER(REPLACE(REPLACE(REPLACE(split_part(split_part(diag_with_priority,':',n),'_',1),',',''),'.',''),' ', '')),
    CASE WHEN icd9_code_1 IS NOT NULL THEN '01' WHEN icd10_code_1 IS NOT NULL THEN '02' ELSE NULL END,
    split_part(split_part(diag_with_priority,':',n),'_',2),
    UPPER(product_service_id),
    'HC',
    CASE WHEN service_unit_count IS NOT NULL THEN service_unit_count ELSE service_ane_minutes END,
    procedure_modifier_1,
    procedure_modifier_2,
    procedure_modifier_3,
    procedure_modifier_4,
    ndc_drug_code,
    dest_claim_filing_indicator,
    line_item_charge_amount,
    total_claim_charge_amount,
    rendering_provider_npi,
    billing_npi,
    referring_provider_1_npi,
    facility_npi,
    CASE WHEN dest_system_payer_entity IS NOT NULL THEN dest_system_payer_entity ELSE dest_client_payer_id END,
    CASE WHEN dest_system_payer_name IS NOT NULL THEN dest_system_payer_name ELSE dest_client_payer_name END,
    dest_insurance_type_code,
    rendering_state_license_no,
    rendering_upin,
    rendering_commercial,
    COALESCE(rendering_last_name, '') || ', ' || COALESCE(rendering_first_name, '') || ', ' || COALESCE(rendering_middle_name, ''),
    rendering_taxonomy_code,
    CASE WHEN billing_tax_id_qualifier = 'EI' THEN billing_tax_id ELSE NULL END,
    CASE WHEN billing_tax_id_qualifier = 'SY' THEN billing_tax_id ELSE NULL END,
    billing_commercial,
    billing_organization_name,
    COALESCE(billing_last_name, '') || ', ' || COALESCE(billing_first_name, '') || ', ' || COALESCE(billing_middle_name, ''),
    billing_street_address_line_1,
    billing_street_address_line_2,
    billing_street_address_city,
    billing_street_address_state,
    billing_street_address_zip,
    billing_taxonomy_code,
    referring_1_state_license_no,
    referring_1_upin,
    referring_1_commercial_id,
    COALESCE(referring_1_last_name, '') || ', ' || COALESCE(referring_1_first_name, '') || ', ' || COALESCE(referring_1_middle_name, ''),
    facility_organization_name,
    facility_street_address_line_1,
    facility_street_address_line_2,
    facility_street_address_city,
    facility_street_address_state,
    facility_street_address_zip,
    box9_client_payer_id,
    box9_claim_filing_indicator,
    box9_insurance_type_code,
    third_client_payer_id,
    third_claim_filing_indicator,
    third_insurance_type_code
FROM
    tmp2
    CROSS JOIN split_indices
    CROSS JOIN date_explode_indices
    LEFT JOIN matching_payload payload USING (hvjoinkey)
    LEFT JOIN zip3_to_state state ON state.zip3 = payload.threeDigitZip
WHERE split_part(diag_with_priority,':',n) IS NOT NULL
    AND split_part(diag_with_priority,':',n) != ''
    AND split_part(diag_with_priority,':',n) not ilike 'c127%'
    AND (service_to_date::date - service_from_date::date) IS NOT NULL
    AND (service_to_date::date - service_from_date::date) > 0
    AND service_from_date::date + '366 day'::interval > service_to_date::date
    AND service_from_date::date + date_explode_indices.d <= service_to_date::date;

INSERT INTO medicalclaims_common_model (
    claim_id,
    hvid,
    patient_gender,
    patient_year_of_birth,
    patient_zip3,
    patient_state,
    claim_type,
    date_received, -- date
    date_service, -- date
    date_service_end, -- date
    place_of_service_std_id,
    service_line_number,
    diagnosis_code,
    diagnosis_code_qual,
    diagnosis_priority,
    procedure_code,
    procedure_code_qual,
    procedure_units,
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
    payer_vendor_id,
    payer_name,
    payer_type,
    prov_rendering_state_license,
    prov_rendering_upin,
    prov_rendering_commercial_id,
    prov_rendering_name_2,
    prov_rendering_std_taxonomy,
    prov_billing_tax_id,
    prov_billing_ssn,
    prov_billing_commercial_id,
    prov_billing_name_1,
    prov_billing_name_2,
    prov_billing_address_1,
    prov_billing_address_2,
    prov_billing_city,
    prov_billing_state,
    prov_billing_zip,
    prov_billing_std_taxonomy,
    prov_referring_state_license,
    prov_referring_upin,
    prov_referring_commercial_id,
    prov_referring_name_2,
    prov_facility_name_1,
    prov_facility_address_1,
    prov_facility_address_2,
    prov_facility_city,
    prov_facility_state,
    prov_facility_zip,
    cob_payer_vendor_id_1,
    cob_payer_claim_filing_ind_code_1,
    cob_ins_type_code_1,
    cob_payer_vendor_id_2,
    cob_payer_claim_filing_ind_code_2,
    cob_ins_type_code_2
)
SELECT
    COALESCE(navicure_client_id, '') || COALESCE(unique_claim_id, '') || COALESCE(claim_revision_no, ''),
    payload.hvid,
    payload.gender,
    payload.yearofbirth,
    payload.threedigitzip,
    state.state,
    'P',
    claim_submit_date,
    service_from_date,
    service_to_date,
    place_of_service_code,
    service_line,
    UPPER(REPLACE(REPLACE(REPLACE(split_part(split_part(diag_with_priority,':',n),'_',1),',',''),'.',''),' ', '')),
    CASE WHEN icd9_code_1 IS NOT NULL THEN '01' WHEN icd10_code_1 IS NOT NULL THEN '02' ELSE NULL END,
    split_part(split_part(diag_with_priority,':',n),'_',2),
    UPPER(product_service_id),
    'HC',
    CASE WHEN service_unit_count IS NOT NULL THEN service_unit_count ELSE service_ane_minutes END,
    procedure_modifier_1,
    procedure_modifier_2,
    procedure_modifier_3,
    procedure_modifier_4,
    ndc_drug_code,
    dest_claim_filing_indicator,
    line_item_charge_amount,
    total_claim_charge_amount,
    rendering_provider_npi,
    billing_npi,
    referring_provider_1_npi,
    facility_npi,
    CASE WHEN dest_system_payer_entity IS NOT NULL THEN dest_system_payer_entity ELSE dest_client_payer_id END,
    CASE WHEN dest_system_payer_name IS NOT NULL THEN dest_system_payer_name ELSE dest_client_payer_name END,
    dest_insurance_type_code,
    rendering_state_license_no,
    rendering_upin,
    rendering_commercial,
    COALESCE(rendering_last_name, '') || ', ' || COALESCE(rendering_first_name, '') || ', ' || COALESCE(rendering_middle_name, ''),
    rendering_taxonomy_code,
    CASE WHEN billing_tax_id_qualifier = 'EI' THEN billing_tax_id ELSE NULL END,
    CASE WHEN billing_tax_id_qualifier = 'SY' THEN billing_tax_id ELSE NULL END,
    billing_commercial,
    billing_organization_name,
    COALESCE(billing_last_name, '') || ', ' || COALESCE(billing_first_name, '') || ', ' || COALESCE(billing_middle_name, ''),
    billing_street_address_line_1,
    billing_street_address_line_2,
    billing_street_address_city,
    billing_street_address_state,
    billing_street_address_zip,
    billing_taxonomy_code,
    referring_1_state_license_no,
    referring_1_upin,
    referring_1_commercial_id,
    COALESCE(referring_1_last_name, '') || ', ' || COALESCE(referring_1_first_name, '') || ', ' || COALESCE(referring_1_middle_name, ''),
    facility_organization_name,
    facility_street_address_line_1,
    facility_street_address_line_2,
    facility_street_address_city,
    facility_street_address_state,
    facility_street_address_zip,
    box9_client_payer_id,
    box9_claim_filing_indicator,
    box9_insurance_type_code,
    third_client_payer_id,
    third_claim_filing_indicator,
    third_insurance_type_code
FROM
    tmp2
    CROSS JOIN split_indices
    LEFT JOIN matching_payload payload USING (hvjoinkey)
    LEFT JOIN zip3_to_state state ON state.zip3 = payload.threeDigitZip
WHERE split_part(diag_with_priority,':',n) IS NOT NULL
    AND split_part(diag_with_priority,':',n) != ''
    AND split_part(diag_with_priority,':',n) not ilike 'c127%'
    AND ((service_to_date::date - service_from_date::date) IS NULL
        OR (service_to_date::date - service_from_date::date) <= 0
        OR service_from_date::date + '366 day'::interval <= service_to_date::date);

-- Fix blank first/middle names
UPDATE medicalclaims_common_model
SET prov_referring_name_2 = 
    CASE WHEN prov_referring_name_2 = ', , ' THEN NULL
    WHEN prov_referring_name_2 LIKE '%, , ' THEN regexp_replace(prov_referring_name_2, ', , $', '')
    WHEN prov_referring_name_2 LIKE '%, ' THEN regexp_replace(prov_referring_name_2, ', $', '')
    ELSE prov_referring_name_2 END
WHERE prov_referring_name_2 = ', , ' OR prov_referring_name_2 LIKE '%, , ' OR prov_referring_name_2 LIKE '%, ';

-- Fix blank first/middle names
UPDATE medicalclaims_common_model
SET prov_rendering_name_2 =
    CASE WHEN prov_rendering_name_2 = ', , ' THEN NULL
    WHEN prov_rendering_name_2 LIKE '%, , ' THEN regexp_replace(prov_rendering_name_2, ', , $', '')
    WHEN prov_rendering_name_2 LIKE '%, ' THEN regexp_replace(prov_rendering_name_2, ', $', '')
    ELSE prov_rendering_name_2 END
WHERE prov_rendering_name_2 = ', , ' OR prov_rendering_name_2 LIKE '%, , ' OR prov_rendering_name_2 LIKE '%, ';

-- Fix blank first/middle names
UPDATE medicalclaims_common_model
SET prov_billing_name_2 =
    CASE WHEN prov_billing_name_2 = ', , ' THEN NULL
    WHEN prov_billing_name_2 LIKE '%, , ' THEN regexp_replace(prov_billing_name_2, ', , $', '')
    WHEN prov_billing_name_2 LIKE '%, ' THEN regexp_replace(prov_billing_name_2, ', $', '')
    ELSE prov_billing_name_2 END
WHERE prov_billing_name_2 = ', , ' OR prov_billing_name_2 LIKE '%, , ' OR prov_billing_name_2 LIKE '%, ';

UPDATE medicalclaims_common_model SET patient_year_of_birth=NULL
WHERE
-- 32873 is roughly 90 years, Redshift doesn't support year intervals
date_service IS NULL OR (patient_year_of_birth < (extract('year' from date_service::date - '32873 days'::interval)::text)) OR patient_year_of_birth > (extract('year' from getdate())::text);
