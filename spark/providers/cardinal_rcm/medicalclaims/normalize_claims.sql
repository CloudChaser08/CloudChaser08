INSERT INTO medicalclaims_common_model
SELECT DISTINCT
    NULL,                   -- record_id
    t.claim_id,             -- claim_id
    (
    SELECT MIN(mp.hvid)
    FROM transactions t2
        INNER JOIN matching_payload mp ON t2.hvjoinkey = mp.hvjoinkey
    WHERE t2.line_seq_no = '1'
        AND t2.claim_id = t.claim_id
        ),                  -- hvid
    NULL,                   -- created
    2,                      -- model_version
    NULL,                   -- data_set
    NULL,                   -- data_feed
    NULL,                   -- data_vendor
    NULL,                   -- source_version
    (
    SELECT MIN(mp.gender)
    FROM transactions t2
        INNER JOIN matching_payload mp ON t2.hvjoinkey = mp.hvjoinkey
    WHERE t2.line_seq_no = '1'
        AND t2.claim_id = t.claim_id
        ),                  -- patient_gender
    (
    SELECT MIN(mp.age)
    FROM transactions t2
        INNER JOIN matching_payload mp ON t2.hvjoinkey = mp.hvjoinkey
    WHERE t2.line_seq_no = '1'
        AND t2.claim_id = t.claim_id
        ),                  -- patient_age
    (
    SELECT MIN(mp.yearOfBirth)
    FROM transactions t2
        INNER JOIN matching_payload mp ON t2.hvjoinkey = mp.hvjoinkey
    WHERE t2.line_seq_no = '1'
        AND t2.claim_id = t.claim_id
        ),                  -- patient_year_of_birth
    (
    SELECT MIN(mp.threeDigitZip)
    FROM transactions t2
        INNER JOIN matching_payload mp ON t2.hvjoinkey = mp.hvjoinkey
    WHERE t2.line_seq_no = '1'
        AND t2.claim_id = t.claim_id
        ),                  -- patient_zip3
    (
    SELECT MIN(UPPER(mp.state))
    FROM transactions t2
        INNER JOIN matching_payload mp ON t2.hvjoinkey = mp.hvjoinkey
    WHERE t2.line_seq_no = '1'
        AND t2.claim_id = t.claim_id
        ),                  -- patient_state
    'P',                    -- claim_type
    NULL,                   -- date_received
    (SELECT MIN(EXTRACT_DATE(
                t2.date_svc_start, '%Y-%m-%d'
                ))
    FROM transactions t2
    WHERE t.claim_id = t2.claim_id
        ),                  -- date_service
    (SELECT MAX(EXTRACT_DATE(
                t2.date_svc_end, '%Y-%m-%d'
                ))
    FROM transactions t2
    WHERE t.claim_id = t2.claim_id
        ),                  -- date_service_end
    NULL,                   -- inst_date_admitted
    NULL,                   -- inst_date_discharged
    NULL,                   -- inst_admit_type_std_id
    NULL,                   -- inst_admit_type_vendor_id
    NULL,                   -- inst_admit_type_vendor_desc
    NULL,                   -- inst_admit_source_std_id
    NULL,                   -- inst_admit_source_vendor_id
    NULL,                   -- inst_admit_source_vendor_desc
    NULL,                   -- inst_discharge_status_std_id
    NULL,                   -- inst_discharge_status_vendor_id
    NULL,                   -- inst_discharge_status_vendor_desc
    NULL,                   -- inst_type_of_bill_std_id
    NULL,                   -- inst_type_of_bill_vendor_id
    NULL,                   -- inst_type_of_bill_vendor_desc
    NULL,                   -- inst_drg_std_id
    NULL,                   -- inst_drg_vendor_id
    NULL,                   -- inst_drg_vendor_desc
    (
    SELECT MIN(t2.facility_code)
    FROM transactions t2
    WHERE t2.line_seq_no = '1'
        AND t2.claim_id = t.claim_id
        ),                  -- place_of_service_std_id
    NULL,                   -- place_of_service_vendor_id
    NULL,                   -- place_of_service_vendor_desc
    NULL,                   -- service_line_number
    ARRAY(
        t.diag_principal, t.diag_2, t.diag_3, t.diag_4,
        t.diag_5, t.diag_6, t.diag_7, t.diag_8
        )[diag_explode.n],  -- diagnosis_code
    NULL,                   -- diagnosis_code_qual
    NULL,                   -- diagnosis_priority
    NULL,                   -- admit_diagnosis_ind
    NULL,                   -- procedure_code
    NULL,                   -- procedure_code_qual
    NULL,                   -- principal_proc_ind
    NULL,                   -- procedure_units
    NULL,                   -- procedure_modifier_1
    NULL,                   -- procedure_modifier_2
    NULL,                   -- procedure_modifier_3
    NULL,                   -- procedure_modifier_4
    NULL,                   -- revenue_code
    NULL,                   -- ndc_code
    NULL,                   -- medical_coverage_type
    NULL,                   -- line_charge
    NULL,                   -- line_allowed
    (
    SELECT MIN(t2.submitted_chg_total)
    FROM transactions t2
    WHERE t2.line_seq_no = '1'
        AND t2.claim_id = t.claim_id
        ),                  -- total_charge
    NULL,                   -- total_allowed
    NULL,                   -- prov_rendering_npi
    (
    SELECT MIN(
            CASE
            WHEN t2.bill_prov_id_qual = 'XX'
            THEN COALESCE(t2.bill_prov_npid, t2.bill_prov_id)
            ELSE t2.bill_prov_npid
            END
            )
    FROM transactions t2
    WHERE t2.line_seq_no = '1'
        AND t2.claim_id = t.claim_id
        ),                  -- prov_billing_npi
    NULL,                   -- prov_referring_npi
    NULL,                   -- prov_facility_npi
    (
    SELECT MIN(t2.payer_id)
    FROM transactions t2
    WHERE t2.line_seq_no = '1'
        AND t2.claim_id = t.claim_id
        ),                  -- payer_vendor_id
    (
    SELECT MIN(t2.payer_name)
    FROM transactions t2
    WHERE t2.line_seq_no = '1'
        AND t2.claim_id = t.claim_id
        ),                  -- payer_name
    NULL,                   -- payer_parent_name
    NULL,                   -- payer_org_name
    NULL,                   -- payer_plan_id
    NULL,                   -- payer_plan_name
    NULL,                   -- payer_type
    NULL,                   -- prov_rendering_vendor_id
    NULL,                   -- prov_rendering_tax_id
    NULL,                   -- prov_rendering_dea_id
    NULL,                   -- prov_rendering_ssn
    NULL,                   -- prov_rendering_state_license
    NULL,                   -- prov_rendering_upin
    NULL,                   -- prov_rendering_commercial_id
    NULL,                   -- prov_rendering_name_1
    NULL,                   -- prov_rendering_name_2
    NULL,                   -- prov_rendering_address_1
    NULL,                   -- prov_rendering_address_2
    NULL,                   -- prov_rendering_city
    NULL,                   -- prov_rendering_state
    NULL,                   -- prov_rendering_zip
    NULL,                   -- prov_rendering_std_taxonomy
    NULL,                   -- prov_rendering_vendor_specialty
    (
    SELECT MIN(
            CASE
            WHEN t2.bill_prov_id_qual <> 'XX'
            THEN t2.bill_prov_id
            END
            )
    FROM transactions t2
    WHERE t2.line_seq_no = '1'
        AND t2.claim_id = t.claim_id
        ),                  -- prov_billing_vendor_id
    NULL,                   -- prov_billing_tax_id
    NULL,                   -- prov_billing_dea_id
    NULL,                   -- prov_billing_ssn
    NULL,                   -- prov_billing_state_license
    NULL,                   -- prov_billing_upin
    NULL,                   -- prov_billing_commercial_id
    (
    SELECT MIN(t2.bill_prov_name)
    FROM transactions t2
    WHERE t2.line_seq_no = '1'
        AND t2.claim_id = t.claim_id
        ),                  -- prov_billing_name_1
    NULL,                   -- prov_billing_name_2
    NULL,                   -- prov_billing_address_1
    NULL,                   -- prov_billing_address_2
    NULL,                   -- prov_billing_city
    NULL,                   -- prov_billing_state
    NULL,                   -- prov_billing_zip
    (
    SELECT MIN(t2.bill_prov_taxonomy_code)
    FROM transactions t2
    WHERE t2.line_seq_no = '1'
        AND t2.claim_id = t.claim_id
        ),                  -- prov_billing_std_taxonomy
    NULL,                   -- prov_billing_vendor_specialty
    NULL,                   -- prov_referring_vendor_id
    NULL,                   -- prov_referring_tax_id
    NULL,                   -- prov_referring_dea_id
    NULL,                   -- prov_referring_ssn
    NULL,                   -- prov_referring_state_license
    NULL,                   -- prov_referring_upin
    NULL,                   -- prov_referring_commercial_id
    NULL,                   -- prov_referring_name_1
    NULL,                   -- prov_referring_name_2
    NULL,                   -- prov_referring_address_1
    NULL,                   -- prov_referring_address_2
    NULL,                   -- prov_referring_city
    NULL,                   -- prov_referring_state
    NULL,                   -- prov_referring_zip
    NULL,                   -- prov_referring_std_taxonomy
    NULL,                   -- prov_referring_vendor_specialty
    NULL,                   -- prov_facility_vendor_id
    NULL,                   -- prov_facility_tax_id
    NULL,                   -- prov_facility_dea_id
    NULL,                   -- prov_facility_ssn
    NULL,                   -- prov_facility_state_license
    NULL,                   -- prov_facility_upin
    NULL,                   -- prov_facility_commercial_id
    NULL,                   -- prov_facility_name_1
    NULL,                   -- prov_facility_name_2
    NULL,                   -- prov_facility_address_1
    NULL,                   -- prov_facility_address_2
    NULL,                   -- prov_facility_city
    NULL,                   -- prov_facility_state
    NULL,                   -- prov_facility_zip
    NULL,                   -- prov_facility_std_taxonomy
    NULL,                   -- prov_facility_vendor_specialty
    NULL,                   -- cob_payer_vendor_id_1
    NULL,                   -- cob_payer_seq_code_1
    NULL,                   -- cob_payer_hpid_1
    NULL,                   -- cob_payer_claim_filing_ind_code_1
    NULL,                   -- cob_ins_type_code_1
    NULL,                   -- cob_payer_vendor_id_2
    NULL,                   -- cob_payer_seq_code_2
    NULL,                   -- cob_payer_hpid_2
    NULL,                   -- cob_payer_claim_filing_ind_code_2
    NULL                    -- cob_ins_type_code_2
FROM transactions t
    CROSS JOIN claim_diag_exploder diag_explode
WHERE

-- include rows for non-null diagnoses
    ARRAY(
        t.diag_principal, t.diag_2, t.diag_3, t.diag_4,
        t.diag_5, t.diag_6, t.diag_7, t.diag_8
        )[diag_explode.n] IS NOT NULL

-- only add rows where the diagnosis code does not exist already in
-- the table for this claim
    AND NOT EXISTS (
    SELECT record_id
    FROM medicalclaims_common_model svc
    WHERE svc.claim_id = t.claim_id
        AND svc.diagnosis_code = ARRAY(
            t.diag_principal, t.diag_2, t.diag_3, t.diag_4,
            t.diag_5, t.diag_6, t.diag_7, t.diag_8
            )[diag_explode.n]
        )
    ;
