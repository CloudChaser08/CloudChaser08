INSERT INTO medicalclaims_common_model
SELECT DISTINCT
    NULL,                        -- record_id
    t.claim_id,                  -- claim_id
    mp.hvid,                     -- hvid
    NULL,                        -- created
    2,                           -- model_version
    NULL,                        -- data_set
    NULL,                        -- data_feed
    NULL,                        -- data_vendor
    NULL,                        -- source_version
    mp.gender,                   -- patient_gender
    mp.age,                      -- patient_age
    mp.yearOfBirth,              -- patient_year_of_birth
    mp.threeDigitZip,            -- patient_zip3
    UPPER(mp.state),             -- patient_state
    'P',                         -- claim_type
    NULL,                        -- date_received
    EXTRACT_DATE(
        t.date_svc_start, '%Y-%m-%d'
        ),                       -- date_service
    EXTRACT_DATE(
        t.date_svc_end, '%Y-%m-%d'
        ),                       -- date_service_end
    NULL,                        -- inst_date_admitted
    NULL,                        -- inst_date_discharged
    NULL,                        -- inst_admit_type_std_id
    NULL,                        -- inst_admit_type_vendor_id
    NULL,                        -- inst_admit_type_vendor_desc
    NULL,                        -- inst_admit_source_std_id
    NULL,                        -- inst_admit_source_vendor_id
    NULL,                        -- inst_admit_source_vendor_desc
    NULL,                        -- inst_discharge_status_std_id
    NULL,                        -- inst_discharge_status_vendor_id
    NULL,                        -- inst_discharge_status_vendor_desc
    NULL,                        -- inst_type_of_bill_std_id
    NULL,                        -- inst_type_of_bill_vendor_id
    NULL,                        -- inst_type_of_bill_vendor_desc
    NULL,                        -- inst_drg_std_id
    NULL,                        -- inst_drg_vendor_id
    NULL,                        -- inst_drg_vendor_desc
    t.facility_code,             -- place_of_service_std_id
    NULL,                        -- place_of_service_vendor_id
    NULL,                        -- place_of_service_vendor_desc
    t.line_seq_no,               -- service_line_number
    ARRAY(
        t.linked_diag_1, t.linked_diag_2, t.linked_diag_3,
        t.linked_diag_4
        )[diag_explode.n],       -- diagnosis_code
    NULL,                        -- diagnosis_code_qual
    NULL,                        -- diagnosis_priority
    NULL,                        -- admit_diagnosis_ind
    t.proc_code,                 -- procedure_code
    CASE
        WHEN t.proc_code IS NOT NULl
        THEN t.proc_code_qual
    END,                         -- procedure_code_qual
    NULL,                        -- principal_proc_ind
    CASE
        WHEN t.proc_code IS NOT NULL
        THEN t.submitted_units
    END,                         -- procedure_units
    CASE
        WHEN t.proc_code IS NOT NULL
        THEN t.mod_1
    END,                         -- procedure_modifier_1
    CASE
        WHEN t.proc_code IS NOT NULL
        THEN t.mod_2
    END,                         -- procedure_modifier_2
    CASE
        WHEN t.proc_code IS NOT NULL
        THEN t.mod_3
    END,                         -- procedure_modifier_3
    CASE
        WHEN t.proc_code IS NOT NULL
        THEN t.mod_4
    END,                         -- procedure_modifier_4
    NULL,                        -- revenue_code
    NULL,                        -- ndc_code
    NULL,                        -- medical_coverage_type
    t.submitted_chg,             -- line_charge
    NULL,                        -- line_allowed
    t.submitted_chg_total,       -- total_charge
    NULL,                        -- total_allowed
    CASE
        WHEN (0 <> LENGTH(COALESCE(t.rend_prov_npid, '')))
        OR  (t.rend_prov_id_qual = 'XX' AND 0 <> LENGTH(COALESCE(t.rend_prov_id, '')))
        THEN (
            CASE
                WHEN t.rend_prov_id_qual = 'XX'
                THEN COALESCE(t.rend_prov_npid, t.rend_prov_id)
                ELSE t.rend_prov_npid
            END
            )
        WHEN (0 <> LENGTH(COALESCE(t.ord_prov_npid, '')))
        OR (t.ord_prov_id_qual = 'XX' AND 0 <> LENGTH(COALESCE(t.ord_prov_id, '')))
        THEN (
            CASE
                WHEN t.ord_prov_id_qual = 'XX'
                THEN COALESCE(t.ord_prov_npid, t.ord_prov_id)
                ELSE t.ord_prov_npid
            END
            )
    END,                         -- prov_rendering_npi
    CASE
        WHEN t.bill_prov_id_qual = 'XX'
        THEN COALESCE(t.bill_prov_npid, t.bill_prov_id)
        ELSE t.bill_prov_npid
    END,                         -- prov_billing_npi
    CASE
        WHEN t.refer_prov_id_qual = 'XX'
        THEN COALESCE(t.refer_prov_npid, t.refer_prov_id)
        ELSE t.refer_prov_npid
    END,                         -- prov_referring_npi
    CASE
        WHEN t.serv_facility_id_qual = 'XX'
        THEN t.serv_facility_id
    END,                         -- prov_facility_npi
    t.payer_id,                  -- payer_vendor_id
    t.payer_name,                -- payer_name
    NULL,                        -- payer_parent_name
    NULL,                        -- payer_org_name
    NULL,                        -- payer_plan_id
    NULL,                        -- payer_plan_name
    NULL,                        -- payer_type
    CASE
        WHEN (0 <> LENGTH(COALESCE(t.rend_prov_npid, '')))
        OR (t.rend_prov_id_qual = 'XX' AND 0 <> LENGTH(COALESCE(t.rend_prov_id, '')))
        THEN (
            CASE
                WHEN t.rend_prov_id_qual <> 'XX'
                THEN t.rend_prov_id
            END
            )
        WHEN (0 <> LENGTH(COALESCE(t.ord_prov_npid, '')))
        OR (t.ord_prov_id_qual = 'XX' AND 0 <> LENGTH(COALESCE(t.ord_prov_id, '')))
        THEN (
            CASE
                WHEN t.ord_prov_id_qual <> 'XX'
                THEN t.ord_prov_id
            END
            )
        WHEN 0 <> LENGTH(COALESCE(t.rend_prov_npid, ''))
        OR 0 <> LENGTH(COALESCE(t.rend_prov_id, ''))
        OR 0 <> LENGTH(COALESCE(t.rend_prov_name, ''))
        OR 0 <> LENGTH(COALESCE(t.rend_prov_taxonomy_code, ''))
        THEN (
            CASE
                WHEN t.rend_prov_id_qual <> 'XX'
                THEN t.rend_prov_id
            END
            )
        ELSE (
            CASE
                WHEN t.ord_prov_id_qual <> 'XX'
                THEN t.ord_prov_id
            END
            )
    END,                         -- prov_rendering_vendor_id
    NULL,                        -- prov_rendering_tax_id
    NULL,                        -- prov_rendering_dea_id
    NULL,                        -- prov_rendering_ssn
    NULL,                        -- prov_rendering_state_license
    NULL,                        -- prov_rendering_upin
    NULL,                        -- prov_rendering_commercial_id
    CASE
        WHEN (0 <> LENGTH(COALESCE(t.rend_prov_npid, '')))
        OR (t.rend_prov_id_qual = 'XX' AND 0 <> LENGTH(COALESCE(t.rend_prov_id, '')))
        THEN t.rend_prov_name
        WHEN (0 <> LENGTH(COALESCE(t.ord_prov_npid, '')))
        OR (t.ord_prov_id_qual = 'XX' AND 0 <> LENGTH(COALESCE(t.ord_prov_id, '')))
        THEN t.ord_prov_name
        WHEN 0 <> LENGTH(COALESCE(t.rend_prov_npid, ''))
        OR 0 <> LENGTH(COALESCE(t.rend_prov_id, ''))
        OR 0 <> LENGTH(COALESCE(t.rend_prov_name, ''))
        OR 0 <> LENGTH(COALESCE(t.rend_prov_taxonomy_code, ''))
        THEN t.rend_prov_name
        ELSE t.ord_prov_name
    END,                         -- prov_rendering_name_1
    NULL,                        -- prov_rendering_name_2
    NULL,                        -- prov_rendering_address_1
    NULL,                        -- prov_rendering_address_2
    NULL,                        -- prov_rendering_city
    NULL,                        -- prov_rendering_state
    NULL,                        -- prov_rendering_zip
    CASE
        WHEN (0 <> LENGTH(COALESCE(t.rend_prov_npid, '')))
        OR (t.rend_prov_id_qual = 'XX' AND 0 <> LENGTH(COALESCE(t.rend_prov_id, '')))
        THEN t.rend_prov_taxonomy_code
        WHEN (0 <> LENGTH(COALESCE(t.ord_prov_npid, '')))
        OR (t.ord_prov_id_qual = 'XX' AND 0 <> LENGTH(COALESCE(t.ord_prov_id, '')))
        THEN NULL
        WHEN 0 <> LENGTH(COALESCE(t.rend_prov_npid, ''))
        OR 0 <> LENGTH(COALESCE(t.rend_prov_id, ''))
        OR 0 <> LENGTH(COALESCE(t.rend_prov_name, ''))
        OR 0 <> LENGTH(COALESCE(t.rend_prov_taxonomy_code, ''))
        THEN t.rend_prov_taxonomy_code
    END,                         -- prov_rendering_std_taxonomy
    NULL,                        -- prov_rendering_vendor_specialty
    CASE
        WHEN t.bill_prov_id_qual <> 'XX'
        THEN t.bill_prov_id
    END,                         -- prov_billing_vendor_id
    NULL,                        -- prov_billing_tax_id
    NULL,                        -- prov_billing_dea_id
    NULL,                        -- prov_billing_ssn
    NULL,                        -- prov_billing_state_license
    NULL,                        -- prov_billing_upin
    NULL,                        -- prov_billing_commercial_id
    t.bill_prov_name,            -- prov_billing_name_1
    NULL,                        -- prov_billing_name_2
    NULL,                        -- prov_billing_address_1
    NULL,                        -- prov_billing_address_2
    NULL,                        -- prov_billing_city
    NULL,                        -- prov_billing_state
    NULL,                        -- prov_billing_zip
    t.bill_prov_taxonomy_code,   -- prov_billing_std_taxonomy
    NULL,                        -- prov_billing_vendor_specialty
    CASE
        WHEN t.refer_prov_id_qual <> 'XX'
        THEN t.refer_prov_id
    END,                         -- prov_referring_vendor_id
    NULL,                        -- prov_referring_tax_id
    NULL,                        -- prov_referring_dea_id
    NULL,                        -- prov_referring_ssn
    NULL,                        -- prov_referring_state_license
    NULL,                        -- prov_referring_upin
    NULL,                        -- prov_referring_commercial_id
    t.refer_prov_name,           -- prov_referring_name_1
    NULL,                        -- prov_referring_name_2
    NULL,                        -- prov_referring_address_1
    NULL,                        -- prov_referring_address_2
    NULL,                        -- prov_referring_city
    NULL,                        -- prov_referring_state
    NULL,                        -- prov_referring_zip
    t.refer_prov_taxonomy_code,  -- prov_referring_std_taxonomy
    NULL,                        -- prov_referring_vendor_specialty
    t.facility_id,               -- prov_facility_vendor_id
    NULL,                        -- prov_facility_tax_id
    NULL,                        -- prov_facility_dea_id
    NULL,                        -- prov_facility_ssn
    NULL,                        -- prov_facility_state_license
    NULL,                        -- prov_facility_upin
    NULL,                        -- prov_facility_commercial_id
    t.serv_facility_name,        -- prov_facility_name_1
    NULL,                        -- prov_facility_name_2
    t.serv_facility_address,     -- prov_facility_address_1
    NULL,                        -- prov_facility_address_2
    t.serv_facility_city,        -- prov_facility_city
    t.serv_facility_state,       -- prov_facility_state
    t.serv_facility_zip,         -- prov_facility_zip
    NULL,                        -- prov_facility_std_taxonomy
    NULL,                        -- prov_facility_vendor_specialty
    NULL,                        -- cob_payer_vendor_id_1
    NULL,                        -- cob_payer_seq_code_1
    NULL,                        -- cob_payer_hpid_1
    NULL,                        -- cob_payer_claim_filing_ind_code_1
    NULL,                        -- cob_ins_type_code_1
    NULL,                        -- cob_payer_vendor_id_2
    NULL,                        -- cob_payer_seq_code_2
    NULL,                        -- cob_payer_hpid_2
    NULL,                        -- cob_payer_claim_filing_ind_code_2
    NULL                         -- cob_ins_type_code_2
FROM transactions t
    INNER JOIN matching_payload mp ON t.hvjoinkey = mp.hvjoinkey
    CROSS JOIN svc_diag_exploder diag_explode
WHERE

-- include rows for non-null diagnoses
    (
        ARRAY(
            t.linked_diag_1, t.linked_diag_2, t.linked_diag_3,
            t.linked_diag_4
            )[diag_explode.n] IS NOT NULL
        )

-- include rows with all null diagnoses
    OR (
        COALESCE(
            t.linked_diag_1, t.linked_diag_2, t.linked_diag_3,
            t.linked_diag_4
            ) IS NULL
         AND diag_explode.n = 0
    )
    ;
