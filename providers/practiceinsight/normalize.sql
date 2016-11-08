INSERT INTO medicalclaims_common_model (
        claim_id,
        hvid,
        patient_gender,
        patient_age,
        patient_year_of_birth,
        patient_zip3,
        patient_state,
        claim_type,
        date_received,
        date_service,
        date_service_end,
        inst_date_admitted,
        inst_date_discharged,
        inst_admit_type_std_id,
        inst_admit_source_std_id,
        inst_discharge_status_std_id,
        inst_type_of_bill_std_id,
        inst_drg_std_id,
        place_of_service_std_id,
        service_line_number,
        diagnosis_code,
        diagnosis_priority,
        admit_diagnosis_ind,
        procedure_code,
        procedure_code_qual,
        principal_proc_ind,
        procedure_units,
        procedure_modifier_1,
        procedure_modifier_2,
        procedure_modifier_3,
        procedure_modifier_4,
        revenue_code,
        ndc_code,
        medical_coverage_type,
        line_charge,
        total_charge,
        prov_rendering_npi,
        prov_billing_npi,
        prov_referring_npi,
        prov_facility_npi,
        payer_name,
        prov_rendering_state_license,
        prov_rendering_upin,
        prov_rendering_commercial_id,
        prov_rendering_name_1,
        prov_rendering_name_2,
        prov_rendering_std_taxonomy,
        prov_billing_tax_id,
        prov_billing_state_license,
        prov_billing_upin,
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
        prov_referring_name_1,
        prov_referring_name_2,
        prov_facility_state_license,
        prov_facility_upin,
        prov_facility_commercial_id,
        prov_facility_name_1,
        prov_facility_address_1,
        prov_facility_address_2,
        prov_facility_city,
        prov_facility_state,
        prov_facility_zip
        )
SELECT DISTINCT  
    transactional.src_claim_id,                                                       -- claim_id
    COALESCE(mp.parentid, mp.hvid),                                                   -- hvid
    mp.gender,                                                                        -- patient_gender
    'CALC AGE?',                                                                      -- patient_age
    mp.yearOfBirth,                                                                   -- patient_year_of_birth
    mp.threeDigitZip,                                                                 -- patient_zip3
    mp.state,                                                                         -- patient_state
    transactional.claim_type_cd,                                                      -- claim_type
    transactional.edi_interchange_creation_dt                                         -- date_received
    COALESCE(transactional.svc_from_dt, transactional.stmt_from_dt),                  -- date_service
    CASE 
    WHEN transactional.svc_from_dt IS NOT NULL 
    THEN transactional.svc_to_dt 
    ELSE transactional.stmt_to_dt 
    END                                                                               -- date_service_end
    transactional.admsn_dt,                                                           -- inst_date_admitted
    transactional.dischg_dt,                                                          -- inst_date_discharged
    transactional.admsn_type_cd,                                                      -- inst_admit_type_std_id
    transactional.admsn_src_cd,                                                       -- inst_admit_source_std_id
    transactional.patnt_sts_cd,                                                       -- inst_discharge_status_std_id
    transactional.fclty_type_pos_cd,                                                  -- inst_type_of_bill_std_id
    transactional.drg_cd,                                                             -- inst_drg_std_id
    transactional.pos_cd,                                                             -- place_of_service_std_id
    transactional.src_svc_id,                                                         -- service_line_number
    diags.diag_code,                                                                  -- diagnosis_code
    CASE WHEN diags.diag_code = transactional.diag_cd_1 THEN '1' 
    ELSE WHEN diags.diag_code = transactional.diag_cd_2 THEN '2'
    ELSE WHEN diags.diag_code = transactional.diag_cd_3 THEN '3'
    ELSE WHEN diags.diag_code = transactional.diag_cd_4 THEN '4' 
    END,                                                                              -- diagnosis_priority
    transactional.admtg_diag_cd,                                                      -- admit_diagnosis_ind
    procs.proc_code,                                                                  -- procedure_code
    transactional.proc_cd_qual,                                                       -- procedure_code_qual
    CASE WHEN procs.proc_code = transactional.principal_proc_cd THEN 'Y'
    ELSE 'N' END,                                                                     -- principal_proc_ind
    transactional.units,                                                              -- procedure_units
    transactional.proc_modfr_1,                                                       -- procedure_modifier_1
    transactional.proc_modfr_2,                                                       -- procedure_modifier_2
    transactional.proc_modfr_3,                                                       -- procedure_modifier_3
    transactional.proc_modfr_4,                                                       -- procedure_modifier_4
    transactional.revnu_cd,                                                           -- revenue_code
    transactional.ndc,                                                                -- ndc_code
    transactional.dest_payer_claim_flng_ind_cd,                                       -- medical_coverage_type
    transactional.line_charg,                                                         -- line_charge
    transactional.tot_claim_charg_amt,                                                -- total_charge
    transactional.rendr_provdr_npi,                                                   -- prov_rendering_npi
    transactional.billg_provdr_npi,                                                   -- prov_billing_npi
    transactional.refrn_provdr_npi,                                                   -- prov_referring_npi
    transactional.fclty_npi,                                                          -- prov_facility_npi
    transactional.dest_payer_nm,                                                      -- payer_name
    transactional.rendr_provdr_stlc_nbr,                                              -- prov_rendering_state_license
    transactional.rendr_provdr_upin,                                                  -- prov_rendering_upin
    transactional.rendr_provdr_comm_nbr,                                              -- prov_rendering_commercial_id
    transactional.rendr_provdr_last_nm,                                               -- prov_rendering_name_1
    transactional.rendr_provdr_first_nm,                                              -- prov_rendering_name_2
    COALESCE(transactional.rendr_provdr_txnmy_svc, transactional.rendr_provdr_txnmy)  -- prov_rendering_std_taxonomy
    transactional.billg_provdr_tax_id,                                                -- prov_billing_tax_id
    transactional.billg_provdr_stlc_nbr,                                              -- prov_billing_state_license
    transactional.billg_provdr_upin,                                                  -- prov_billing_upin
    transactional.billg_provdr_last_or_orgal_nm,                                      -- prov_billing_name_1
    transactional.billg_provdr_first_nm,                                              -- prov_billing_name_2
    transactional.billg_provdr_addr_1,                                                -- prov_billing_address_1
    transactional.billg_provdr_addr_2,                                                -- prov_billing_address_2
    transactional.billg_provdr_addr_city,                                             -- prov_billing_city
    transactional.billg_provdr_addr_state,                                            -- prov_billing_state
    transactional.billg_provdr_addr_zip,                                              -- prov_billing_zip
    transactional.billg_provdr_txnmy,                                                 -- prov_billing_std_taxonomy
    transactional.refrn_provdr_stlc_nbr,                                              -- prov_referring_state_license
    transactional.refrn_provdr_upin,                                                  -- prov_referring_upin
    transactional.refrn_provdr_comm_nbr,                                              -- prov_referring_commercial_id
    transactional.refrn_provdr_last_nm,                                               -- prov_referring_name_1
    transactional.refrn_provdr_first_nm,                                              -- prov_referring_name_2
    transactional.fclty_stlc_nbr,                                                     -- prov_facility_state_license
    transactional.fclty_comm_nbr,                                                     -- prov_facility_commercial_id
    transactional.fclty_nm,                                                           -- prov_facility_name_1
    transactional.fclty_addr_1,                                                       -- prov_facility_address_1
    transactional.fclty_addr_2,                                                       -- prov_facility_address_2
    transactional.fclty_addr_city,                                                    -- prov_facility_city
    transactional.fclty_addr_state,                                                   -- prov_facility_state
    transactional.fclty_addr_zip                                                      -- prov_facility_zip
FROM transactional_raw transactional
    INNER JOIN exploded_diag_codes diags ON transactional.src_claim_id || transactional.src_svc_id = diags.claim_svc_num 
    INNER JOIN exploded_proc_codes procs ON transactional.src_claim_id || transactional.src_svc_id = procs.claim_svc_num
    LEFT JOIN matching_payload mp ON transactional.src_claim_id = mp.claimid
WHERE split_part(trim(transactional.ecodes), ',', ecodes_exploder.n) IS NOT NULL
    AND split_part(trim(transactional.ecodes), ',', ecodes_exploder.n) != ''
    AND split_part(trim(transactional.dx), ',', dx_exploder.n) IS NOT NULL
    AND split_part(trim(transactional.dx), ',', dx_exploder.n) != ''
    AND split_part(trim(transactional.other_diag_codes), ',', other_diag_codes_exploder.n) IS NOT NULL
    AND split_part(trim(transactional.other_diag_codes), ',', other_diag_codes_exploder.n) != ''
    ;
