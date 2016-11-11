INSERT INTO medicalclaims_common_model (
        claim_id,
        hvid,
        source_version,
        patient_gender,
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
        prov_facility_commercial_id,
        prov_facility_name_1,
        prov_facility_address_1,
        prov_facility_address_2,
        prov_facility_city,
        prov_facility_state,
        prov_facility_zip
        )
SELECT DISTINCT  
    transactional.src_claim_id,                                                        -- claim_id
    COALESCE(mp.parentid, mp.hvid),                                                    -- hvid
    '1',                                                                               -- source_version
    mp.gender,                                                                         -- patient_gender
    mp.yearOfBirth,                                                                    -- patient_year_of_birth
    mp.threeDigitZip,                                                                  -- patient_zip3
    UPPER(mp.state),                                                                   -- patient_state
    transactional.claim_type_cd,                                                       -- claim_type
    transactional.edi_interchange_creation_dt,                                         -- date_received
    CASE 
    WHEN transactional.svc_from_dt IS NOT NULL 
    AND diags.diag_code IN (transactional.diag_cd_1, transactional.diag_cd_2, 
        transactional.diag_cd_3, transactional.diag_cd_4)
    THEN svc_from.formatted
    WHEN transactional.stmnt_from_dt IS NOT NULL 
    THEN stmnt_from.formatted
    ELSE (
    SELECT MIN(svc_from2.formatted) 
    FROM transactional_raw t2 
        LEFT JOIN dates svc_from2 ON t2.svc_from_dt = svc_from2.date
    WHERE t2.src_claim_id = transactional.src_claim_id
        )
    END,                                                                               -- date_service
    CASE 
    WHEN transactional.svc_from_dt IS NOT NULL 
    AND diags.diag_code IN (transactional.diag_cd_1, transactional.diag_cd_2, 
        transactional.diag_cd_3, transactional.diag_cd_4)
    THEN svc_to.formatted
    WHEN transactional.stmnt_from_dt IS NOT NULL
    THEN stmnt_to.formatted
    ELSE (
    SELECT MAX(svc_to2.formatted)
    FROM transactional_raw t2
        LEFT JOIN dates svc_to2 ON t2.svc_to_dt = svc_to2.date
    WHERE t2.src_claim_id = transactional.src_claim_id
        ) 
    END,                                                                               -- date_service_end
    CASE 
    WHEN transactional.claim_type_cd = 'I'
    THEN admsn_dt.formatted
    END,                                                                               -- inst_date_admitted
    CASE
    WHEN transactional.claim_type_cd = 'I'
    THEN dischg_dt.formatted
    END,                                                                               -- inst_date_discharged
    CASE 
    WHEN transactional.claim_type_cd = 'I'
    THEN transactional.admsn_type_cd
    END,                                                                               -- inst_admit_type_std_id
    CASE
    WHEN transactional.claim_type_cd = 'I'
    THEN transactional.admsn_src_cd
    END,                                                                               -- inst_admit_source_std_id
    CASE 
    WHEN transactional.claim_type_cd = 'I'
    THEN transactional.patnt_sts_cd
    END,                                                                               -- inst_discharge_status_std_id
    CASE 
    WHEN transactional.claim_type_cd = 'I'
    THEN (transactional.fclty_type_pos_cd || transactional.claim_freq_cd)
    END,                                                                               -- inst_type_of_bill_std_id
    CASE
    WHEN transactional.claim_type_cd = 'I'
    THEN transactional.drg_cd
    END,                                                                               -- inst_drg_std_id
    CASE 
    WHEN transactional.claim_type_cd = 'P'
    THEN COALESCE(transactional.pos_cd, transactional.fclty_type_pos_cd)
    END,                                                                               -- place_of_service_std_id
    CASE 
    WHEN diags.diag_code IN (transactional.diag_cd_1, transactional.diag_cd_2, 
        transactional.diag_cd_3, transactional.diag_cd_4)
    THEN  transactional.line_nbr
    END,                                                                               -- service_line_number
    REPLACE(REPLACE(REPLACE(diags.diag_code, '.', ''), ',', ''), ' ', ''),             -- diagnosis_code
    CASE 
    WHEN transactional.claim_type_cd = 'I' THEN NULL
    WHEN diags.diag_code = transactional.diag_cd_1 THEN '1' 
    WHEN diags.diag_code = transactional.diag_cd_2 THEN '2'
    WHEN diags.diag_code = transactional.diag_cd_3 THEN '3'
    WHEN diags.diag_code = transactional.diag_cd_4 THEN '4' 
    END,                                                                               -- diagnosis_priority
    CASE 
    WHEN transactional.claim_type_cd = 'P' THEN NULL
    WHEN transactional.admtg_diag_cd = diags.diag_code THEN 'Y'
    ELSE 'N'
    END,                                                                               -- admit_diagnosis_ind
    CASE
    WHEN diags.diag_code IN (transactional.diag_cd_1, transactional.diag_cd_2, 
        transactional.diag_cd_3, transactional.diag_cd_4)
    THEN REPLACE(REPLACE(REPLACE(procs.proc_code, '.', ''), ',', ''), ' ', '')
    END,                                                                               -- procedure_code
    CASE 
    WHEN diags.diag_code IN (transactional.diag_cd_1, transactional.diag_cd_2, 
        transactional.diag_cd_3, transactional.diag_cd_4)
    THEN transactional.proc_cd_qual
    END,                                                                               -- procedure_code_qual
    CASE 
    WHEN diags.diag_code NOT IN (transactional.diag_cd_1, transactional.diag_cd_2, 
        transactional.diag_cd_3, transactional.diag_cd_4)
    OR transactional.claim_type_cd = 'I'
    THEN NULL
    WHEN procs.proc_code = transactional.prinpl_proc_cd 
    THEN 'Y'
    ELSE 'N' END,                                                                      -- principal_proc_ind
    CASE
    WHEN diags.diag_code IN (transactional.diag_cd_1, transactional.diag_cd_2, 
        transactional.diag_cd_3, transactional.diag_cd_4)
    THEN transactional.units
    END,                                                                               -- procedure_units
    CASE 
    WHEN diags.diag_code IN (transactional.diag_cd_1, transactional.diag_cd_2, 
        transactional.diag_cd_3, transactional.diag_cd_4)
    THEN transactional.proc_modfr_1
    END,                                                                               -- procedure_modifier_1
    CASE
    WHEN diags.diag_code IN (transactional.diag_cd_1, transactional.diag_cd_2, 
        transactional.diag_cd_3, transactional.diag_cd_4)
    THEN transactional.proc_modfr_2
    END,                                                                               -- procedure_modifier_2
    CASE
    WHEN diags.diag_code IN (transactional.diag_cd_1, transactional.diag_cd_2, 
        transactional.diag_cd_3, transactional.diag_cd_4)
    THEN transactional.proc_modfr_3
    END,                                                                               -- procedure_modifier_3
    CASE
    WHEN diags.diag_code IN (transactional.diag_cd_1, transactional.diag_cd_2, 
        transactional.diag_cd_3, transactional.diag_cd_4)
    THEN transactional.proc_modfr_4
    END,                                                                               -- procedure_modifier_4
    CASE
    WHEN diags.diag_code IN (transactional.diag_cd_1, transactional.diag_cd_2, 
        transactional.diag_cd_3, transactional.diag_cd_4)
    THEN transactional.revnu_cd
    END,                                                                               -- revenue_code
    CASE
    WHEN diags.diag_code IN (transactional.diag_cd_1, transactional.diag_cd_2, 
        transactional.diag_cd_3, transactional.diag_cd_4)
    THEN transactional.ndc
    END,                                                                               -- ndc_code
    transactional.dest_payer_claim_flng_ind_cd,                                        -- medical_coverage_type
    CASE 
    WHEN diags.diag_code IN (transactional.diag_cd_1, transactional.diag_cd_2, 
        transactional.diag_cd_3, transactional.diag_cd_4)
    THEN transactional.line_charg
    END,                                                                               -- line_charge
    transactional.tot_claim_charg_amt,                                                 -- total_charge
    transactional.rendr_provdr_npi,                                                    -- prov_rendering_npi
    transactional.billg_provdr_npi,                                                    -- prov_billing_npi
    transactional.refrn_provdr_npi,                                                    -- prov_referring_npi
    transactional.fclty_npi,                                                           -- prov_facility_npi
    REPLACE(transactional.dest_payer_nm, '"', ''),                                     -- payer_name
    transactional.rendr_provdr_stlc_nbr,                                               -- prov_rendering_state_license
    transactional.rendr_provdr_upin,                                                   -- prov_rendering_upin
    transactional.rendr_provdr_comm_nbr,                                               -- prov_rendering_commercial_id
    transactional.rendr_provdr_last_nm,                                                -- prov_rendering_name_1
    transactional.rendr_provdr_first_nm,                                               -- prov_rendering_name_2
    COALESCE(transactional.rendr_provdr_txnmy_svc, transactional.rendr_provdr_txnmy),  -- prov_rendering_std_taxonomy
    transactional.billg_provdr_tax_id,                                                 -- prov_billing_tax_id
    transactional.billg_provdr_stlc_nbr,                                               -- prov_billing_state_license
    transactional.billg_provdr_upin,                                                   -- prov_billing_upin
    transactional.billg_provdr_last_or_orgal_nm,                                       -- prov_billing_name_1
    transactional.billg_provdr_first_nm,                                               -- prov_billing_name_2
    transactional.billg_provdr_addr_1,                                                 -- prov_billing_address_1
    transactional.billg_provdr_addr_2,                                                 -- prov_billing_address_2
    transactional.billg_provdr_addr_city,                                              -- prov_billing_city
    transactional.billg_provdr_addr_state,                                             -- prov_billing_state
    transactional.billg_provdr_addr_zip,                                               -- prov_billing_zip
    transactional.billg_provdr_txnmy,                                                  -- prov_billing_std_taxonomy
    transactional.refrn_provdr_stlc_nbr,                                               -- prov_referring_state_license
    transactional.refrn_provdr_upin,                                                   -- prov_referring_upin
    transactional.refrn_provdr_comm_nbr,                                               -- prov_referring_commercial_id
    transactional.refrn_provdr_last_nm,                                                -- prov_referring_name_1
    transactional.refrn_provdr_first_nm,                                               -- prov_referring_name_2
    transactional.fclty_stlc_nbr,                                                      -- prov_facility_state_license
    transactional.fclty_comm_nbr,                                                      -- prov_facility_commercial_id
    transactional.fclty_nm,                                                            -- prov_facility_name_1
    REPLACE(transactional.fclty_addr_1, '"', ''),                                      -- prov_facility_address_1
    REPLACE(transactional.fclty_addr_2, '"', ''),                                      -- prov_facility_address_2
    REPLACE(transactional.fclty_addr_city, '"', ''),                                   -- prov_facility_city
    transactional.fclty_addr_state,                                                    -- prov_facility_state
    transactional.fclty_addr_zip                                                       -- prov_facility_zip
FROM transactional_raw transactional
    LEFT JOIN matching_payload mp ON transactional.src_claim_id = mp.claimid

    -- fix dates
    LEFT JOIN dates svc_to ON transactional.svc_to_dt = svc_to.date
    LEFT JOIN dates svc_from ON transactional.svc_from_dt = svc_from.date
    LEFT JOIN dates stmnt_to ON transactional.stmnt_to_dt = stmnt_to.date
    LEFT JOIN dates stmnt_from ON transactional.stmnt_from_dt = stmnt_from.date
    LEFT JOIN dates admsn_dt ON transactional.admsn_dt = admsn_dt.date
    LEFT JOIN dates dischg_dt ON transactional.dischg_dt = dischg_dt.date

    -- these inner joins will each perform a cartesian product on this table, exploding the table for each diag/proc
    INNER JOIN exploded_diag_codes diags ON (transactional.src_claim_id || transactional.src_svc_id) = diags.claim_svc_num 
    INNER JOIN exploded_proc_codes procs ON (transactional.src_claim_id || transactional.src_svc_id) = procs.claim_svc_num
    ;


-- Insert service lines for institutional claims with diagnoses (nulled out above)
INSERT INTO medicalclaims_common_model (
        claim_id,
        hvid,
        source_version,
        patient_gender,
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
        service_line_number,
        procedure_code,
        procedure_code_qual,
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
        prov_facility_commercial_id,
        prov_facility_name_1,
        prov_facility_address_1,
        prov_facility_address_2,
        prov_facility_city,
        prov_facility_state,
        prov_facility_zip
        )
SELECT DISTINCT  
    transactional.src_claim_id,                                                        -- claim_id
    COALESCE(mp.parentid, mp.hvid),                                                    -- hvid
    '1',                                                                               -- source_version
    mp.gender,                                                                         -- patient_gender
    mp.yearOfBirth,                                                                    -- patient_year_of_birth
    mp.threeDigitZip,                                                                  -- patient_zip3
    UPPER(mp.state),                                                                   -- patient_state
    transactional.claim_type_cd,                                                       -- claim_type
    transactional.edi_interchange_creation_dt,                                                -- date_received
    CASE 
    WHEN transactional.svc_from_dt IS NOT NULL 
    THEN svc_from.formatted
    WHEN transactional.stmnt_from_dt IS NOT NULL 
    THEN stmnt_from.formatted
    ELSE (
    SELECT MIN(svc_from2.formatted) 
    FROM transactional_raw t2 
        LEFT JOIN dates svc_from2 ON t2.svc_from_dt = svc_from2.date
    WHERE t2.src_claim_id = transactional.src_claim_id
        )
    END,                                                                               -- date_service
    CASE 
    WHEN transactional.svc_from_dt IS NOT NULL 
    THEN svc_to.formatted
    WHEN transactional.stmnt_from_dt IS NOT NULL
    THEN stmnt_to.formatted
    ELSE (
    SELECT MAX(svc_to2.formatted)
    FROM transactional_raw t2
        LEFT JOIN dates svc_to2 ON t2.svc_to_dt = svc_to2.date
    WHERE t2.src_claim_id = transactional.src_claim_id
        ) 
    END,                                                                               -- date_service_end
    admsn_dt.formatted,                                                                -- inst_date_admitted
    dischg_dt.formatted,                                                               -- inst_date_discharged
    transactional.admsn_type_cd,                                                       -- inst_admit_type_std_id
    transactional.admsn_src_cd,                                                        -- inst_admit_source_std_id
    transactional.patnt_sts_cd,                                                        -- inst_discharge_status_std_id
    (transactional.fclty_type_pos_cd || transactional.claim_freq_cd),                  -- inst_type_of_bill_std_id
    transactional.drg_cd,                                                              -- inst_drg_std_id
    transactional.line_nbr,                                                            -- service_line_number
    REPLACE(REPLACE(REPLACE(procs.proc_code, '.', ''), ',', ''), ' ', ''),             -- procedure_code
    transactional.proc_cd_qual,                                                        -- procedure_code_qual
    transactional.units,                                                               -- procedure_units
    transactional.proc_modfr_1,                                                        -- procedure_modifier_1
    transactional.proc_modfr_2,                                                        -- procedure_modifier_2
    transactional.proc_modfr_3,                                                        -- procedure_modifier_3
    transactional.proc_modfr_4,                                                        -- procedure_modifier_4
    transactional.revnu_cd,                                                            -- revenue_code
    transactional.ndc,                                                                 -- ndc_code
    transactional.dest_payer_claim_flng_ind_cd,                                        -- medical_coverage_type
    transactional.line_charg,                                                          -- line_charge
    transactional.tot_claim_charg_amt,                                                 -- total_charge
    transactional.rendr_provdr_npi,                                                    -- prov_rendering_npi
    transactional.billg_provdr_npi,                                                    -- prov_billing_npi
    transactional.refrn_provdr_npi,                                                    -- prov_referring_npi
    transactional.fclty_npi,                                                           -- prov_facility_npi
    REPLACE(transactional.dest_payer_nm, '"', ''),                                     -- payer_name
    transactional.rendr_provdr_stlc_nbr,                                               -- prov_rendering_state_license
    transactional.rendr_provdr_upin,                                                   -- prov_rendering_upin
    transactional.rendr_provdr_comm_nbr,                                               -- prov_rendering_commercial_id
    transactional.rendr_provdr_last_nm,                                                -- prov_rendering_name_1
    transactional.rendr_provdr_first_nm,                                               -- prov_rendering_name_2
    COALESCE(transactional.rendr_provdr_txnmy_svc, transactional.rendr_provdr_txnmy),  -- prov_rendering_std_taxonomy
    transactional.billg_provdr_tax_id,                                                 -- prov_billing_tax_id
    transactional.billg_provdr_stlc_nbr,                                               -- prov_billing_state_license
    transactional.billg_provdr_upin,                                                   -- prov_billing_upin
    transactional.billg_provdr_last_or_orgal_nm,                                       -- prov_billing_name_1
    transactional.billg_provdr_first_nm,                                               -- prov_billing_name_2
    transactional.billg_provdr_addr_1,                                                 -- prov_billing_address_1
    transactional.billg_provdr_addr_2,                                                 -- prov_billing_address_2
    transactional.billg_provdr_addr_city,                                              -- prov_billing_city
    transactional.billg_provdr_addr_state,                                             -- prov_billing_state
    transactional.billg_provdr_addr_zip,                                               -- prov_billing_zip
    transactional.billg_provdr_txnmy,                                                  -- prov_billing_std_taxonomy
    transactional.refrn_provdr_stlc_nbr,                                               -- prov_referring_state_license
    transactional.refrn_provdr_upin,                                                   -- prov_referring_upin
    transactional.refrn_provdr_comm_nbr,                                               -- prov_referring_commercial_id
    transactional.refrn_provdr_last_nm,                                                -- prov_referring_name_1
    transactional.refrn_provdr_first_nm,                                               -- prov_referring_name_2
    transactional.fclty_stlc_nbr,                                                      -- prov_facility_state_license
    transactional.fclty_comm_nbr,                                                      -- prov_facility_commercial_id
    transactional.fclty_nm,                                                            -- prov_facility_name_1
    REPLACE(transactional.fclty_addr_1, '"', ''),                                      -- prov_facility_address_1
    REPLACE(transactional.fclty_addr_2, '"', ''),                                      -- prov_facility_address_2
    REPLACE(transactional.fclty_addr_city, '"', ''),                                   -- prov_facility_city
    transactional.fclty_addr_state,                                                    -- prov_facility_state
    transactional.fclty_addr_zip                                                       -- prov_facility_zip
FROM transactional_raw transactional
    LEFT JOIN matching_payload mp ON transactional.src_claim_id = mp.claimid

    -- fix dates
    LEFT JOIN dates svc_to ON transactional.svc_to_dt = svc_to.date
    LEFT JOIN dates svc_from ON transactional.svc_from_dt = svc_from.date
    LEFT JOIN dates stmnt_to ON transactional.stmnt_to_dt = stmnt_to.date
    LEFT JOIN dates stmnt_from ON transactional.stmnt_from_dt = stmnt_from.date
    LEFT JOIN dates admsn_dt ON transactional.admsn_dt = admsn_dt.date
    LEFT JOIN dates dischg_dt ON transactional.dischg_dt = dischg_dt.date

    -- these inner joins will each perform a cartesian product on this table, exploding the table for each proc
    INNER JOIN exploded_proc_codes procs ON (transactional.src_claim_id || transactional.src_svc_id) = procs.claim_svc_num
WHERE transactional.src_claim_id IN (
    SELECT DISTINCT claim_id 
    FROM medicalclaims_common_model
    WHERE claim_type = 'I'
        AND diagnosis_code IS NOT NULL
        )
    ;

-- delete diagnosis codes that should not have been added
DELETE FROM medicalclaims_common_model
WHERE record_id IN (
    SELECT record_id 
    FROM medicalclaims_common_model base 
    WHERE base.service_line_number IS NULL
        AND base.diagnosis_code IN (
        SELECT sub.diagnosis_code
        FROM medicalclaims_common_model sub
        WHERE sub.claim_id = base.claim_id
            AND sub.service_line_number IS NOT NULL
            )
        )
