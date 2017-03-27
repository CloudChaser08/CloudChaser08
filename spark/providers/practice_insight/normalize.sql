DROP TABLE IF EXISTS tmp;
CREATE TABLE tmp AS 
SELECT * FROM medicalclaims_common_model
;

INSERT INTO tmp
SELECT DISTINCT  
    monotonically_increasing_id(),                                -- record_id
    transactional.src_claim_id,                                   -- claim_id
    mp.hvid,                                                      -- hvid
    {today},                                                      -- created
    '1',                                                          -- model_version
    {setid},                                                      -- data_set
    {feedname},                                                   -- data_feed
    {vendor},                                                     -- data_vendor
    '1',                                                          -- source_version
    mp.gender,                                                    -- patient_gender
    NULL,                                                         -- patient_age
    mp.yearOfBirth,                                               -- patient_year_of_birth
    mp.threeDigitZip,                                             -- patient_zip3
    UPPER(mp.state),                                              -- patient_state
    transactional.claim_type_cd,                                  -- claim_type
    extract_date(
        transactional.edi_interchange_creation_dt, '%Y-%m-%d'
        ),                                                        -- date_received
    CASE 
    WHEN extract_date(transactional.svc_from_dt, '%Y%m%d') IS NOT NULL 
    AND diags.diag_code IN (transactional.diag_cd_1, transactional.diag_cd_2, 
        transactional.diag_cd_3, transactional.diag_cd_4)
    THEN extract_date(transactional.svc_from_dt, '%Y%m%d')
    WHEN extract_date(transactional.stmnt_from_dt, '%Y%m%d') IS NOT NULL 
    THEN extract_date(transactional.stmnt_from_dt, '%Y%m%d')
    ELSE (
    SELECT MIN(extract_date(t2.svc_from_dt, '%Y%m%d'))
    FROM transactional_raw t2 
    WHERE t2.src_claim_id = transactional.src_claim_id
        )
    END,                                                          -- date_service
    CASE 
    WHEN extract_date(transactional.svc_from_dt, '%Y%m%d') IS NOT NULL 
    AND diags.diag_code IN (transactional.diag_cd_1, transactional.diag_cd_2, 
        transactional.diag_cd_3, transactional.diag_cd_4)
    THEN extract_date(transactional.svc_to_dt, '%Y%m%d')
    WHEN extract_date(transactional.stmnt_from_dt, '%Y%m%d') IS NOT NULL
    THEN extract_date(transactional.stmnt_to_dt, '%Y%m%d')
    ELSE (
    SELECT MAX(extract_date(t2.svc_to_dt, '%Y%m%d'))
    FROM transactional_raw t2
    WHERE t2.src_claim_id = transactional.src_claim_id
        ) 
    END,                                                          -- date_service_end
    CASE 
    WHEN transactional.claim_type_cd = 'I'
    THEN extract_date(transactional.admsn_dt, '%Y%m%d')
    END,                                                          -- inst_date_admitted
    CASE
    WHEN transactional.claim_type_cd = 'I'
    THEN extract_date(transactional.dischg_dt, '%Y%m%d')
    END,                                                          -- inst_date_discharged
    CASE 
    WHEN transactional.claim_type_cd = 'I'
    THEN transactional.admsn_type_cd
    END,                                                          -- inst_admit_type_std_id
    NULL,                                                         -- inst_admit_type_vendor_id
    NULL,                                                         -- inst_admit_type_vendor_desc
    CASE
    WHEN transactional.claim_type_cd = 'I'
    THEN transactional.admsn_src_cd
    END,                                                          -- inst_admit_source_std_id
    NULL,                                                         -- inst_admit_source_vendor_id
    NULL,                                                         -- inst_admit_source_vendor_desc
    CASE 
    WHEN transactional.claim_type_cd = 'I'
    THEN scrub_discharge_status(transactional.patnt_sts_cd)
    END,                                                          -- inst_discharge_status_std_id
    NULL,                                                         -- inst_discharge_status_vendor_id
    NULL,                                                         -- inst_discharge_status_vendor_desc
    CASE 
    WHEN transactional.claim_type_cd = 'I'
    THEN CONCAT(transactional.fclty_type_pos_cd, transactional.claim_freq_cd)
    END,                                                          -- inst_type_of_bill_std_id
    NULL,                                                         -- inst_type_of_bill_vendor_id
    NULL,                                                         -- inst_type_of_bill_vendor_desc
    CASE
    WHEN transactional.claim_type_cd = 'I'
    THEN nullify_drg_blacklist(transactional.drg_cd)
    END,                                                          -- inst_drg_std_id
    NULL,                                                         -- inst_drg_vendor_id
    NULL,                                                         -- inst_drg_vendor_desc
    CASE 
    WHEN transactional.claim_type_cd = 'P'
    THEN (
        CASE
        WHEN transactional.pos_cd IS NOT NULL
        AND transactional.pos_cd <> '' THEN transactional.pos_cd
        WHEN transactional.fclty_type_pos_cd IS NOT NULL
        AND transactional.fclty_type_pos_cd <> '' THEN transactional.fclty_type_pos_cd
        END
        )
    END,                                                          -- place_of_service_std_id
    NULL,                                                         -- place_of_service_vendor_id
    NULL,                                                         -- place_of_service_vendor_desc
    CASE 
    WHEN diags.diag_code IN (transactional.diag_cd_1, transactional.diag_cd_2,
        transactional.diag_cd_3, transactional.diag_cd_4)
    THEN  transactional.line_nbr
    END,                                                          -- service_line_number
    clean_up_diagnosis_code(
        diags.diag_code, NULL,
        -- exact definition of service date above
        CASE 
        WHEN extract_date(transactional.svc_from_dt, '%Y%m%d') IS NOT NULL 
        AND diags.diag_code IN (transactional.diag_cd_1, transactional.diag_cd_2, 
            transactional.diag_cd_3, transactional.diag_cd_4)
        THEN extract_date(transactional.svc_from_dt, '%Y%m%d')
        WHEN extract_date(transactional.stmnt_from_dt, '%Y%m%d') IS NOT NULL 
        THEN extract_date(transactional.stmnt_from_dt, '%Y%m%d')
        ELSE (
        SELECT MIN(extract_date(t2.svc_from_dt, '%Y%m%d'))
        FROM transactional_raw t2 
        WHERE t2.src_claim_id = transactional.src_claim_id
            )
        END
        ),                                                        -- diagnosis_code
    NULL,                                                         -- diagnosis_code_qual
    CASE 
    WHEN transactional.claim_type_cd = 'I' THEN NULL
    WHEN diags.diag_code = transactional.diag_cd_1 THEN '1' 
    WHEN diags.diag_code = transactional.diag_cd_2 THEN '2'
    WHEN diags.diag_code = transactional.diag_cd_3 THEN '3'
    WHEN diags.diag_code = transactional.diag_cd_4 THEN '4' 
    END,                                                          -- diagnosis_priority
    CASE 
    WHEN transactional.claim_type_cd = 'P' THEN NULL
    WHEN transactional.admtg_diag_cd = diags.diag_code THEN 'Y'
    ELSE 'N'
    END,                                                          -- admit_diagnosis_ind
    CASE
    WHEN diags.diag_code IN (transactional.diag_cd_1, transactional.diag_cd_2, 
        transactional.diag_cd_3, transactional.diag_cd_4)
    THEN clean_up_procedure_code(procs.proc_code)
    END,                                                          -- procedure_code
    CASE 
    WHEN diags.diag_code IN (transactional.diag_cd_1, transactional.diag_cd_2, 
        transactional.diag_cd_3, transactional.diag_cd_4)
    THEN transactional.proc_cd_qual
    END,                                                          -- procedure_code_qual
    CASE 
    WHEN diags.diag_code NOT IN (transactional.diag_cd_1, transactional.diag_cd_2, 
        transactional.diag_cd_3, transactional.diag_cd_4)
    OR transactional.claim_type_cd = 'I'
    THEN NULL
    WHEN procs.proc_code = transactional.prinpl_proc_cd 
    THEN 'Y'
    ELSE 'N' END,                                                 -- principal_proc_ind
    CASE
    WHEN diags.diag_code IN (transactional.diag_cd_1, transactional.diag_cd_2, 
        transactional.diag_cd_3, transactional.diag_cd_4)
    THEN transactional.units
    END,                                                          -- procedure_units
    CASE 
    WHEN diags.diag_code IN (transactional.diag_cd_1, transactional.diag_cd_2, 
        transactional.diag_cd_3, transactional.diag_cd_4)
    THEN transactional.proc_modfr_1
    END,                                                          -- procedure_modifier_1
    CASE
    WHEN diags.diag_code IN (transactional.diag_cd_1, transactional.diag_cd_2, 
        transactional.diag_cd_3, transactional.diag_cd_4)
    THEN transactional.proc_modfr_2
    END,                                                          -- procedure_modifier_2
    CASE
    WHEN diags.diag_code IN (transactional.diag_cd_1, transactional.diag_cd_2, 
        transactional.diag_cd_3, transactional.diag_cd_4)
    THEN transactional.proc_modfr_3
    END,                                                          -- procedure_modifier_3
    CASE
    WHEN diags.diag_code IN (transactional.diag_cd_1, transactional.diag_cd_2, 
        transactional.diag_cd_3, transactional.diag_cd_4)
    THEN transactional.proc_modfr_4
    END,                                                          -- procedure_modifier_4
    CASE
    WHEN diags.diag_code IN (transactional.diag_cd_1, transactional.diag_cd_2, 
        transactional.diag_cd_3, transactional.diag_cd_4)
    THEN transactional.revnu_cd
    END,                                                          -- revenue_code
    CASE
    WHEN diags.diag_code IN (transactional.diag_cd_1, transactional.diag_cd_2, 
        transactional.diag_cd_3, transactional.diag_cd_4)
    THEN transactional.ndc
    END,                                                          -- ndc_code
    transactional.dest_payer_claim_flng_ind_cd,                   -- medical_coverage_type
    CASE 
    WHEN diags.diag_code IN (transactional.diag_cd_1, transactional.diag_cd_2, 
        transactional.diag_cd_3, transactional.diag_cd_4)
    THEN extract_currency(transactional.line_charg)
    END,                                                          -- line_charge
    NULL,                                                         -- line_allowed
    extract_currency(transactional.tot_claim_charg_amt),            -- total_charge
    NULL,                                                         -- total_allowed
    CASE
    WHEN transactional.claim_type_cd != 'I' 
    AND diags.diag_code IN (transactional.diag_cd_1, transactional.diag_cd_2, 
        transactional.diag_cd_3, transactional.diag_cd_4)
    AND transactional.rendr_provdr_npi_svc IS NOT NULL
    AND transactional.rendr_provdr_npi_svc <> ''
    THEN transactional.rendr_provdr_npi_svc
    ELSE transactional.rendr_provdr_npi
    END,                                                          -- prov_rendering_npi
    transactional.billg_provdr_npi,                               -- prov_billing_npi
    CASE
    WHEN transactional.claim_type_cd != 'I' 
    AND diags.diag_code IN (transactional.diag_cd_1, transactional.diag_cd_2, 
        transactional.diag_cd_3, transactional.diag_cd_4)
    AND transactional.refrn_provdr_npi_svc IS NOT NULL
    THEN transactional.refrn_provdr_npi_svc
    ELSE transactional.refrn_provdr_npi
    END,                                                          -- prov_referring_npi
    CASE
    WHEN transactional.claim_type_cd != 'I' 
    AND diags.diag_code IN (transactional.diag_cd_1, transactional.diag_cd_2, 
        transactional.diag_cd_3, transactional.diag_cd_4)
    AND transactional.fclty_npi_svc IS NOT NULL
    THEN transactional.fclty_npi_svc
    ELSE transactional.fclty_npi
    END,                                                          -- prov_facility_npi
    NULL,                                                         -- payer_vendor_id
    REGEXP_REPLACE(transactional.dest_payer_nm, '"', ''),         -- payer_name
    NULL,                                                         -- payer_parent_name
    NULL,                                                         -- payer_org_name
    NULL,                                                         -- payer_plan_id
    NULL,                                                         -- payer_plan_name
    NULL,                                                         -- payer_type
    NULL,                                                         -- prov_rendering_vendor_id
    NULL,                                                         -- prov_rendering_tax_id
    NULL,                                                         -- prov_rendering_dea_id
    NULL,                                                         -- prov_rendering_ssn
    CASE
    WHEN transactional.claim_type_cd = 'I'
    OR diags.diag_code NOT IN (transactional.diag_cd_1, transactional.diag_cd_2,
        transactional.diag_cd_3, transactional.diag_cd_4)
    OR transactional.rendr_provdr_npi_svc IS NULL
    THEN transactional.rendr_provdr_stlc_nbr
    ELSE NULL
    END,                                                          -- prov_rendering_state_license
    CASE
    WHEN transactional.claim_type_cd = 'I'
    OR diags.diag_code NOT IN (transactional.diag_cd_1, transactional.diag_cd_2,
        transactional.diag_cd_3, transactional.diag_cd_4)
    OR transactional.rendr_provdr_npi_svc IS NULL
    THEN transactional.rendr_provdr_upin
    ELSE NULL
    END,                                                          -- prov_rendering_upin
    CASE
    WHEN transactional.claim_type_cd = 'I'
    OR diags.diag_code NOT IN (transactional.diag_cd_1, transactional.diag_cd_2,
        transactional.diag_cd_3, transactional.diag_cd_4)
    OR transactional.rendr_provdr_npi_svc IS NULL
    THEN transactional.rendr_provdr_comm_nbr
    ELSE NULL
    END,                                                          -- prov_rendering_commercial_id
    CASE
    WHEN transactional.claim_type_cd = 'I'
    OR diags.diag_code NOT IN (transactional.diag_cd_1, transactional.diag_cd_2,
        transactional.diag_cd_3, transactional.diag_cd_4)
    OR transactional.rendr_provdr_npi_svc IS NULL
    THEN transactional.rendr_provdr_last_nm
    ELSE NULL
    END,                                                          -- prov_rendering_name_1
    CASE
    WHEN transactional.claim_type_cd = 'I'
    OR diags.diag_code NOT IN (transactional.diag_cd_1, transactional.diag_cd_2,
        transactional.diag_cd_3, transactional.diag_cd_4)
    OR transactional.rendr_provdr_npi_svc IS NULL
    THEN transactional.rendr_provdr_first_nm
    ELSE NULL
    END,                                                          -- prov_rendering_name_2
    NULL,                                                         -- prov_rendering_address_1
    NULL,                                                         -- prov_rendering_address_2
    NULL,                                                         -- prov_rendering_city
    NULL,                                                         -- prov_rendering_state
    NULL,                                                         -- prov_rendering_zip
    CASE
    WHEN transactional.claim_type_cd != 'I' 
    AND diags.diag_code IN (transactional.diag_cd_1, transactional.diag_cd_2, 
        transactional.diag_cd_3, transactional.diag_cd_4)
    AND transactional.rendr_provdr_npi_svc IS NOT NULL
    THEN transactional.rendr_provdr_txnmy_svc
    ELSE transactional.rendr_provdr_txnmy
    END,                                                          -- prov_rendering_std_taxonomy
    NULL,                                                         -- prov_rendering_vendor_specialty
    NULL,                                                         -- prov_billing_vendor_id
    transactional.billg_provdr_tax_id,                            -- prov_billing_tax_id
    NULL,                                                         -- prov_billing_dea_id
    NULL,                                                         -- prov_billing_ssn
    transactional.billg_provdr_stlc_nbr,                          -- prov_billing_state_license
    transactional.billg_provdr_upin,                              -- prov_billing_upin
    NULL,                                                         -- prov_billing_commercial_id
    transactional.billg_provdr_last_or_orgal_nm,                  -- prov_billing_name_1
    transactional.billg_provdr_first_nm,                          -- prov_billing_name_2
    transactional.billg_provdr_addr_1,                            -- prov_billing_address_1
    transactional.billg_provdr_addr_2,                            -- prov_billing_address_2
    transactional.billg_provdr_addr_city,                         -- prov_billing_city
    transactional.billg_provdr_addr_state,                        -- prov_billing_state
    transactional.billg_provdr_addr_zip,                          -- prov_billing_zip
    transactional.billg_provdr_txnmy,                             -- prov_billing_std_taxonomy
    NULL,                                                         -- prov_billing_vendor_specialty
    NULL,                                                         -- prov_referring_vendor_id
    NULL,                                                         -- prov_referring_tax_id
    NULL,                                                         -- prov_referring_dea_id
    NULL,                                                         -- prov_referring_ssn
    CASE
    WHEN transactional.claim_type_cd = 'I'
    OR diags.diag_code NOT IN (transactional.diag_cd_1, transactional.diag_cd_2,
        transactional.diag_cd_3, transactional.diag_cd_4)
    OR transactional.refrn_provdr_npi_svc IS NULL
    THEN transactional.refrn_provdr_stlc_nbr
    ELSE NULL
    END,                                                          -- prov_referring_state_license
    CASE
    WHEN transactional.claim_type_cd = 'I'
    OR diags.diag_code NOT IN (transactional.diag_cd_1, transactional.diag_cd_2,
        transactional.diag_cd_3, transactional.diag_cd_4)
    OR transactional.refrn_provdr_npi_svc IS NULL
    THEN transactional.refrn_provdr_upin
    ELSE NULL
    END,                                                          -- prov_referring_upin
    CASE
    WHEN transactional.claim_type_cd = 'I'
    OR diags.diag_code NOT IN (transactional.diag_cd_1, transactional.diag_cd_2,
        transactional.diag_cd_3, transactional.diag_cd_4)
    OR transactional.refrn_provdr_npi_svc IS NULL
    THEN transactional.refrn_provdr_comm_nbr
    ELSE NULL
    END,                                                          -- prov_referring_commercial_id
    CASE
    WHEN transactional.claim_type_cd = 'I'
    OR diags.diag_code NOT IN (transactional.diag_cd_1, transactional.diag_cd_2,
        transactional.diag_cd_3, transactional.diag_cd_4)
    OR transactional.refrn_provdr_npi_svc IS NULL
    THEN transactional.refrn_provdr_last_nm
    ELSE NULL
    END,                                                          -- prov_referring_name_1
    CASE
    WHEN transactional.claim_type_cd = 'I'
    OR diags.diag_code NOT IN (transactional.diag_cd_1, transactional.diag_cd_2,
        transactional.diag_cd_3, transactional.diag_cd_4)
    OR transactional.refrn_provdr_npi_svc IS NULL
    THEN transactional.refrn_provdr_first_nm
    ELSE NULL
    END,                                                          -- prov_referring_name_2
    NULL,                                                         -- prov_referring_address_1
    NULL,                                                         -- prov_referring_address_2
    NULL,                                                         -- prov_referring_city
    NULL,                                                         -- prov_referring_state
    NULL,                                                         -- prov_referring_zip
    NULL,                                                         -- prov_referring_std_taxonomy
    NULL,                                                         -- prov_referring_vendor_specialty
    NULL,                                                         -- prov_facility_vendor_id
    NULL,                                                         -- prov_facility_tax_id
    NULL,                                                         -- prov_facility_dea_id
    NULL,                                                         -- prov_facility_ssn
    CASE
    WHEN transactional.claim_type_cd = 'I'
    OR diags.diag_code NOT IN (transactional.diag_cd_1, transactional.diag_cd_2,
        transactional.diag_cd_3, transactional.diag_cd_4)
    OR transactional.fclty_npi_svc IS NULL
    THEN transactional.fclty_stlc_nbr
    ELSE NULL
    END,                                                          -- prov_facility_state_license
    NULL,                                                         -- prov_facility_upin
    CASE
    WHEN transactional.claim_type_cd = 'I'
    OR diags.diag_code NOT IN (transactional.diag_cd_1, transactional.diag_cd_2,
        transactional.diag_cd_3, transactional.diag_cd_4)
    OR transactional.fclty_npi_svc IS NULL
    THEN transactional.fclty_comm_nbr
    ELSE NULL
    END,                                                          -- prov_facility_commercial_id
    CASE
    WHEN transactional.claim_type_cd = 'I'
    OR diags.diag_code NOT IN (transactional.diag_cd_1, transactional.diag_cd_2,
        transactional.diag_cd_3, transactional.diag_cd_4)
    OR transactional.fclty_npi_svc IS NULL
    THEN transactional.fclty_nm
    ELSE NULL
    END,                                                          -- prov_facility_name_1
    NULL,                                                         -- prov_facility_name_2
    CASE
    WHEN transactional.claim_type_cd = 'I'
    OR diags.diag_code NOT IN (transactional.diag_cd_1, transactional.diag_cd_2,
        transactional.diag_cd_3, transactional.diag_cd_4)
    OR transactional.fclty_npi_svc IS NULL
    THEN REGEXP_REPLACE(transactional.fclty_addr_1, '"', '')
    ELSE NULL
    END,                                                          -- prov_facility_address_1
    CASE
    WHEN transactional.claim_type_cd = 'I'
    OR diags.diag_code NOT IN (transactional.diag_cd_1, transactional.diag_cd_2,
        transactional.diag_cd_3, transactional.diag_cd_4)
    OR transactional.fclty_npi_svc IS NULL
    THEN REGEXP_REPLACE(transactional.fclty_addr_2, '"', '')
    ELSE NULL
    END,                                                          -- prov_facility_address_2
    CASE
    WHEN transactional.claim_type_cd = 'I'
    OR diags.diag_code NOT IN (transactional.diag_cd_1, transactional.diag_cd_2,
        transactional.diag_cd_3, transactional.diag_cd_4)
    OR transactional.fclty_npi_svc IS NULL
    THEN REGEXP_REPLACE(transactional.fclty_addr_city, '"', '')
    ELSE NULL
    END,                                                          -- prov_facility_city
    CASE
    WHEN transactional.claim_type_cd = 'I'
    OR diags.diag_code NOT IN (transactional.diag_cd_1, transactional.diag_cd_2,
        transactional.diag_cd_3, transactional.diag_cd_4)
    OR transactional.fclty_npi_svc IS NULL
    THEN transactional.fclty_addr_state
    ELSE NULL
    END,                                                          -- prov_facility_state
    CASE
    WHEN transactional.claim_type_cd = 'I'
    OR diags.diag_code NOT IN (transactional.diag_cd_1, transactional.diag_cd_2,
        transactional.diag_cd_3, transactional.diag_cd_4)
    AND transactional.fclty_npi_svc IS NOT NULL
    THEN transactional.fclty_addr_zip
    ELSE NULL
    END,                                                          -- prov_facility_zip
    NULL,                                                         -- prov_facility_std_taxonomy
    NULL,                                                         -- prov_facility_vendor_specialty
    NULL,                                                         -- cob_payer_vendor_id_1
    NULL,                                                         -- cob_payer_seq_code_1
    NULL,                                                         -- cob_payer_hpid_1
    NULL,                                                         -- cob_payer_claim_filing_ind_code_1
    NULL,                                                         -- cob_ins_type_code_1
    NULL,                                                         -- cob_payer_vendor_id_2
    NULL,                                                         -- cob_payer_seq_code_2
    NULL,                                                         -- cob_payer_hpid_2
    NULL,                                                         -- cob_payer_claim_filing_ind_code_2
    NULL                                                          -- cob_ins_type_code_2
FROM transactional_raw transactional
    LEFT JOIN matching_payload mp ON transactional.src_claim_id = mp.claimid

    -- these inner joins will each perform a cartesian product on this table, exploding the table for each diag/proc
    INNER JOIN exploded_diag_codes diags ON CONCAT(transactional.src_claim_id, transactional.src_svc_id) = diags.claim_svc_num 
    INNER JOIN exploded_proc_codes procs ON CONCAT(transactional.src_claim_id, transactional.src_svc_id) = procs.claim_svc_num
    ;

-- Insert service lines for institutional claims with diagnoses (nulled out above)
INSERT INTO tmp
SELECT DISTINCT
    monotonically_increasing_id(),                                -- record_id
    transactional.src_claim_id,                                   -- claim_id
    mp.hvid,                                                      -- hvid
    {today},                                                      -- created
    '1',                                                          -- model_version
    {setid},                                                      -- data_set
    {feedname},                                                   -- data_feed
    {vendor},                                                     -- data_vendor
    '1',                                                          -- source_version
    mp.gender,                                                    -- patient_gender
    NULL,                                                         -- patient_age
    mp.yearOfBirth,                                               -- patient_year_of_birth
    mp.threeDigitZip,                                             -- patient_zip3
    UPPER(mp.state),                                              -- patient_state
    transactional.claim_type_cd,                                  -- claim_type
    extract_date(
        transactional.edi_interchange_creation_dt, '%Y-%m-%d'
        ),                                                        -- date_received
    CASE 
    WHEN extract_date(transactional.svc_from_dt, '%Y%m%d') IS NOT NULL 
    THEN extract_date(transactional.svc_from_dt, '%Y%m%d')
    WHEN extract_date(transactional.stmnt_from_dt, '%Y%m%d') IS NOT NULL 
    THEN extract_date(transactional.stmnt_from_dt, '%Y%m%d')
    ELSE (
    SELECT MIN(extract_date(t2.svc_from_dt, '%Y%m%d')) 
    FROM transactional_raw t2 
    WHERE t2.src_claim_id = transactional.src_claim_id
        )
    END,                                                          -- date_service
    CASE 
    WHEN extract_date(transactional.svc_from_dt, '%Y%m%d') IS NOT NULL 
    THEN extract_date(transactional.svc_to_dt, '%Y%m%d')
    WHEN extract_date(transactional.stmnt_from_dt, '%Y%m%d') IS NOT NULL
    THEN extract_date(transactional.stmnt_to_dt, '%Y%m%d')
    ELSE (
    SELECT MAX(extract_date(t2.svc_to_dt, '%Y%m%d'))
    FROM transactional_raw t2
    WHERE t2.src_claim_id = transactional.src_claim_id
        ) 
    END,                                                          -- date_service_end
    extract_date(transactional.admsn_dt, '%Y%m%d'),                     -- inst_date_admitted
    extract_date(transactional.dischg_dt, '%Y%m%d'),                    -- inst_date_discharged
    transactional.admsn_type_cd,                                  -- inst_admit_type_std_id
    NULL,                                                         -- inst_admit_type_vendor_id
    NULL,                                                         -- inst_admit_type_vendor_desc
    transactional.admsn_src_cd,                                   -- inst_admit_source_std_id
    NULL,                                                         -- inst_admit_source_vendor_id
    NULL,                                                         -- inst_admit_source_vendor_desc
    scrub_discharge_status(transactional.patnt_sts_cd),           -- inst_discharge_status_std_id
    NULL,                                                         -- inst_discharge_status_vendor_id
    NULL,                                                         -- inst_discharge_status_vendor_desc
    CONCAT(
        transactional.fclty_type_pos_cd,
        transactional.claim_freq_cd
        ),                                                        -- inst_type_of_bill_std_id
    NULL,                                                         -- inst_type_of_bill_vendor_id
    NULL,                                                         -- inst_type_of_bill_vendor_desc
    nullify_drg_blacklist(transactional.drg_cd),                  -- inst_drg_std_id
    NULL,                                                         -- inst_drg_vendor_id
    NULL,                                                         -- inst_drg_vendor_desc
    NULL,                                                         -- place_of_service_std_id
    NULL,                                                         -- place_of_service_vendor_id
    NULL,                                                         -- place_of_service_vendor_desc
    transactional.line_nbr,                                       -- service_line_number
    NULL,                                                         -- diagnosis_code
    NULL,                                                         -- diagnosis_code_qual
    NULL,                                                         -- diagnosis_priority
    NULL,                                                         -- admit_diagnosis_ind
    clean_up_procedure_code(procs.proc_code),                                                        -- procedure_code
    transactional.proc_cd_qual,                                   -- procedure_code_qual
    NULL,                                                         -- principal_proc_ind
    transactional.units,                                          -- procedure_units
    transactional.proc_modfr_1,                                   -- procedure_modifier_1
    transactional.proc_modfr_2,                                   -- procedure_modifier_2
    transactional.proc_modfr_3,                                   -- procedure_modifier_3
    transactional.proc_modfr_4,                                   -- procedure_modifier_4
    transactional.revnu_cd,                                       -- revenue_code
    transactional.ndc,                                            -- ndc_code
    transactional.dest_payer_claim_flng_ind_cd,                   -- medical_coverage_type
    extract_currency(transactional.line_charg),                     -- line_charge
    NULL,                                                         -- line_allowed
    extract_currency(transactional.tot_claim_charg_amt),            -- total_charge
    NULL,                                                         -- total_allowed
    CASE WHEN transactional.rendr_provdr_npi_svc IS NOT NULL
    AND transactional.rendr_provdr_npi_svc <> ''
    THEN transactional.rendr_provdr_npi_svc    ,
        transactional.rendr_provdr_npi
        ),                                                        -- prov_rendering_npi
    transactional.billg_provdr_npi,                               -- prov_billing_npi
    COALESCE(
        transactional.refrn_provdr_npi_svc,
        transactional.refrn_provdr_npi
        ),                                                        -- prov_referring_npi
    COALESCE(
        transactional.fclty_npi_svc,
        transactional.fclty_npi
        ),                                                        -- prov_facility_npi
    NULL,                                                         -- payer_vendor_id
    REGEXP_REPLACE(transactional.dest_payer_nm, '"', ''),         -- payer_name
    NULL,                                                         -- payer_parent_name
    NULL,                                                         -- payer_org_name
    NULL,                                                         -- payer_plan_id
    NULL,                                                         -- payer_plan_name
    NULL,                                                         -- payer_type
    NULL,                                                         -- prov_rendering_vendor_id
    NULL,                                                         -- prov_rendering_tax_id
    NULL,                                                         -- prov_rendering_dea_id
    NULL,                                                         -- prov_rendering_ssn
    CASE 
    WHEN transactional.rendr_provdr_npi_svc IS NULL
    THEN transactional.rendr_provdr_stlc_nbr
    ELSE NULL
    END,                                                          -- prov_rendering_state_license
    CASE
    WHEN transactional.rendr_provdr_npi_svc IS NULL
    THEN transactional.rendr_provdr_upin
    ELSE NULL
    END,                                                          -- prov_rendering_upin
    CASE
    WHEN transactional.rendr_provdr_npi_svc IS NULL
    THEN transactional.rendr_provdr_comm_nbr
    ELSE NULL
    END,                                                          -- prov_rendering_commercial_id
    CASE
    WHEN transactional.rendr_provdr_npi_svc IS NULL
    THEN transactional.rendr_provdr_last_nm
    ELSE NULL
    END,                                                          -- prov_rendering_name_1
    CASE
    WHEN transactional.rendr_provdr_npi_svc IS NULL
    THEN transactional.rendr_provdr_first_nm
    ELSE NULL
    END,                                                          -- prov_rendering_name_2
    NULL,                                                         -- prov_rendering_address_1
    NULL,                                                         -- prov_rendering_address_2
    NULL,                                                         -- prov_rendering_city
    NULL,                                                         -- prov_rendering_state
    NULL,                                                         -- prov_rendering_zip
    COALESCE(
        transactional.rendr_provdr_txnmy_svc,
        transactional.rendr_provdr_txnmy
        ),                                                        -- prov_rendering_std_taxonomy
    NULL,                                                         -- prov_rendering_vendor_specialty
    NULL,                                                         -- prov_billing_vendor_id
    transactional.billg_provdr_tax_id,                            -- prov_billing_tax_id
    NULL,                                                         -- prov_billing_dea_id
    NULL,                                                         -- prov_billing_ssn
    transactional.billg_provdr_stlc_nbr,                          -- prov_billing_state_license
    transactional.billg_provdr_upin,                              -- prov_billing_upin
    NULL,                                                         -- prov_billing_commercial_id
    transactional.billg_provdr_last_or_orgal_nm,                  -- prov_billing_name_1
    transactional.billg_provdr_first_nm,                          -- prov_billing_name_2
    transactional.billg_provdr_addr_1,                            -- prov_billing_address_1
    transactional.billg_provdr_addr_2,                            -- prov_billing_address_2
    transactional.billg_provdr_addr_city,                         -- prov_billing_city
    transactional.billg_provdr_addr_state,                        -- prov_billing_state
    transactional.billg_provdr_addr_zip,                          -- prov_billing_zip
    transactional.billg_provdr_txnmy,                             -- prov_billing_std_taxonomy
    NULL,                                                         -- prov_billing_vendor_specialty
    NULL,                                                         -- prov_referring_vendor_id
    NULL,                                                         -- prov_referring_tax_id
    NULL,                                                         -- prov_referring_dea_id
    NULL,                                                         -- prov_referring_ssn
    CASE
    WHEN transactional.refrn_provdr_npi_svc IS NULL
    THEN transactional.refrn_provdr_stlc_nbr
    ELSE NULL
    END,                                                          -- prov_referring_state_license
    CASE
    WHEN transactional.refrn_provdr_npi_svc IS NULL
    THEN transactional.refrn_provdr_upin
    ELSE NULL
    END,                                                          -- prov_referring_upin
    CASE
    WHEN transactional.refrn_provdr_npi_svc IS NULL
    THEN transactional.refrn_provdr_comm_nbr
    ELSE NULL
    END,                                                          -- prov_referring_commercial_id
    CASE
    WHEN transactional.refrn_provdr_npi_svc IS NULL
    THEN transactional.refrn_provdr_last_nm
    ELSE NULL
    END,                                                          -- prov_referring_name_1
    CASE
    WHEN transactional.refrn_provdr_npi_svc IS NULL
    THEN transactional.refrn_provdr_first_nm
    ELSE NULL
    END,                                                          -- prov_referring_name_2
    NULL,                                                         -- prov_referring_address_1
    NULL,                                                         -- prov_referring_address_2
    NULL,                                                         -- prov_referring_city
    NULL,                                                         -- prov_referring_state
    NULL,                                                         -- prov_referring_zip
    NULL,                                                         -- prov_referring_std_taxonomy
    NULL,                                                         -- prov_referring_vendor_specialty
    NULL,                                                         -- prov_facility_vendor_id
    NULL,                                                         -- prov_facility_tax_id
    NULL,                                                         -- prov_facility_dea_id
    NULL,                                                         -- prov_facility_ssn
    CASE
    WHEN transactional.fclty_npi_svc IS NULL
    THEN transactional.fclty_stlc_nbr
    ELSE NULL
    END,                                                          -- prov_facility_state_license
    NULL,                                                         -- prov_facility_upin
    CASE
    WHEN transactional.fclty_npi_svc IS NULL
    THEN transactional.fclty_comm_nbr
    ELSE NULL
    END,                                                          -- prov_facility_commercial_id
    CASE
    WHEN transactional.fclty_npi_svc IS NULL
    THEN transactional.fclty_nm
    ELSE NULL
    END,                                                          -- prov_facility_name_1
    NULL,                                                         -- prov_facility_name_2
    CASE
    WHEN transactional.fclty_npi_svc IS NULL
    THEN REGEXP_REPLACE(transactional.fclty_addr_1, '"', '')
    ELSE NULL
    END,                                                          -- prov_facility_address_1
    CASE
    WHEN transactional.fclty_npi_svc IS NULL
    THEN REGEXP_REPLACE(transactional.fclty_addr_2, '"', '')
    ELSE NULL
    END,                                                          -- prov_facility_address_2
    CASE
    WHEN transactional.fclty_npi_svc IS NULL
    THEN REGEXP_REPLACE(transactional.fclty_addr_city, '"', '')
    ELSE NULL
    END,                                                          -- prov_facility_city
    CASE
    WHEN transactional.fclty_npi_svc IS NULL
    THEN transactional.fclty_addr_state
    ELSE NULL
    END,                                                          -- prov_facility_state
    CASE
    WHEN transactional.fclty_npi_svc IS NULL
    THEN transactional.fclty_addr_zip
    ELSE NULL
    END,                                                          -- prov_facility_zip
    NULL,                                                         -- prov_facility_std_taxonomy
    NULL,                                                         -- prov_facility_vendor_specialty
    NULL,                                                         -- cob_payer_vendor_id_1
    NULL,                                                         -- cob_payer_seq_code_1
    NULL,                                                         -- cob_payer_hpid_1
    NULL,                                                         -- cob_payer_claim_filing_ind_code_1
    NULL,                                                         -- cob_ins_type_code_1
    NULL,                                                         -- cob_payer_vendor_id_2
    NULL,                                                         -- cob_payer_seq_code_2
    NULL,                                                         -- cob_payer_hpid_2
    NULL,                                                         -- cob_payer_claim_filing_ind_code_2
    NULL                                                          -- cob_ins_type_code_2
FROM transactional_raw transactional
    LEFT JOIN matching_payload mp ON transactional.src_claim_id = mp.claimid

    -- these inner joins will each perform a cartesian product on this table, exploding the table for each proc
    INNER JOIN exploded_proc_codes procs ON CONCAT(transactional.src_claim_id, transactional.src_svc_id) = procs.claim_svc_num
WHERE transactional.src_claim_id IN (
    SELECT DISTINCT claim_id 
    FROM tmp
    WHERE claim_type = 'I'
        AND diagnosis_code IS NOT NULL
        )
    ;

-- delete diagnosis codes that should not have been added
INSERT INTO medicalclaims_common_model
SELECT * 
FROM tmp base 
WHERE base.service_line_number IS NOT NULL
;

INSERT INTO medicalclaims_common_model
SELECT * 
FROM tmp base 
WHERE base.service_line_number IS NULL
    AND base.diagnosis_code NOT IN (
    SELECT sub.diagnosis_code
    FROM tmp sub
    WHERE sub.claim_id = base.claim_id
        AND sub.service_line_number IS NOT NULL
        )
