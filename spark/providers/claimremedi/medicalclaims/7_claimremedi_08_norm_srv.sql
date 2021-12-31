-- Insert service lines for institutional claims with diagnoses (NULLed out above)
SELECT DISTINCT
    transactional.src_claim_id                                                      AS claim_id,
    mp.hvid                                                                         AS hvid,
    CURRENT_DATE()                                                                  AS created,
    '1'                                                                             AS model_version,
    SPLIT(transactional.input_file_name, '/')[SIZE(SPLIT(transactional.input_file_name, '/')) - 1]
                                                                                    AS data_set,
    '264'                                                                            AS data_feed,
    '3'                                                                             AS data_vendor,
    '1'                                                                             AS source_version,
    mp.gender                                                                       AS patient_gender,
    CAST(NULL AS STRING)                                                            AS patient_age,
    CAP_YEAR_OF_BIRTH(
        NULL,
            CASE
            WHEN extract_date(transactional.svc_from_dt, '%Y%m%d',
                CAST('{EARLIEST_SERVICE_DATE}' as date), CAST('{VDR_FILE_DT}' as date)) IS NOT NULL
            THEN CAST(extract_date(transactional.svc_from_dt, '%Y%m%d',
                CAST('{EARLIEST_SERVICE_DATE}' as date), CAST('{VDR_FILE_DT}' as date)) AS DATE)
            WHEN extract_date(transactional.stmnt_from_dt, '%Y%m%d',
                CAST('{EARLIEST_SERVICE_DATE}' as date), CAST('{VDR_FILE_DT}' as date)) IS NOT NULL
            THEN CAST(extract_date(transactional.stmnt_from_dt, '%Y%m%d',
                CAST('{EARLIEST_SERVICE_DATE}' as date), CAST('{VDR_FILE_DT}' as date)) AS DATE)
            ELSE MIN(
            CAST(extract_date(transactional.svc_from_dt, '%Y%m%d',
            CAST('{EARLIEST_SERVICE_DATE}' as date), CAST('{VDR_FILE_DT}' as date)) AS DATE)
            ) OVER(PARTITION BY transactional.src_claim_id
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
            END,
        mp.yearOfBirth
        )                                                                           AS patient_year_of_birth,
    mp.threeDigitZip                                                                AS patient_zip3,
    UPPER(mp.state)                                                                 AS patient_state,
    transactional.claim_type_cd                                                     AS claim_type,
    EXTRACT_DATE(
        transactional.edi_interchange_creation_dt, '%Y-%m-%d', CAST('{EARLIEST_SERVICE_DATE}' as date), CAST('{VDR_FILE_DT}' as date)
        )                                                                           AS date_received,
    CASE
        WHEN extract_date(transactional.svc_from_dt, '%Y%m%d',
            CAST('{EARLIEST_SERVICE_DATE}' as date), CAST('{VDR_FILE_DT}' as date)) IS NOT NULL
        THEN CAST(extract_date(transactional.svc_from_dt, '%Y%m%d',
            CAST('{EARLIEST_SERVICE_DATE}' as date), CAST('{VDR_FILE_DT}' as date)) AS DATE)
        WHEN extract_date(transactional.stmnt_from_dt, '%Y%m%d',
            CAST('{EARLIEST_SERVICE_DATE}' as date), CAST('{VDR_FILE_DT}' as date)) IS NOT NULL
        THEN CAST(extract_date(transactional.stmnt_from_dt, '%Y%m%d',
            CAST('{EARLIEST_SERVICE_DATE}' as date), CAST('{VDR_FILE_DT}' as date)) AS DATE)
        ELSE MIN(
        CAST(extract_date(transactional.svc_from_dt, '%Y%m%d',
        CAST('{EARLIEST_SERVICE_DATE}' as date), CAST('{VDR_FILE_DT}' as date)) AS DATE)
        ) OVER (PARTITION BY transactional.src_claim_id
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
    END                                                                             AS date_service,
    CASE
        WHEN extract_date(transactional.svc_from_dt, '%Y%m%d', CAST('{EARLIEST_SERVICE_DATE}' as date), CAST('{VDR_FILE_DT}' as date)) IS NOT NULL
            THEN extract_date(transactional.svc_to_dt, '%Y%m%d', CAST('{EARLIEST_SERVICE_DATE}' as date), CAST('{VDR_FILE_DT}' as date))
        WHEN extract_date(transactional.stmnt_from_dt, '%Y%m%d', CAST('{EARLIEST_SERVICE_DATE}' as date), CAST('{VDR_FILE_DT}' as date)) IS NOT NULL
            THEN extract_date(transactional.stmnt_to_dt, '%Y%m%d', CAST('{EARLIEST_SERVICE_DATE}' as date), CAST('{VDR_FILE_DT}' as date))
        ELSE MAX(extract_date(transactional.svc_to_dt, '%Y%m%d', CAST('{EARLIEST_SERVICE_DATE}' as date), CAST('{VDR_FILE_DT}' as date)))
            OVER(PARTITION BY transactional.src_claim_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
    END                                                                             AS date_service_end,
    EXTRACT_DATE(
        transactional.admsn_dt, '%Y%m%d', CAST('{EARLIEST_SERVICE_DATE}' as date), CAST('{VDR_FILE_DT}' as date)
        )                                                                           AS inst_date_admitted,
    EXTRACT_DATE(
        transactional.dischg_dt, '%Y%m%d', CAST('{EARLIEST_SERVICE_DATE}' as date), CAST('{VDR_FILE_DT}' as date)
        )                                                                           AS inst_date_discharged,
    transactional.admsn_type_cd                                                     AS inst_admit_type_std_id,
    CAST(NULL AS STRING)                                                            AS inst_admit_type_vendor_id,
    CAST(NULL AS STRING)                                                            AS inst_admit_type_vendor_desc,
    transactional.admsn_src_cd                                                      AS inst_admit_source_std_id,
    CAST(NULL AS STRING)                                                            AS inst_admit_source_vendor_id,
    CAST(NULL AS STRING)                                                            AS inst_admit_source_vendor_desc,
    scrub_discharge_status(transactional.patnt_sts_cd)                              AS inst_discharge_status_std_id,
    CAST(NULL AS STRING)                                                            AS inst_discharge_status_vendor_id,
    CAST(NULL AS STRING)                                                            AS inst_discharge_status_vendor_desc,
    --    obscure_inst_type_of_bill(
    --        generate_inst_type_of_bill_std_id(
    --            transactional.fclty_type_pos_cd,
    --            transactional.claim_freq_cd
    --            )),                                            -- inst_type_of_bill_std_id
    (CAST(fclty_type_pos_cd AS STRING) + CAST(claim_freq_cd AS STRING))             AS inst_type_of_bill_std_id,
    CAST(NULL AS STRING)                                                            AS inst_type_of_bill_vendor_id,
    CAST(NULL AS STRING)                                                            AS inst_type_of_bill_vendor_desc,
    nullify_drg_blacklist(transactional.drg_cd)                                     AS inst_drg_std_id,
    CAST(NULL AS STRING)                                                            AS inst_drg_vendor_id,
    CAST(NULL AS STRING)                                                            AS inst_drg_vendor_desc,
    CAST(NULL AS STRING)                                                            AS place_of_service_std_id,
    CAST(NULL AS STRING)                                                            AS place_of_service_vendor_id,
    CAST(NULL AS STRING)                                                            AS place_of_service_vendor_desc,
    transactional.line_nbr                                                          AS service_line_number,
    CAST(NULL AS STRING)                                                            AS diagnosis_code,
    CAST(NULL AS STRING)                                                            AS diagnosis_code_qual,
    CAST(NULL AS STRING)                                                            AS diagnosis_priority,
    CAST(NULL AS STRING)                                                            AS admit_diagnosis_ind,
    clean_up_procedure_code(procs.proc_code)                                        AS procedure_code,
    transactional.proc_cd_qual                                                      AS procedure_code_qual,
    CAST(NULL AS STRING)                                                            AS principal_proc_ind,
    transactional.units                                                             AS procedure_units,
    transactional.proc_modfr_1                                                      AS procedure_modifier_1,
    transactional.proc_modfr_2                                                      AS procedure_modifier_2,
    transactional.proc_modfr_3                                                      AS procedure_modifier_3,
    transactional.proc_modfr_4                                                      AS procedure_modifier_4,
    transactional.revnu_cd                                                          AS revenue_code,
    transactional.ndc                                                               AS ndc_code,
    transactional.dest_payer_claim_flng_ind_cd                                      AS medical_coverage_type,
    extract_currency(transactional.line_charg)                                      AS line_charge,
    CAST(NULL AS STRING)                                                            AS line_allowed,
    extract_currency(transactional.tot_claim_charg_amt)                             AS total_charge,
    CAST(NULL AS STRING)                                                            AS total_allowed,
    CASE
        WHEN transactional.rendr_provdr_npi_svc IS NOT NULL AND TRIM(transactional.rendr_provdr_npi_svc) <> ''
            THEN transactional.rendr_provdr_npi_svc
        ELSE MIN(CASE WHEN TRIM(transactional.rendr_provdr_npi) = '' THEN NULL ELSE transactional.rendr_provdr_npi END)
            OVER(PARTITION BY transactional.src_claim_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
    END                                                                             AS prov_rendering_npi,
    transactional.billg_provdr_npi                                                  AS prov_billing_npi,
    CASE
        WHEN transactional.refrn_provdr_npi_svc IS NOT NULL AND TRIM(transactional.refrn_provdr_npi_svc) <> ''
            THEN transactional.refrn_provdr_npi_svc
        ELSE MIN(CASE WHEN TRIM(transactional.refrn_provdr_npi) = '' THEN NULL ELSE transactional.refrn_provdr_npi END)
            OVER(PARTITION BY transactional.src_claim_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
    END                                                                             AS prov_referring_npi,
    CASE
        WHEN transactional.fclty_npi_svc IS NOT NULL AND TRIM(transactional.fclty_npi_svc) <> ''
            THEN transactional.fclty_npi_svc
        ELSE MIN(CASE WHEN TRIM(transactional.fclty_npi) = '' THEN NULL ELSE transactional.fclty_npi END)
            OVER(PARTITION BY transactional.src_claim_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
    END                                                                             AS prov_facility_npi,
    CAST(NULL AS STRING)                                                            AS payer_vendor_id,
    REGEXP_REPLACE(transactional.dest_payer_nm, '"', '')                            AS payer_name,
    CAST(NULL AS STRING)                                                            AS payer_parent_name,
    CAST(NULL AS STRING)                                                            AS payer_org_name,
    CAST(NULL AS STRING)                                                            AS payer_plan_id,
    CAST(NULL AS STRING)                                                            AS payer_plan_name,
    CAST(NULL AS STRING)                                                            AS payer_type,
    CAST(NULL AS STRING)                                                            AS prov_rendering_vendor_id,
    CAST(NULL AS STRING)                                                            AS prov_rendering_tax_id,
    CAST(NULL AS STRING)                                                            AS prov_rendering_dea_id,
    CAST(NULL AS STRING)                                                            AS prov_rendering_ssn,
    CASE
        WHEN transactional.rendr_provdr_npi_svc IS NULL OR transactional.rendr_provdr_npi_svc = ''
            THEN MIN(CASE WHEN TRIM(transactional.rendr_provdr_stlc_nbr) = '' THEN NULL ELSE transactional.rendr_provdr_stlc_nbr END)
                OVER(PARTITION BY transactional.src_claim_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
        ELSE NULL
    END                                                                             AS prov_rendering_state_license,
    CASE
        WHEN transactional.rendr_provdr_npi_svc IS NULL OR transactional.rendr_provdr_npi_svc = ''
            THEN MIN(CASE WHEN TRIM(transactional.rendr_provdr_upin) = '' THEN NULL ELSE transactional.rendr_provdr_upin END)
                OVER(PARTITION BY transactional.src_claim_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
        ELSE NULL
    END                                                                             AS prov_rendering_upin,
    CASE
        WHEN transactional.rendr_provdr_npi_svc IS NULL OR transactional.rendr_provdr_npi_svc = ''
            THEN MIN(CASE WHEN TRIM(transactional.rendr_provdr_comm_nbr) = '' THEN NULL ELSE transactional.rendr_provdr_comm_nbr END)
                OVER(PARTITION BY transactional.src_claim_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
        ELSE NULL
    END                                                                             AS prov_rendering_commercial_id,
    CASE
        WHEN transactional.rendr_provdr_npi_svc IS NULL OR transactional.rendr_provdr_npi_svc = ''
            THEN MIN(CASE WHEN TRIM(transactional.rendr_provdr_last_nm) = '' THEN NULL ELSE transactional.rendr_provdr_last_nm END)
                OVER(PARTITION BY transactional.src_claim_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
        ELSE NULL
    END                                                                             AS prov_rendering_name_1,
    CASE
        WHEN transactional.rendr_provdr_npi_svc IS NULL OR transactional.rendr_provdr_npi_svc = ''
            THEN MIN(CASE WHEN TRIM(transactional.rendr_provdr_first_nm) = '' THEN NULL ELSE transactional.rendr_provdr_first_nm END)
            OVER(PARTITION BY transactional.src_claim_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
        ELSE NULL
    END                                                                             AS prov_rendering_name_2,
    CAST(NULL AS STRING)                                                            AS prov_rendering_address_1,
    CAST(NULL AS STRING)                                                            AS prov_rendering_address_2,
    CAST(NULL AS STRING)                                                            AS prov_rendering_city,
    CAST(NULL AS STRING)                                                            AS prov_rendering_state,
    CAST(NULL AS STRING)                                                            AS prov_rendering_zip,
    CASE
        WHEN transactional.rendr_provdr_txnmy_svc IS NOT NULL AND TRIM(transactional.rendr_provdr_txnmy_svc) <> ''
            THEN transactional.rendr_provdr_txnmy_svc
        ELSE transactional.rendr_provdr_txnmy
    END                                                                             AS prov_rendering_std_taxonomy,
    CAST(NULL AS STRING)                                                            AS prov_rendering_vendor_specialty,
    CAST(NULL AS STRING)                                                            AS prov_billing_vendor_id,
    transactional.billg_provdr_tax_id                                               AS prov_billing_tax_id,
    CAST(NULL AS STRING)                                                            AS prov_billing_dea_id,
    CAST(NULL AS STRING)                                                            AS prov_billing_ssn,
    transactional.billg_provdr_stlc_nbr                                             AS prov_billing_state_license,
    transactional.billg_provdr_upin                                                 AS prov_billing_upin,
    CAST(NULL AS STRING)                                                            AS prov_billing_commercial_id,
    transactional.billg_provdr_last_or_orgal_nm                                     AS prov_billing_name_1,
    transactional.billg_provdr_first_nm                                             AS prov_billing_name_2,
    transactional.billg_provdr_addr_1                                               AS prov_billing_address_1,
    transactional.billg_provdr_addr_2                                               AS prov_billing_address_2,
    transactional.billg_provdr_addr_city                                            AS prov_billing_city,
    transactional.billg_provdr_addr_state                                           AS prov_billing_state,
    transactional.billg_provdr_addr_zip                                             AS prov_billing_zip,
    transactional.billg_provdr_txnmy                                                AS prov_billing_std_taxonomy,
    CAST(NULL AS STRING)                                                            AS prov_billing_vendor_specialty,
    CAST(NULL AS STRING)                                                            AS prov_referring_vendor_id,
    CAST(NULL AS STRING)                                                            AS prov_referring_tax_id,
    CAST(NULL AS STRING)                                                            AS prov_referring_dea_id,
    CAST(NULL AS STRING)                                                            AS prov_referring_ssn,
    CASE
        WHEN transactional.refrn_provdr_npi_svc IS NULL OR transactional.refrn_provdr_npi_svc = ''
            THEN MIN(CASE WHEN TRIM(transactional.refrn_provdr_stlc_nbr) = '' THEN NULL ELSE transactional.refrn_provdr_stlc_nbr END)
                OVER(PARTITION BY transactional.src_claim_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
        ELSE NULL
    END                                                                             AS prov_referring_state_license,
    CASE
        WHEN transactional.refrn_provdr_npi_svc IS NULL OR transactional.refrn_provdr_npi_svc = ''
            THEN MIN(CASE WHEN TRIM(transactional.refrn_provdr_upin) = '' THEN NULL ELSE transactional.refrn_provdr_upin END)
                OVER(PARTITION BY transactional.src_claim_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
        ELSE NULL
    END                                                                             AS prov_referring_upin,
    CASE
        WHEN transactional.refrn_provdr_npi_svc IS NULL OR transactional.refrn_provdr_npi_svc = ''
            THEN MIN(CASE WHEN TRIM(transactional.refrn_provdr_comm_nbr) = '' THEN NULL ELSE transactional.refrn_provdr_comm_nbr END)
                OVER(PARTITION BY transactional.src_claim_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
        ELSE NULL
    END                                                                             AS prov_referring_commercial_id,
    CASE
        WHEN transactional.refrn_provdr_npi_svc IS NULL OR transactional.refrn_provdr_npi_svc = ''
            THEN MIN(CASE WHEN TRIM(transactional.refrn_provdr_last_nm) = '' THEN NULL ELSE transactional.refrn_provdr_last_nm END)
                OVER(PARTITION BY transactional.src_claim_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
        ELSE NULL
    END                                                                             AS prov_referring_name_1,
    CASE
        WHEN transactional.refrn_provdr_npi_svc IS NULL OR transactional.refrn_provdr_npi_svc = ''
            THEN MIN(CASE WHEN TRIM(transactional.refrn_provdr_first_nm) = '' THEN NULL ELSE transactional.refrn_provdr_first_nm END)
                OVER(PARTITION BY transactional.src_claim_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
        ELSE NULL
    END                                                                             AS prov_referring_name_2,
    CAST(NULL AS STRING)                                                            AS prov_referring_address_1,
    CAST(NULL AS STRING)                                                            AS prov_referring_address_2,
    CAST(NULL AS STRING)                                                            AS prov_referring_city,
    CAST(NULL AS STRING)                                                            AS prov_referring_state,
    CAST(NULL AS STRING)                                                            AS prov_referring_zip,
    CAST(NULL AS STRING)                                                            AS prov_referring_std_taxonomy,
    CAST(NULL AS STRING)                                                            AS prov_referring_vendor_specialty,
    CAST(NULL AS STRING)                                                            AS prov_facility_vendor_id,
    CAST(NULL AS STRING)                                                            AS prov_facility_tax_id,
    CAST(NULL AS STRING)                                                            AS prov_facility_dea_id,
    CAST(NULL AS STRING)                                                            AS prov_facility_ssn,
    CASE
        WHEN transactional.fclty_npi_svc IS NULL OR transactional.fclty_npi_svc = ''
            THEN MIN(CASE WHEN TRIM(transactional.fclty_stlc_nbr) = '' THEN NULL ELSE transactional.fclty_stlc_nbr END)
                OVER(PARTITION BY transactional.src_claim_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
        ELSE NULL
    END                                                                             AS prov_facility_state_license,
    CAST(NULL AS STRING)                                                            AS prov_facility_upin,
    CASE
        WHEN transactional.fclty_npi_svc IS NULL OR transactional.fclty_npi_svc = ''
            THEN MIN(CASE WHEN TRIM(transactional.fclty_comm_nbr) = '' THEN NULL ELSE transactional.fclty_comm_nbr END)
                OVER(PARTITION BY transactional.src_claim_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
        ELSE NULL
    END                                                                             AS prov_facility_commercial_id,
    CASE
        WHEN transactional.fclty_npi_svc IS NULL OR transactional.fclty_npi_svc = ''
            THEN MIN(CASE WHEN TRIM(transactional.fclty_nm) = '' THEN NULL ELSE transactional.fclty_nm END)
                OVER(PARTITION BY transactional.src_claim_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
        ELSE NULL
    END                                                                             AS prov_facility_name_1,
    CAST(NULL AS STRING)                                                            AS prov_facility_name_2,
    CASE
        WHEN transactional.fclty_npi_svc IS NULL OR transactional.fclty_npi_svc = ''
            THEN MIN(CASE WHEN TRIM(transactional.fclty_addr_1) = '' THEN NULL ELSE REGEXP_REPLACE(transactional.fclty_addr_1, '"', '') END)
                OVER(PARTITION BY transactional.src_claim_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
        ELSE NULL
    END                                                                             AS prov_facility_address_1,
    CASE
        WHEN transactional.fclty_npi_svc IS NULL OR transactional.fclty_npi_svc = ''
            THEN MIN(CASE WHEN TRIM(transactional.fclty_addr_2) = '' THEN NULL ELSE REGEXP_REPLACE(transactional.fclty_addr_2, '"', '') END)
                OVER(PARTITION BY transactional.src_claim_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
        ELSE NULL
    END                                                                             AS prov_facility_address_2,
    CASE
        WHEN transactional.fclty_npi_svc IS NULL OR transactional.fclty_npi_svc = ''
            THEN MIN(CASE WHEN TRIM(transactional.fclty_addr_city) = '' THEN NULL ELSE REGEXP_REPLACE(transactional.fclty_addr_city, '"', '') END)
                OVER(PARTITION BY transactional.src_claim_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
        ELSE NULL
    END                                                                             AS prov_facility_city,
    CASE
        WHEN transactional.fclty_npi_svc IS NULL OR transactional.fclty_npi_svc = ''
            THEN MIN(CASE WHEN TRIM(transactional.fclty_addr_state) = '' THEN NULL ELSE transactional.fclty_addr_state END)
                OVER(PARTITION BY transactional.src_claim_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
        ELSE NULL
    END                                                                             AS prov_facility_state,
    CASE
        WHEN transactional.fclty_npi_svc IS NULL OR transactional.fclty_npi_svc = ''
            THEN MIN(CASE WHEN TRIM(transactional.fclty_addr_zip) = '' THEN NULL ELSE transactional.fclty_addr_zip END)
                OVER(PARTITION BY transactional.src_claim_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
        ELSE NULL
    END                                                                             AS prov_facility_zip,
    CAST(NULL AS STRING)                                                            AS prov_facility_std_taxonomy,
    CAST(NULL AS STRING)                                                            AS prov_facility_vendor_specialty,
    CAST(NULL AS STRING)                                                            AS cob_payer_vendor_id_1,
    CAST(NULL AS STRING)                                                            AS cob_payer_seq_code_1,
    CAST(NULL AS STRING)                                                            AS cob_payer_hpid_1,
    CAST(NULL AS STRING)                                                            AS cob_payer_claim_filing_ind_code_1,
    CAST(NULL AS STRING)                                                            AS cob_ins_type_code_1,
    CAST(NULL AS STRING)                                                            AS cob_payer_vendor_id_2,
    CAST(NULL AS STRING)                                                            AS cob_payer_seq_code_2,
    CAST(NULL AS STRING)                                                            AS cob_payer_hpid_2,
    CAST(NULL AS STRING)                                                            AS cob_payer_claim_filing_ind_code_2,
    CAST(NULL AS STRING)                                                            AS cob_ins_type_code_2,
    'claimremedi'                                                              AS part_provider
FROM txn transactional
    LEFT JOIN matching_payload mp ON transactional.src_claim_id = mp.claimid
-- these inner joins will each perform a cartesian product on this table, exploding the table for each proc
    INNER JOIN claimremedi_05_clm_proc procs ON CONCAT(COALESCE(transactional.src_claim_id, ''), '__', COALESCE(transactional.src_svc_id, '')) = procs.claim_svc_num
WHERE transactional.claim_type_cd = 'I'
