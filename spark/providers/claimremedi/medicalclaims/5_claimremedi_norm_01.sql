SELECT
    txn.src_claim_id                                                       AS claim_id,
    pay.hvid                                                               AS hvid,
    CURRENT_DATE()                                                         AS created,
    '10'                                                                   AS model_version,
    SPLIT(txn.input_file_name, '/')[SIZE(SPLIT(txn.input_file_name, '/')) - 1]             AS data_set,
    264                                                                    AS data_feed,
    3                                                                     AS data_vendor,
    CASE
      WHEN UPPER(pay.gender) IN ('F', 'M','U') THEN UPPER(pay.gender)
      WHEN pay.gender IS NOT NULL  THEN 'U'
    END                                                                    AS patient_gender,
    -- UDF-cap_year_of_birth(age, date_service, year_of_birth)
    CAP_YEAR_OF_BIRTH
    (
    NULL,
    TO_DATE(COALESCE(txn.svc_from_dt, txn.stmnt_from_dt), 'yyyyMMdd'),
    pay.yearofbirth
    )                                                                      AS patient_year_of_birth,
    MASK_ZIP_CODE(pay.threeDigitZip)                                       AS patient_zip3,
    VALIDATE_STATE_CODE(COALESCE(UPPER(pay.state),''))                     AS patient_state,
    txn.claim_type_cd                                                      AS claim_type,
    CAST(txn.edi_interchange_creation_dt AS DATE)                          AS date_received,

    CASE
        WHEN CAST(EXTRACT_DATE(COALESCE(txn.svc_from_dt, txn.stmnt_from_dt), '%Y%m%d') AS DATE) < CAST('{EARLIEST_SERVICE_DATE}' AS DATE)
          OR CAST(EXTRACT_DATE(COALESCE(txn.svc_from_dt, txn.stmnt_from_dt), '%Y%m%d') AS DATE) > CAST('{VDR_FILE_DT}'                 AS DATE) THEN  NULL
    ELSE CAST(EXTRACT_DATE(COALESCE(txn.svc_from_dt, txn.stmnt_from_dt), '%Y%m%d') AS DATE)
    END                                                                    AS date_service,
    CASE
        WHEN CAST(EXTRACT_DATE(COALESCE(txn.svc_to_dt, txn.stmnt_to_dt), '%Y%m%d') AS DATE) < CAST('{EARLIEST_SERVICE_DATE}' AS DATE)
          OR CAST(EXTRACT_DATE(COALESCE(txn.svc_to_dt, txn.stmnt_to_dt), '%Y%m%d') AS DATE) > CAST('{VDR_FILE_DT}'                 AS DATE) THEN  NULL
        ELSE CAST(EXTRACT_DATE(COALESCE(txn.svc_to_dt, txn.stmnt_to_dt), '%Y%m%d') AS DATE)
    END                                                                    AS date_service_end,
    CASE
        WHEN CAST(EXTRACT_DATE(txn.admsn_dt, '%Y%m%d') AS DATE) < CAST('{EARLIEST_SERVICE_DATE}' AS DATE)
          OR CAST(EXTRACT_DATE(txn.admsn_dt, '%Y%m%d') AS DATE) > CAST('{VDR_FILE_DT}'                 AS DATE) THEN  NULL
        ELSE CAST(EXTRACT_DATE(txn.admsn_dt, '%Y%m%d') AS DATE)
    END                                                                    AS inst_date_admitted,
    CASE
        WHEN CAST(EXTRACT_DATE(txn.dischg_dt, '%Y%m%d') AS DATE) < CAST('{EARLIEST_SERVICE_DATE}' AS DATE)
          OR CAST(EXTRACT_DATE(txn.dischg_dt, '%Y%m%d') AS DATE) > CAST('{VDR_FILE_DT}'                 AS DATE) THEN  NULL
        ELSE CAST(EXTRACT_DATE(txn.dischg_dt, '%Y%m%d') AS DATE)
    END                                                                    AS inst_date_discharged,
    -------------------------------------------------------------------------------------------------------------
    ---------------------- inst_admit_type_std_id (Redact 4 - newborn)
    -------------------------------------------------------------------------------------------------------------
    CASE
      WHEN LPAD(txn.admsn_src_cd, 2, '0') = '04' THEN '9'
      ELSE txn.admsn_src_cd
    END                                                                    AS  inst_admit_type_std_id,
    -------------------------------------------------------------------------------------------------------------
    ---------------------- inst_admit_source_std_id (Redact 8 - Court or law enforcement)
    -------------------------------------------------------------------------------------------------------------
    CASE
      WHEN LPAD(txn.admsn_type_cd, 2, '0') = '08' THEN '9'
      ELSE txn.admsn_type_cd
    END                                                                    AS  inst_admit_source_std_id,
    -------------------------------------------------------------------------------------------------------------
    ---------------------- inst_discharge_status_std_id (Redact '20', '21', '40', '41', '42', '69', or '87' )
    -------------------------------------------------------------------------------------------------------------
    CASE
      WHEN COALESCE(txn.patnt_sts_cd, '20') IN ( '20', '21', '40', '41', '42', '69', '87'  ) THEN NULL
      ELSE txn.patnt_sts_cd
    END                                                                    AS  inst_discharge_status_std_id,
    -------------------------------------------------------------------------------------------------------------
    ---------------------- inst_type_of_bill_std_id
    -------------------------------------------------------------------------------------------------------------
    CASE
      WHEN COALESCE(txn.claim_type_cd, '') <> 'I'  THEN NULL
      --WHEN   SUBSTR(txn.fclty_type_pos_cd, 1, 1) = '3' THEN CONCAT('X', SUBSTR(txn.fclty_type_pos_cd, 2), COALESCE(txn.claim_freq_cd, ''))
      ELSE CONCAT(COALESCE(txn.fclty_type_pos_cd, ''), COALESCE(txn.claim_freq_cd, ''))
    END                                                                    AS  inst_type_of_bill_std_id,
    -------------------------------------------------------------------------------------------------------------
    ---------------------- inst_drg_std_id -- Replace HI*DR:
    -------------------------------------------------------------------------------------------------------------
    NULLIFY_DRG_BLACKLIST(REPLACE(txn.drg_cd, 'HI*DR:', ''))	           AS inst_drg_std_id,
    -------------------------------------------------------------------------------------------------------------
    ---------------------- place_of_service_std_id
    -------------------------------------------------------------------------------------------------------------
    CASE
        WHEN UPPER(COALESCE(txn.claim_type_cd, 'X')) <> 'P'                                           THEN NULL
        WHEN LPAD(txn.pos_cd, 2, '0') IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99') THEN '99'
        WHEN  COALESCE(txn.claim_type_cd, 'X')  =  'P'  AND txn.pos_cd IS NOT NULL                    THEN LPAD(txn.pos_cd, 2, '0')
    END                                                                    AS place_of_service_std_id,
   txn.line_nbr                                                            AS service_line_number,
   txn.src_svc_id                                                          AS service_line_id,
   CLEAN_UP_DIAGNOSIS_CODE(pvtdiag.diag, '02', NULL)                       AS diagnosis_code,
   '02'                                                                    AS diagnosis_code_qual,
   pvtdiag.diag_priority                                                   AS diagnosis_priority,
    -------------------------------------------------------------------------------------------------------------
    ---------------------- admit_diagnosis_ind
    -------------------------------------------------------------------------------------------------------------
  CASE
        WHEN COALESCE(txn.claim_type_cd, 'X') <> 'I'                                       THEN NULL
        WHEN COALESCE(txn.claim_type_cd, 'X') = 'I' AND txn.admtg_diag_cd IS NULL          THEN  'N'
        WHEN COALESCE(txn.claim_type_cd, 'X') = 'I' AND
        CLEAN_UP_DIAGNOSIS_CODE(txn.admtg_diag_cd, '02', NULL) = CLEAN_UP_DIAGNOSIS_CODE(pvtdiag.diag, '02', NULL)      THEN  'Y'
    ELSE NULL
    END                                                                    AS admit_diagnosis_ind,
    -------------------------------------------------------------------------------------------------------------
    ---------------------- procedure_code and qualifier
    -------------------------------------------------------------------------------------------------------------
    CLEAN_UP_PROCEDURE_CODE(COALESCE(pvtproc.proc_cd, pvtproc.proc_other)) AS procedure_code,
    CASE
        WHEN CLEAN_UP_PROCEDURE_CODE(COALESCE(pvtproc.proc_cd, pvtproc.proc_other)) IS NOT NULL THEN txn.proc_cd_qual
    ELSE NULL
    END                                                                    AS procedure_code_qual,

    CASE
        WHEN UPPER(COALESCE(txn.claim_type_cd, 'X')) <> 'I'                                    THEN NULL
        WHEN UPPER(COALESCE(txn.claim_type_cd, 'X')) =  'I' AND pvtproc.prinpl_proc_cd IS NULL THEN 'N'
        WHEN CLEAN_UP_PROCEDURE_CODE(COALESCE(pvtproc.proc_cd, pvtproc.proc_other, 'BLANK')) =  COALESCE(pvtproc.prinpl_proc_cd,'EMPTY') THEN 'Y'
        ELSE 'N'
    END                                                                    AS principal_proc_ind,
    CAST(txn.units AS FLOAT)                                               AS procedure_units_billed,
    txn.proc_modfr_1	                                                   AS procedure_modifier_1,
    txn.proc_modfr_2	                                                   AS procedure_modifier_2,
    txn.proc_modfr_3	                                                   AS procedure_modifier_3,
    txn.proc_modfr_4	                                                   AS procedure_modifier_4,
    txn.revnu_cd	                                                       AS revenue_code,
    CLEAN_UP_NDC_CODE(txn.ndc)                                             AS ndc_code,
    txn.dest_payer_claim_flng_ind_cd	                                   AS medical_coverage_type,
    CAST(txn.line_charg AS FLOAT)	                                       AS line_charge,
    CAST(txn.tot_claim_charg_amt AS FLOAT)	                               AS total_charge,
    -------------------------------------------------------------------------------------------------------------
    ---------------------- prov_rendering_npi
    -------------------------------------------------------------------------------------------------------------
    CASE
        WHEN COALESCE(txn.claim_type_cd, 'X') NOT IN ('I', 'P') THEN NULL
        WHEN COALESCE(txn.claim_type_cd, 'X') = 'P' AND LPAD(txn.pos_cd, 2, '0' )  IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99') THEN NULL
        --WHEN COALESCE(txn.claim_type_cd, 'X') = 'I' AND SUBSTR(COALESCE(fclty_type_pos_cd, ''), 1, 1) = '3' THEN NULL
        ELSE CLEAN_UP_NPI_CODE(COALESCE(txn.rendr_provdr_npi, txn.rendr_provdr_npi_2, txn.billg_provdr_npi))
    END                                                                    AS prov_rendering_npi,
    -------------------------------------------------------------------------------------------------------------
    ---------------------- prov_referring_npi (Refer to DA 708)
    -------------------------------------------------------------------------------------------------------------
    CASE
        WHEN COALESCE(txn.claim_type_cd, 'X') NOT IN ('I', 'P') THEN NULL
        WHEN COALESCE(txn.claim_type_cd, 'X') = 'P' AND LPAD(txn.pos_cd, 2, '0' )  IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99') THEN NULL
        --WHEN COALESCE(txn.claim_type_cd, 'X') = 'I' AND SUBSTR(COALESCE(fclty_type_pos_cd, ''), 1, 1) = '3' THEN NULL
        ELSE CLEAN_UP_NPI_CODE(COALESCE(txn.refrn_provdr_npi, txn.refrn_provdr_npi_1))
    END                                                                    AS prov_referring_npi,
    -------------------------------------------------------------------------------------------------------------
    ---------------------- prov_facility_npi
    -------------------------------------------------------------------------------------------------------------
    CASE
        WHEN COALESCE(txn.claim_type_cd, 'X') NOT IN ('I', 'P') THEN NULL
        WHEN COALESCE(txn.claim_type_cd, 'X') = 'P' AND LPAD(txn.pos_cd, 2, '0' )  IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99') THEN NULL
        --WHEN COALESCE(txn.claim_type_cd, 'X') = 'I' AND SUBSTR(COALESCE(fclty_type_pos_cd, ''), 1, 1) = '3' THEN NULL
        ELSE CLEAN_UP_NPI_CODE(COALESCE(txn.fclty_npi, txn.fclty_npi_2))
    END                                                                    AS prov_facility_npi,
    txn.dest_payer_nm                                                      AS payer_name,
    txn.dest_payer_cms_plan_id                                             AS payer_plan_id,
    -------------------------------------------------------------------------------------------------------------
    ---------------------- prov_rendering_state_license
    -------------------------------------------------------------------------------------------------------------
    CASE
        WHEN COALESCE(txn.claim_type_cd, 'X') NOT IN ('I', 'P') THEN NULL
        WHEN COALESCE(txn.claim_type_cd, 'X') = 'P' AND LPAD(txn.pos_cd, 2, '0' )  IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99') THEN NULL
        --WHEN COALESCE(txn.claim_type_cd, 'X') = 'I' AND SUBSTR(COALESCE(fclty_type_pos_cd, ''), 1, 1) = '3' THEN NULL
        ELSE txn.rendr_provdr_stlc_nbr
    END                                                                    AS prov_rendering_state_license,
    -------------------------------------------------------------------------------------------------------------
    ---------------------- prov_rendering_upin
    -------------------------------------------------------------------------------------------------------------
    CASE
        WHEN COALESCE(txn.claim_type_cd, 'X') NOT IN ('I', 'P') THEN NULL
        WHEN COALESCE(txn.claim_type_cd, 'X') = 'P' AND LPAD(txn.pos_cd, 2, '0' )  IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99') THEN NULL
        --WHEN COALESCE(txn.claim_type_cd, 'X') = 'I' AND SUBSTR(COALESCE(fclty_type_pos_cd, ''), 1, 1) = '3' THEN NULL
        ELSE txn.rendr_provdr_upin
    END                                                                    AS prov_rendering_upin,
    -------------------------------------------------------------------------------------------------------------
    ---------------------- prov_rendering_commercial_id
    -------------------------------------------------------------------------------------------------------------
    CASE
        WHEN COALESCE(txn.claim_type_cd, 'X') NOT IN ('I', 'P') THEN NULL
        WHEN COALESCE(txn.claim_type_cd, 'X') = 'P' AND LPAD(txn.pos_cd, 2, '0' )  IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99') THEN NULL
        --WHEN COALESCE(txn.claim_type_cd, 'X') = 'I' AND SUBSTR(COALESCE(fclty_type_pos_cd, ''), 1, 1) = '3' THEN NULL
        ELSE txn.rendr_provdr_comm_nbr
    END                                                                    AS prov_rendering_commercial_id,
    -------------------------------------------------------------------------------------------------------------
    ---------------------- prov_rendering_name_1
    -------------------------------------------------------------------------------------------------------------
    CASE
        WHEN COALESCE(txn.claim_type_cd, 'X') NOT IN ('I', 'P') THEN NULL
        WHEN COALESCE(txn.claim_type_cd, 'X') = 'P' AND LPAD(txn.pos_cd, 2, '0' )  IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99') THEN NULL
        --WHEN COALESCE(txn.claim_type_cd, 'X') = 'I' AND SUBSTR(COALESCE(fclty_type_pos_cd, ''), 1, 1) = '3' THEN NULL
        ELSE txn.rendr_provdr_last_nm
    END                                                                    AS prov_rendering_name_1,
    -------------------------------------------------------------------------------------------------------------
    ---------------------- prov_rendering_name_2
    -------------------------------------------------------------------------------------------------------------
    CASE
        WHEN COALESCE(txn.claim_type_cd, 'X') NOT IN ('I', 'P') THEN NULL
        WHEN COALESCE(txn.claim_type_cd, 'X') = 'P' AND LPAD(txn.pos_cd, 2, '0' )  IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99') THEN NULL
        --WHEN COALESCE(txn.claim_type_cd, 'X') = 'I' AND SUBSTR(COALESCE(fclty_type_pos_cd, ''), 1, 1) = '3' THEN NULL
        ELSE txn.rendr_provdr_first_nm
    END                                                                    AS prov_rendering_name_2,
    txn.rendr_provdr_txnmy                                                 AS prov_rendering_std_taxonomy,
    -------------------------------------------------------------------------------------------------------------
    ---------------------- prov_billing_tax_id
    -------------------------------------------------------------------------------------------------------------
    CASE
        WHEN COALESCE(txn.claim_type_cd, 'X') NOT IN ('I', 'P') THEN NULL
        WHEN COALESCE(txn.claim_type_cd, 'X') = 'P' AND LPAD(txn.pos_cd, 2, '0' )  IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99') THEN NULL
        --WHEN COALESCE(txn.claim_type_cd, 'X') = 'I' AND SUBSTR(COALESCE(fclty_type_pos_cd, ''), 1, 1) = '3' THEN NULL
        ELSE txn.billg_provdr_tax_id
    END                                                                    AS prov_billing_tax_id,
    -------------------------------------------------------------------------------------------------------------
    ---------------------- prov_billing_state_license
    -------------------------------------------------------------------------------------------------------------
    CASE
        WHEN COALESCE(txn.claim_type_cd, 'X') NOT IN ('I', 'P') THEN NULL
        WHEN COALESCE(txn.claim_type_cd, 'X') = 'P' AND LPAD(txn.pos_cd, 2, '0' )  IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99') THEN NULL
        --WHEN COALESCE(txn.claim_type_cd, 'X') = 'I' AND SUBSTR(COALESCE(fclty_type_pos_cd, ''), 1, 1) = '3' THEN NULL
        ELSE txn.billg_provdr_stlc_nbr
    END                                                                    AS prov_billing_state_license,
    -------------------------------------------------------------------------------------------------------------
    ---------------------- prov_billing_upin
    -------------------------------------------------------------------------------------------------------------
    CASE
        WHEN COALESCE(txn.claim_type_cd, 'X') NOT IN ('I', 'P') THEN NULL
        WHEN COALESCE(txn.claim_type_cd, 'X') = 'P' AND LPAD(txn.pos_cd, 2, '0' )  IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99') THEN NULL
        --WHEN COALESCE(txn.claim_type_cd, 'X') = 'I' AND SUBSTR(COALESCE(fclty_type_pos_cd, ''), 1, 1) = '3' THEN NULL
        ELSE txn.billg_provdr_upin
    END                                                                    AS prov_billing_upin,
    -------------------------------------------------------------------------------------------------------------
    ---------------------- prov_billing_name_1
    -------------------------------------------------------------------------------------------------------------
    CASE
        WHEN COALESCE(txn.claim_type_cd, 'X') NOT IN ('I', 'P') THEN NULL
        WHEN COALESCE(txn.claim_type_cd, 'X') = 'P' AND LPAD(txn.pos_cd, 2, '0' )  IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99') THEN NULL
        --WHEN COALESCE(txn.claim_type_cd, 'X') = 'I' AND SUBSTR(COALESCE(fclty_type_pos_cd, ''), 1, 1) = '3' THEN NULL
        ELSE txn.billg_provdr_last_or_orgal_nm
    END                                                                    AS prov_billing_name_1,
    -------------------------------------------------------------------------------------------------------------
    ---------------------- prov_billing_name_2
    -------------------------------------------------------------------------------------------------------------
    CASE
        WHEN COALESCE(txn.claim_type_cd, 'X') NOT IN ('I', 'P') THEN NULL
        WHEN COALESCE(txn.claim_type_cd, 'X') = 'P' AND LPAD(txn.pos_cd, 2, '0' )  IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99') THEN NULL
        --WHEN COALESCE(txn.claim_type_cd, 'X') = 'I' AND SUBSTR(COALESCE(fclty_type_pos_cd, ''), 1, 1) = '3' THEN NULL
        ELSE txn.billg_provdr_first_nm
    END                                                                    AS prov_billing_name_2,
    -------------------------------------------------------------------------------------------------------------
    ---------------------- prov_billing_address_1
    -------------------------------------------------------------------------------------------------------------
    CASE
        WHEN COALESCE(txn.claim_type_cd, 'X') NOT IN ('I', 'P') THEN NULL
        WHEN COALESCE(txn.claim_type_cd, 'X') = 'P' AND LPAD(txn.pos_cd, 2, '0' )  IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99') THEN NULL
        --WHEN COALESCE(txn.claim_type_cd, 'X') = 'I' AND SUBSTR(COALESCE(fclty_type_pos_cd, ''), 1, 1) = '3' THEN NULL
        ELSE txn.billg_provdr_addr_1
    END                                                                    AS prov_billing_address_1,
    -------------------------------------------------------------------------------------------------------------
    ---------------------- prov_billing_address_2 prov_billing_city
    -------------------------------------------------------------------------------------------------------------
    CASE
        WHEN COALESCE(txn.claim_type_cd, 'X') NOT IN ('I', 'P') THEN NULL
        WHEN COALESCE(txn.claim_type_cd, 'X') = 'P' AND LPAD(txn.pos_cd, 2, '0' )  IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99') THEN NULL
        --WHEN COALESCE(txn.claim_type_cd, 'X') = 'I' AND SUBSTR(COALESCE(fclty_type_pos_cd, ''), 1, 1) = '3' THEN NULL
        ELSE txn.billg_provdr_addr_2
    END                                                                    AS prov_billing_address_2,
    -------------------------------------------------------------------------------------------------------------
    ----------------------  prov_billing_city
    -------------------------------------------------------------------------------------------------------------
    CASE
        WHEN COALESCE(txn.claim_type_cd, 'X') NOT IN ('I', 'P') THEN NULL
        WHEN COALESCE(txn.claim_type_cd, 'X') = 'P' AND LPAD(txn.pos_cd, 2, '0' )  IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99') THEN NULL
        --WHEN COALESCE(txn.claim_type_cd, 'X') = 'I' AND SUBSTR(COALESCE(fclty_type_pos_cd, ''), 1, 1) = '3' THEN NULL
        ELSE txn.billg_provdr_addr_city
    END                                                                    AS prov_billing_city,
    -------------------------------------------------------------------------------------------------------------
    ----------------------  prov_billing_state
    -------------------------------------------------------------------------------------------------------------
    CASE
        WHEN COALESCE(txn.claim_type_cd, 'X') NOT IN ('I', 'P') THEN NULL
        WHEN COALESCE(txn.claim_type_cd, 'X') = 'P' AND LPAD(txn.pos_cd, 2, '0' )  IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99') THEN NULL
        --WHEN COALESCE(txn.claim_type_cd, 'X') = 'I' AND SUBSTR(COALESCE(fclty_type_pos_cd, ''), 1, 1) = '3' THEN NULL
        ELSE txn.billg_provdr_addr_state
    END                                                                    AS prov_billing_state,
    -------------------------------------------------------------------------------------------------------------
    ----------------------  prov_billing_zip
    -------------------------------------------------------------------------------------------------------------
    CASE
        WHEN COALESCE(txn.claim_type_cd, 'X') NOT IN ('I', 'P') THEN NULL
        WHEN COALESCE(txn.claim_type_cd, 'X') = 'P' AND LPAD(txn.pos_cd, 2, '0' )  IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99') THEN NULL
        --WHEN COALESCE(txn.claim_type_cd, 'X') = 'I' AND SUBSTR(COALESCE(fclty_type_pos_cd, ''), 1, 1) = '3' THEN NULL
        ELSE txn.billg_provdr_addr_zip
    END                                                                    AS prov_billing_zip,
    txn.billg_provdr_txnmy	                                               AS prov_billing_std_taxonomy,
    -------------------------------------------------------------------------------------------------------------
    ----------------------  prov_referring_state_license
    -------------------------------------------------------------------------------------------------------------
    CASE
        WHEN COALESCE(txn.claim_type_cd, 'X') NOT IN ('I', 'P') THEN NULL
        WHEN COALESCE(txn.claim_type_cd, 'X') = 'P' AND LPAD(txn.pos_cd, 2, '0' )  IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99') THEN NULL
        --WHEN COALESCE(txn.claim_type_cd, 'X') = 'I' AND SUBSTR(COALESCE(fclty_type_pos_cd, ''), 1, 1) = '3' THEN NULL
        ELSE txn.refrn_provdr_stlc_nbr
    END                                                                    AS prov_referring_state_license,
    -------------------------------------------------------------------------------------------------------------
    ----------------------  prov_referring_upin
    -------------------------------------------------------------------------------------------------------------
    CASE
        WHEN COALESCE(txn.claim_type_cd, 'X') NOT IN ('I', 'P') THEN NULL
        WHEN COALESCE(txn.claim_type_cd, 'X') = 'P' AND LPAD(txn.pos_cd, 2, '0' )  IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99') THEN NULL
        --WHEN COALESCE(txn.claim_type_cd, 'X') = 'I' AND SUBSTR(COALESCE(fclty_type_pos_cd, ''), 1, 1) = '3' THEN NULL
        ELSE txn.refrn_provdr_upin
    END                                                                    AS prov_referring_upin,
    -------------------------------------------------------------------------------------------------------------
    ----------------------  prov_referring_commercial_id
    -------------------------------------------------------------------------------------------------------------
    CASE
        WHEN COALESCE(txn.claim_type_cd, 'X') NOT IN ('I', 'P') THEN NULL
        WHEN COALESCE(txn.claim_type_cd, 'X') = 'P' AND LPAD(txn.pos_cd, 2, '0' )  IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99') THEN NULL
        --WHEN COALESCE(txn.claim_type_cd, 'X') = 'I' AND SUBSTR(COALESCE(fclty_type_pos_cd, ''), 1, 1) = '3' THEN NULL
        ELSE txn.refrn_provdr_comm_nbr
    END                                                                    AS prov_referring_commercial_id,
    -------------------------------------------------------------------------------------------------------------
    ----------------------  prov_referring_name_1
    -------------------------------------------------------------------------------------------------------------
    CASE
        WHEN COALESCE(txn.claim_type_cd, 'X') NOT IN ('I', 'P') THEN NULL
        WHEN COALESCE(txn.claim_type_cd, 'X') = 'P' AND LPAD(txn.pos_cd, 2, '0' )  IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99') THEN NULL
        --WHEN COALESCE(txn.claim_type_cd, 'X') = 'I' AND SUBSTR(COALESCE(fclty_type_pos_cd, ''), 1, 1) = '3' THEN NULL
        ELSE txn.refrn_provdr_last_nm
    END                                                                    AS prov_referring_name_1,
    -------------------------------------------------------------------------------------------------------------
    ----------------------  prov_referring_name_2
    -------------------------------------------------------------------------------------------------------------
    CASE
        WHEN COALESCE(txn.claim_type_cd, 'X') NOT IN ('I', 'P') THEN NULL
        WHEN COALESCE(txn.claim_type_cd, 'X') = 'P' AND LPAD(txn.pos_cd, 2, '0' )  IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99') THEN NULL
        --WHEN COALESCE(txn.claim_type_cd, 'X') = 'I' AND SUBSTR(COALESCE(fclty_type_pos_cd, ''), 1, 1) = '3' THEN NULL
        ELSE txn.refrn_provdr_first_nm
    END                                                                    AS prov_referring_name_2,
    -------------------------------------------------------------------------------------------------------------
    ----------------------  prov_facility_state_license
    -------------------------------------------------------------------------------------------------------------
    CASE
        WHEN COALESCE(txn.claim_type_cd, 'X') NOT IN ('I', 'P') THEN NULL
        WHEN COALESCE(txn.claim_type_cd, 'X') = 'P' AND LPAD(txn.pos_cd, 2, '0' )  IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99') THEN NULL
        --WHEN COALESCE(txn.claim_type_cd, 'X') = 'I' AND SUBSTR(COALESCE(fclty_type_pos_cd, ''), 1, 1) = '3' THEN NULL
        ELSE txn.fclty_stlc_nbr
    END                                                                    AS prov_facility_state_license,
    -------------------------------------------------------------------------------------------------------------
    ----------------------   prov_facility_commercial_id
    -------------------------------------------------------------------------------------------------------------
    CASE
        WHEN COALESCE(txn.claim_type_cd, 'X') NOT IN ('I', 'P') THEN NULL
        WHEN COALESCE(txn.claim_type_cd, 'X') = 'P' AND LPAD(txn.pos_cd, 2, '0' )  IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99') THEN NULL
        --WHEN COALESCE(txn.claim_type_cd, 'X') = 'I' AND SUBSTR(COALESCE(fclty_type_pos_cd, ''), 1, 1) = '3' THEN NULL
        ELSE txn.fclty_comm_nbr
    END                                                                    AS prov_facility_commercial_id,
    -------------------------------------------------------------------------------------------------------------
    ----------------------   prov_facility_name_1
    -------------------------------------------------------------------------------------------------------------
    CASE
        WHEN COALESCE(txn.claim_type_cd, 'X') NOT IN ('I', 'P') THEN NULL
        WHEN COALESCE(txn.claim_type_cd, 'X') = 'P' AND LPAD(txn.pos_cd, 2, '0' )  IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99') THEN NULL
        --WHEN COALESCE(txn.claim_type_cd, 'X') = 'I' AND SUBSTR(COALESCE(fclty_type_pos_cd, ''), 1, 1) = '3' THEN NULL
        ELSE txn.fclty_nm
    END                                                                    AS prov_facility_name_1,
    -------------------------------------------------------------------------------------------------------------
    ----------------------   prov_facility_address_1
    -------------------------------------------------------------------------------------------------------------
    CASE
        WHEN COALESCE(txn.claim_type_cd, 'X') NOT IN ('I', 'P') THEN NULL
        WHEN COALESCE(txn.claim_type_cd, 'X') = 'P' AND LPAD(txn.pos_cd, 2, '0' )  IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99') THEN NULL
        --WHEN COALESCE(txn.claim_type_cd, 'X') = 'I' AND SUBSTR(COALESCE(fclty_type_pos_cd, ''), 1, 1) = '3' THEN NULL
        ELSE txn.fclty_addr_1
    END                                                                    AS prov_facility_address_1,
    -------------------------------------------------------------------------------------------------------------
    ----------------------   prov_facility_address_2
    -------------------------------------------------------------------------------------------------------------
    CASE
        WHEN COALESCE(txn.claim_type_cd, 'X') NOT IN ('I', 'P') THEN NULL
        WHEN COALESCE(txn.claim_type_cd, 'X') = 'P' AND LPAD(txn.pos_cd, 2, '0' )  IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99') THEN NULL
        --WHEN COALESCE(txn.claim_type_cd, 'X') = 'I' AND SUBSTR(COALESCE(fclty_type_pos_cd, ''), 1, 1) = '3' THEN NULL
        ELSE txn.fclty_addr_2
    END                                                                    AS prov_facility_address_2,
    -------------------------------------------------------------------------------------------------------------
    ----------------------   prov_facility_city
    -------------------------------------------------------------------------------------------------------------
    CASE
        WHEN COALESCE(txn.claim_type_cd, 'X') NOT IN ('I', 'P') THEN NULL
        WHEN COALESCE(txn.claim_type_cd, 'X') = 'P' AND LPAD(txn.pos_cd, 2, '0' )  IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99') THEN NULL
        --WHEN COALESCE(txn.claim_type_cd, 'X') = 'I' AND SUBSTR(COALESCE(fclty_type_pos_cd, ''), 1, 1) = '3' THEN NULL
        ELSE txn.fclty_addr_city
    END                                                                    AS prov_facility_city,
    -------------------------------------------------------------------------------------------------------------
    ----------------------   prov_facility_state
    -------------------------------------------------------------------------------------------------------------
    CASE
        WHEN COALESCE(txn.claim_type_cd, 'X') NOT IN ('I', 'P') THEN NULL
        WHEN COALESCE(txn.claim_type_cd, 'X') = 'P' AND LPAD(txn.pos_cd, 2, '0' )  IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99') THEN NULL
        --WHEN COALESCE(txn.claim_type_cd, 'X') = 'I' AND SUBSTR(COALESCE(fclty_type_pos_cd, ''), 1, 1) = '3' THEN NULL
        ELSE txn.fclty_addr_state
    END                                                                    AS prov_facility_state,
    -------------------------------------------------------------------------------------------------------------
    ----------------------   prov_facility_zip
    -------------------------------------------------------------------------------------------------------------
    CASE
        WHEN COALESCE(txn.claim_type_cd, 'X') NOT IN ('I', 'P') THEN NULL
        WHEN COALESCE(txn.claim_type_cd, 'X') = 'P' AND LPAD(txn.pos_cd, 2, '0' )  IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99') THEN NULL
        --WHEN COALESCE(txn.claim_type_cd, 'X') = 'I' AND SUBSTR(COALESCE(fclty_type_pos_cd, ''), 1, 1) = '3' THEN NULL
        ELSE txn.fclty_addr_zip
    END                                                                    AS prov_facility_zip,
    'claimremedi'	                                                       AS part_provider,
	CASE
	    WHEN 0 = LENGTH(COALESCE
	                            (
	                                CAP_DATE
                                        (
                                            CAST(EXTRACT_DATE(COALESCE(txn.svc_from_dt,txn.stmnt_from_dt), '%Y%m%d') AS DATE),
                                            CAST(COALESCE('{AVAILABLE_START_DATE}', '{EARLIEST_SERVICE_DATE}') AS DATE),
                                            CAST('{VDR_FILE_DT}' AS DATE)
                                        ),
                                    ''
                                ))
	        THEN '0_PREDATES_HVM_HISTORY'
	    ELSE CONCAT
	            (
                    SUBSTR(COALESCE(txn.svc_from_dt,txn.stmnt_from_dt), 1, 4), '-',
                    SUBSTR(COALESCE(txn.svc_from_dt,txn.stmnt_from_dt), 5, 2), '-01'
                )
	END                                                                   AS part_best_date
FROM claimremedi_837_dedup txn
LEFT OUTER JOIN claimremedi_payload_dedup pay
    ON txn.src_claim_id = pay.claimid
LEFT OUTER JOIN claimremedi_diag_pvt pvtdiag
    ON txn.src_claim_id = pvtdiag.src_claim_id
LEFT OUTER JOIN claimremedi_proc_pvt pvtproc
    ON txn.src_claim_id = pvtproc.src_claim_id
GROUP BY
1,	2,	3,	4,	5,	6,	7,	8,	9,	10, 11,	12,	13,	14,	15,	16,	17,	18,	19,	20,
21,	22,	23,	24,	25,	26,	27,	28,	29,	30, 31,	32,	33,	34,	35,	36,	37,	38,	39,	40,
41,	42,	43,	44,	45,	46,	47,	48,	49,	50, 51,	52,	53,	54,	55,	56,	57,	58,	59,	60,
61,	62,	63,	64,	65,	66,	67,	68,	69, 70,	71,	72,	73,	74,	75,	76,	77,	78, 79
