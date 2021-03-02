SELECT
    sln.claim_number  AS claim_id,
    sln.pay_hvid                                                                                 AS hvid,
    CURRENT_DATE()                                                                           AS created,
 	'10'                                                                                     AS model_version,
    SPLIT(sln.input_file_name, '/')[SIZE(SPLIT(sln.input_file_name, '/')) - 1]               AS data_set,
 	'219'                                                                                    AS data_feed,
 	'576'                                                                                    AS data_vendor,
    CASE
        WHEN SUBSTR(UPPER(clm.patient_gender_code), 1, 1) IN ('F', 'M') THEN SUBSTR(UPPER(clm.patient_gender_code), 1, 1)
        WHEN SUBSTR(UPPER(sln.pln_patient_gender     ), 1, 1) IN ('F', 'M') THEN SUBSTR(UPPER(sln.pln_patient_gender     ), 1, 1)
        WHEN SUBSTR(UPPER(sln.pay_gender             ), 1, 1) IN ('F', 'M') THEN SUBSTR(UPPER(sln.pay_gender             ), 1, 1)
        ELSE 'U'
    END                                                                                       AS patient_gender,
    -- UDF-cap_year_of_birth(age, date_service, year_of_birth)
    CAP_YEAR_OF_BIRTH
    (
    NULL,
    CAST(EXTRACT_DATE(COALESCE(sln.service_to_date, sln.service_from_date), 'yyyyMMdd') AS DATE),
    COALESCE(clm.patient_birth_year, clm.member_birth_year, sln.pay_yearofbirth)
    )                                                                                         AS patient_year_of_birth,
    MASK_ZIP_CODE(COALESCE(clm.patient_zip3, sln.pay_threedigitzip))                              AS patient_zip3,
    VALIDATE_STATE_CODE(UPPER(COALESCE(clm.patient_state, sln.pay_state)))                        AS patient_state,
    clm.claim_type	                                                                          AS claim_type,
    COALESCE(TO_DATE(clm.received_date, 'yyyyMMdd'), CAST(clm.received_date AS DATE))         AS date_received,

    -----------------
    CASE
        WHEN TO_DATE(COALESCE(sln.service_from_date, clm.statement_from_date), 'yyyyMMdd')  < CAST('{EARLIEST_SERVICE_DATE}' AS DATE)
          OR TO_DATE(COALESCE(sln.service_from_date, clm.statement_from_date), 'yyyyMMdd') > CAST('{VDR_FILE_DT}' AS DATE)
          THEN NULL
        ELSE TO_DATE(COALESCE(sln.service_from_date, clm.statement_from_date), 'yyyyMMdd')
    END                                                                                      AS date_service,
    CASE
        WHEN TO_DATE(COALESCE(sln.service_to_date, clm.statement_to_date), 'yyyyMMdd')  < CAST('{EARLIEST_SERVICE_DATE}' AS DATE)
          OR TO_DATE(COALESCE(sln.service_to_date, clm.statement_to_date), 'yyyyMMdd') > CAST('{VDR_FILE_DT}' AS DATE)
          THEN NULL
        ELSE TO_DATE(COALESCE(sln.service_to_date, clm.statement_to_date), 'yyyyMMdd')
    END                                                                                      AS date_service_end,
    CASE
        WHEN TO_DATE(clm.admission_date, 'yyyyMMdd')  < CAST('{EARLIEST_SERVICE_DATE}' AS DATE)
          OR TO_DATE(clm.admission_date, 'yyyyMMdd')  > CAST('{VDR_FILE_DT}' AS DATE)
          THEN NULL
    ELSE TO_DATE(clm.admission_date, 'yyyyMMdd')
    END                                                                                      AS inst_date_admitted,

    clm.admission_type_code                                                                  AS inst_admit_type_std_id,
    clm.admission_source_code                                                                AS inst_admit_source_std_id,
    clm.patient_status_code                                                                  AS inst_discharge_status_std_id,
    CASE
        WHEN UPPER(COALESCE(clm.claim_type, 'X')) <> 'I'      THEN NULL
        WHEN SUBSTR(COALESCE(clm.bill_type, 'X'), 1, 1) = '3' THEN CONCAT('X', SUBSTR(clm.bill_type, 2))
        ELSE clm.bill_type
    END                                                                                     AS inst_type_of_bill_std_id,
    CASE
         WHEN UPPER(COALESCE(clm.claim_type, 'X')) = 'I'   AND clm.drg_code IN ('283', '284', '285', '789') THEN  NULL
         WHEN UPPER(COALESCE(clm.claim_type, 'X')) = 'I'   THEN clm.drg_code
         ELSE NULL
    END                                                                                     AS inst_drg_std_id,
    CASE
        WHEN UPPER(COALESCE(clm.claim_type, 'X')) <> 'P'                                                        THEN NULL
        --- From service line
        WHEN  sln.place_of_service IS NULL                                                                       THEN NULL
        WHEN LPAD(sln.place_of_service , 2, '0') IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99') THEN '99'
        WHEN  sln.place_of_service IS NOT NULL                                                                   THEN LPAD(sln.place_of_service , 2, '0')
        --------- from claim
        WHEN LPAD(clm.bill_type, 2, '0') IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')         THEN '99'
        WHEN  COALESCE(clm.claim_type, 'X')  =  'P'  AND clm.bill_type IS NOT NULL                               THEN LPAD(clm.bill_type , 2, '0')
    END                                                                                     AS place_of_service_std_id,
    sln.service_line_number                                                                 AS service_line_number,
    ---------------------------------------------------------------------------------------------------------------------------
    -- Diag
    ---------------------------------------------------------------------------------------------------------------------------
    CASE
        WHEN ARRAY(sln.diagnosis_code_pointer_1,sln.diagnosis_code_pointer_2,sln.diagnosis_code_pointer_3,sln.diagnosis_code_pointer_4)[diag_explode.n] = 1
            THEN clm.principal_diagnosis_code
        WHEN ARRAY(sln.diagnosis_code_pointer_1,sln.diagnosis_code_pointer_2,sln.diagnosis_code_pointer_3,sln.diagnosis_code_pointer_4)[diag_explode.n] = 2
            THEN clm.other_diagnosis_code_1
        WHEN ARRAY(sln.diagnosis_code_pointer_1,sln.diagnosis_code_pointer_2,sln.diagnosis_code_pointer_3,sln.diagnosis_code_pointer_4)[diag_explode.n] = 3
            THEN clm.other_diagnosis_code_2
        WHEN ARRAY(sln.diagnosis_code_pointer_1,sln.diagnosis_code_pointer_2,sln.diagnosis_code_pointer_3,sln.diagnosis_code_pointer_4)[diag_explode.n] = 4
            THEN clm.other_diagnosis_code_3
        WHEN ARRAY(sln.diagnosis_code_pointer_1,sln.diagnosis_code_pointer_2,sln.diagnosis_code_pointer_3,sln.diagnosis_code_pointer_4)[diag_explode.n] = 5
            THEN clm.other_diagnosis_code_4
        WHEN ARRAY(sln.diagnosis_code_pointer_1,sln.diagnosis_code_pointer_2,sln.diagnosis_code_pointer_3,sln.diagnosis_code_pointer_4)[diag_explode.n] = 6
            THEN clm.other_diagnosis_code_5
        WHEN ARRAY(sln.diagnosis_code_pointer_1,sln.diagnosis_code_pointer_2,sln.diagnosis_code_pointer_3,sln.diagnosis_code_pointer_4)[diag_explode.n] = 7
            THEN clm.other_diagnosis_code_6
        WHEN ARRAY(sln.diagnosis_code_pointer_1,sln.diagnosis_code_pointer_2,sln.diagnosis_code_pointer_3,sln.diagnosis_code_pointer_4)[diag_explode.n] = 8
            THEN clm.other_diagnosis_code_7
    END  AS diagnosis_code,

    CASE
        WHEN clm.coding_type = '9' THEN '01'
        WHEN clm.coding_type = 'X' THEN '02'
    ELSE NULL
    END                                                                                   AS diagnosis_code_qual,
	/* diagnosis_priority */
    CASE
       WHEN ARRAY(sln.diagnosis_code_pointer_1,sln.diagnosis_code_pointer_2,sln.diagnosis_code_pointer_3,sln.diagnosis_code_pointer_4)[diag_explode.n] IS NULL
             THEN NULL
        ELSE CAST(1 AS INTEGER) + diag_explode.n
	END 																					AS diagnosis_priority,

    CASE
        WHEN COALESCE(claim_type, 'X') <> 'I' THEN NULL
        WHEN COALESCE(claim_type, 'X') = 'I' AND clm.admitting_diagnosis_code IS     NULL THEN  'N'
        WHEN COALESCE(claim_type, 'X') = 'I' AND clm.admitting_diagnosis_code IS NOT NULL
        AND
        (
           clm.principal_diagnosis_code =
        CASE
            WHEN ARRAY(sln.diagnosis_code_pointer_1,sln.diagnosis_code_pointer_2,sln.diagnosis_code_pointer_3,sln.diagnosis_code_pointer_4)[diag_explode.n] = 1
                THEN clm.principal_diagnosis_code
            WHEN ARRAY(sln.diagnosis_code_pointer_1,sln.diagnosis_code_pointer_2,sln.diagnosis_code_pointer_3,sln.diagnosis_code_pointer_4)[diag_explode.n] = 2
                THEN clm.other_diagnosis_code_1
            WHEN ARRAY(sln.diagnosis_code_pointer_1,sln.diagnosis_code_pointer_2,sln.diagnosis_code_pointer_3,sln.diagnosis_code_pointer_4)[diag_explode.n] = 3
                THEN clm.other_diagnosis_code_2
            WHEN ARRAY(sln.diagnosis_code_pointer_1,sln.diagnosis_code_pointer_2,sln.diagnosis_code_pointer_3,sln.diagnosis_code_pointer_4)[diag_explode.n] = 4
                THEN clm.other_diagnosis_code_3
            WHEN ARRAY(sln.diagnosis_code_pointer_1,sln.diagnosis_code_pointer_2,sln.diagnosis_code_pointer_3,sln.diagnosis_code_pointer_4)[diag_explode.n] = 5
                THEN clm.other_diagnosis_code_4
            WHEN ARRAY(sln.diagnosis_code_pointer_1,sln.diagnosis_code_pointer_2,sln.diagnosis_code_pointer_3,sln.diagnosis_code_pointer_4)[diag_explode.n] = 6
                THEN clm.other_diagnosis_code_5
            WHEN ARRAY(sln.diagnosis_code_pointer_1,sln.diagnosis_code_pointer_2,sln.diagnosis_code_pointer_3,sln.diagnosis_code_pointer_4)[diag_explode.n] = 7
                THEN clm.other_diagnosis_code_6
            WHEN ARRAY(sln.diagnosis_code_pointer_1,sln.diagnosis_code_pointer_2,sln.diagnosis_code_pointer_3,sln.diagnosis_code_pointer_4)[diag_explode.n] = 8
                THEN clm.other_diagnosis_code_7
        END
        )
        THEN  'Y'
    ELSE NULL
    END                                                                        AS admit_diagnosis_ind,
    CLEAN_UP_PROCEDURE_CODE(sln.procedure_code)                                AS procedure_code,
    CASE
        WHEN sln.procedure_code IS NULL THEN NULL
    ELSE sln.procedure_code_qual
    END                                                                        AS procedure_code_qual,

    CASE
        WHEN UPPER(COALESCE(clm.claim_type, 'X')) <> 'I'      THEN NULL
        WHEN UPPER(COALESCE(clm.claim_type, 'X')) = 'I' AND clm.principal_icd_procedure_code IS     NULL THEN  'N'
        WHEN UPPER(COALESCE(clm.claim_type, 'X')) = 'I' AND clm.principal_icd_procedure_code IS NOT NULL
        AND sln.procedure_code  = clm.principal_icd_procedure_code THEN  'Y'
        ELSE NULL
    END                                                                        AS principal_proc_ind,
    sln.unit_count                                                             AS procedure_units_billed,
	CLEAN_UP_ALPHANUMERIC_CODE(SUBSTR(UPPER(sln.procedure_modifier_1), 1, 2))  AS procedure_modifier_1,
	CLEAN_UP_ALPHANUMERIC_CODE(SUBSTR(UPPER(sln.procedure_modifier_2), 1, 2))  AS procedure_modifier_2,
	CLEAN_UP_ALPHANUMERIC_CODE(SUBSTR(UPPER(sln.procedure_modifier_3), 1, 2))  AS procedure_modifier_3,
	CLEAN_UP_ALPHANUMERIC_CODE(SUBSTR(UPPER(sln.procedure_modifier_4), 1, 2))  AS procedure_modifier_4,
    sln.revenue_code                                                           AS revenue_code,
    CLEAN_UP_NDC_CODE(sln.national_drug_code)                                  AS ndc_code,
    clm.claim_filing_indicator_cd                                              AS medical_coverage_type,
    sln.service_line_charge_amount                                             AS line_charge,
    clm.total_claim_charge_amt	                                               AS total_charge,

    CASE
        WHEN COALESCE(clm.claim_type, 'X') NOT IN ('I', 'P') THEN NULL
        WHEN COALESCE(clm.claim_type, 'X') = 'P' AND LPAD(clm.bill_type, 2, '0' )  IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99') THEN NULL
        WHEN COALESCE(clm.claim_type, 'X') = 'I' AND SUBSTR(COALESCE(clm.bill_type, ''), 1, 1) = '3' THEN NULL
        ELSE CLEAN_UP_NPI_CODE(clm.rendering_attending_prov_npi)
    END                                                                        AS prov_rendering_npi,
    CASE
        WHEN COALESCE(clm.claim_type, 'X') NOT IN ('I', 'P') THEN NULL
        WHEN COALESCE(clm.claim_type, 'X') = 'P' AND LPAD(clm.bill_type, 2, '0' )  IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99') THEN NULL
        WHEN COALESCE(clm.claim_type, 'X') = 'I' AND SUBSTR(COALESCE(clm.bill_type, ''), 1, 1) = '3' THEN NULL
        ELSE CLEAN_UP_NPI_CODE(clm.billing_prov_npi)
    END                                                                        AS prov_billing_npi,

    CASE
        WHEN COALESCE(clm.claim_type, 'X') NOT IN ('I', 'P') THEN NULL
        WHEN COALESCE(clm.claim_type, 'X') = 'P' AND LPAD(clm.bill_type, 2, '0' )  IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99') THEN NULL
        WHEN COALESCE(clm.claim_type, 'X') = 'I' AND SUBSTR(COALESCE(clm.bill_type, ''), 1, 1) = '3' THEN NULL
        ELSE CLEAN_UP_NPI_CODE(clm.referring_prov_npi)
    END                                                                        AS prov_referring_npi,
    CASE
        WHEN COALESCE(clm.claim_type, 'X') NOT IN ('I', 'P') THEN NULL
        WHEN COALESCE(clm.claim_type, 'X') = 'P' AND LPAD(clm.bill_type, 2, '0' )  IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99') THEN NULL
        WHEN COALESCE(clm.claim_type, 'X') = 'I' AND SUBSTR(COALESCE(clm.bill_type, ''), 1, 1) = '3' THEN NULL
        ELSE CLEAN_UP_NPI_CODE(clm.facility_npi)
    END                                                                        AS prov_facility_npi,
    clm.payer_name                                                             AS payer_name,
    clm.payer_id                                                               AS payer_plan_id,
    CASE
        WHEN COALESCE(clm.claim_type, 'X') NOT IN ('I', 'P') THEN NULL
        WHEN COALESCE(clm.claim_type, 'X') = 'P' AND LPAD(clm.bill_type, 2, '0' )  IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99') THEN NULL
        WHEN COALESCE(clm.claim_type, 'X') = 'I' AND SUBSTR(COALESCE(clm.bill_type, ''), 1, 1) = '3' THEN NULL
        ELSE clm.rendering_attending_prov_state_license
    END                                                                        AS prov_rendering_state_license,
    CASE
        WHEN COALESCE(claim_type, 'X') NOT IN ('I', 'P') THEN NULL
        WHEN COALESCE(claim_type, 'X') = 'P' AND LPAD(clm.bill_type, 2, '0' )  IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99') THEN NULL
        WHEN COALESCE(claim_type, 'X') = 'I' AND SUBSTR(COALESCE(clm.bill_type, ''), 1, 1) = '3' THEN NULL
        WHEN COALESCE(claim_type, 'X') = 'P' THEN clm.rendering_attending_prov_ind_name
        WHEN COALESCE(claim_type, 'X') = 'I' THEN clm.rendering_prov_org_name
        ELSE NULL
    END                                                                        AS prov_rendering_name_1,
   clm.rendering_attending_prov_taxonomy			                           AS prov_rendering_std_taxonomy,
    -----------------------------------------------------------------------------------------
    ------------------------------------ Billing Provider
    -----------------------------------------------------------------------------------------
    CASE
        WHEN COALESCE(clm.claim_type, 'X') NOT IN ('I', 'P') THEN NULL
        WHEN COALESCE(clm.claim_type, 'X') = 'P' AND LPAD(clm.bill_type, 2, '0' )  IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99') THEN NULL
        WHEN COALESCE(clm.claim_type, 'X') = 'I' AND SUBSTR(COALESCE(clm.bill_type, ''), 1, 1) = '3' THEN NULL
        ELSE clm.billing_prov_id
    END                                                                        AS prov_billing_vendor_id,
    CASE
        WHEN COALESCE(clm.claim_type, 'X') NOT IN ('I', 'P') THEN NULL
        WHEN COALESCE(clm.claim_type, 'X') = 'P' AND LPAD(clm.bill_type, 2, '0' )  IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99') THEN NULL
        WHEN COALESCE(clm.claim_type, 'X') = 'I' AND SUBSTR(COALESCE(clm.bill_type, ''), 1, 1) = '3' THEN NULL
        ELSE clm.billing_pr_ssn
    END                                                                        AS prov_billing_ssn,

    CASE
        WHEN COALESCE(clm.claim_type, 'X') NOT IN ('I', 'P') THEN NULL
        WHEN COALESCE(clm.claim_type, 'X') = 'P' AND LPAD(clm.bill_type, 2, '0' )  IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99') THEN NULL
        WHEN COALESCE(clm.claim_type, 'X') = 'I' AND SUBSTR(COALESCE(clm.bill_type, ''), 1, 1) = '3' THEN NULL
        ELSE clm.billing_pr_state_license
    END                                                                        AS prov_billing_state_license,
    CASE
        WHEN COALESCE(clm.claim_type, 'X') NOT IN ('I', 'P') THEN NULL
        WHEN COALESCE(clm.claim_type, 'X') = 'P' AND LPAD(clm.bill_type, 2, '0' )  IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99') THEN NULL
        WHEN COALESCE(clm.claim_type, 'X') = 'I' AND SUBSTR(COALESCE(clm.bill_type, ''), 1, 1) = '3' THEN NULL
        WHEN COALESCE(clm.claim_type, 'X') = 'P' THEN clm.billing_prov_ind_name
        WHEN COALESCE(clm.claim_type, 'X') = 'I' THEN clm.billing_prov_org_name
        ELSE NULL
    END                                                                         AS prov_billing_name_1,
    CASE
        WHEN COALESCE(clm.claim_type, 'X') NOT IN ('I', 'P') THEN NULL
        WHEN COALESCE(clm.claim_type, 'X') = 'P' AND LPAD(clm.bill_type, 2, '0' )  IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99') THEN NULL
        WHEN COALESCE(clm.claim_type, 'X') = 'I' AND SUBSTR(COALESCE(clm.bill_type, ''), 1, 1) = '3' THEN NULL
        ELSE clm.billing_prov_street_address_1
    END                                                                         AS prov_billing_address_1,
    CASE
        WHEN COALESCE(clm.claim_type, 'X') NOT IN ('I', 'P') THEN NULL
        WHEN COALESCE(clm.claim_type, 'X') = 'P' AND LPAD(clm.bill_type, 2, '0' )  IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99') THEN NULL
        WHEN COALESCE(clm.claim_type, 'X') = 'I' AND SUBSTR(COALESCE(clm.bill_type, ''), 1, 1) = '3' THEN NULL
        ELSE clm.billing_prov_street_address_2
    END                                                                         AS prov_billing_address_2,
    CASE
        WHEN COALESCE(clm.claim_type, 'X') NOT IN ('I', 'P') THEN NULL
        WHEN COALESCE(clm.claim_type, 'X') = 'P' AND LPAD(clm.bill_type, 2, '0' )  IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99') THEN NULL
        WHEN COALESCE(clm.claim_type, 'X') = 'I' AND SUBSTR(COALESCE(clm.bill_type, ''), 1, 1) = '3' THEN NULL
        ELSE clm.billing_prov_city
    END                                                                         AS prov_billing_city,
    CASE
        WHEN COALESCE(clm.claim_type, 'X') NOT IN ('I', 'P') THEN NULL
        WHEN COALESCE(clm.claim_type, 'X') = 'P' AND LPAD(clm.bill_type, 2, '0' )  IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99') THEN NULL
        WHEN COALESCE(clm.claim_type, 'X') = 'I' AND SUBSTR(COALESCE(clm.bill_type, ''), 1, 1) = '3' THEN NULL
        ELSE VALIDATE_STATE_CODE(clm.billing_prov_state)
    END                                                                         AS prov_billing_state,
    CASE
        WHEN COALESCE(clm.claim_type, 'X') NOT IN ('I', 'P') THEN NULL
        WHEN COALESCE(clm.claim_type, 'X') = 'P' AND LPAD(clm.bill_type, 2, '0' )  IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99') THEN NULL
        WHEN COALESCE(clm.claim_type, 'X') = 'I' AND SUBSTR(COALESCE(clm.bill_type, ''), 1, 1) = '3' THEN NULL
        ELSE VALIDATE_STATE_CODE(clm.billing_prov_zip)
    END                                                                         AS prov_billing_zip,
    clm.billing_pr_taxonomy			                                            AS prov_billing_std_taxonomy,

    -----------------------------------------------------------------------------------------
    ------------------------------------ Referring Provider
    -----------------------------------------------------------------------------------------
    CASE
        WHEN COALESCE(clm.claim_type, 'X') NOT IN ('I', 'P') THEN NULL
        WHEN COALESCE(clm.claim_type, 'X') = 'P' AND LPAD(clm.bill_type, 2, '0' )  IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99') THEN NULL
        WHEN COALESCE(clm.claim_type, 'X') = 'I' AND SUBSTR(COALESCE(clm.bill_type, ''), 1, 1) = '3' THEN NULL
        ELSE clm.referring_prov_name
    END                                                                         AS prov_referring_name_1,
    -----------------------------------------------------------------------------------------
    ------------------------------------ Facility Provider
    -----------------------------------------------------------------------------------------
    CASE
        WHEN COALESCE(clm.claim_type, 'X') NOT IN ('I', 'P') THEN NULL
        WHEN COALESCE(clm.claim_type, 'X') = 'P' AND LPAD(clm.bill_type, 2, '0' )  IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99') THEN NULL
        WHEN COALESCE(clm.claim_type, 'X') = 'I' AND SUBSTR(COALESCE(clm.bill_type, ''), 1, 1) = '3' THEN NULL
        ELSE clm.facility_state_license
    END                                                                         AS prov_facility_state_license,
    CASE
        WHEN COALESCE(clm.claim_type, 'X') NOT IN ('I', 'P') THEN NULL
        WHEN COALESCE(clm.claim_type, 'X') = 'P' AND LPAD(clm.bill_type, 2, '0' )  IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99') THEN NULL
        WHEN COALESCE(clm.claim_type, 'X') = 'I' AND SUBSTR(COALESCE(clm.bill_type, ''), 1, 1) = '3' THEN NULL
        ELSE clm.facility_name
    END                                                                         AS prov_facility_name_1,

    CASE
        WHEN COALESCE(clm.claim_type, 'X') NOT IN ('I', 'P') THEN NULL
        WHEN COALESCE(clm.claim_type, 'X') = 'P' AND LPAD(clm.bill_type, 2, '0' )  IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99') THEN NULL
        WHEN COALESCE(clm.claim_type, 'X') = 'I' AND SUBSTR(COALESCE(clm.bill_type, ''), 1, 1) = '3' THEN NULL
        ELSE clm.facility_street_address_1
    END                                                                         AS prov_facility_address_1,
    CASE
        WHEN COALESCE(clm.claim_type, 'X') NOT IN ('I', 'P') THEN NULL
        WHEN COALESCE(clm.claim_type, 'X') = 'P' AND LPAD(clm.bill_type, 2, '0' )  IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99') THEN NULL
        WHEN COALESCE(clm.claim_type, 'X') = 'I' AND SUBSTR(COALESCE(clm.bill_type, ''), 1, 1) = '3' THEN NULL
        ELSE clm.facility_street_address_2
    END                                                                         AS prov_facility_address_2,
    CASE
        WHEN COALESCE(clm.claim_type, 'X') NOT IN ('I', 'P') THEN NULL
        WHEN COALESCE(clm.claim_type, 'X') = 'P' AND LPAD(clm.bill_type, 2, '0' )  IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99') THEN NULL
        WHEN COALESCE(clm.claim_type, 'X') = 'I' AND SUBSTR(COALESCE(clm.bill_type, ''), 1, 1) = '3' THEN NULL
        ELSE clm.facility_city
    END                                                                         AS prov_facility_city,
    CASE
        WHEN COALESCE(clm.claim_type, 'X') NOT IN ('I', 'P') THEN NULL
        WHEN COALESCE(clm.claim_type, 'X') = 'P' AND LPAD(clm.bill_type, 2, '0' )  IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99') THEN NULL
        WHEN COALESCE(clm.claim_type, 'X') = 'I' AND SUBSTR(COALESCE(clm.bill_type, ''), 1, 1) = '3' THEN NULL
        ELSE VALIDATE_STATE_CODE(clm.facility_state)
    END                                                                         AS prov_facility_state,
    CASE
        WHEN COALESCE(clm.claim_type, 'X') NOT IN ('I', 'P') THEN NULL
        WHEN COALESCE(clm.claim_type, 'X') = 'P' AND LPAD(clm.bill_type, 2, '0' )  IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99') THEN NULL
        WHEN COALESCE(clm.claim_type, 'X') = 'I' AND SUBSTR(COALESCE(clm.bill_type, ''), 1, 1) = '3' THEN NULL
        ELSE clm.facility_zip
    END                                                                         AS prov_facility_zip,
    sln.pas_pcn                                                                     AS medical_claim_link_text,
    'change_dx'                                                                    AS part_provider,
    CASE
        WHEN TO_DATE(COALESCE(sln.service_from_date, clm.statement_from_date), 'yyyyMMdd')  < CAST('{AVAILABLE_START_DATE}' AS DATE)
          OR TO_DATE(COALESCE(sln.service_from_date, clm.statement_from_date), 'yyyyMMdd')  > CAST('{VDR_FILE_DT}' AS DATE)
          THEN '0_PREDATES_HVM_HISTORY'
        ELSE CONCAT
        (
            SUBSTR(COALESCE(sln.service_from_date, clm.statement_from_date), 1,4), '-',
            SUBSTR(COALESCE(sln.service_from_date, clm.statement_from_date), 5,2), '-01'
        )
    END                                                                         AS part_best_date,
    ---------------------------------------------------------------------------------------------------------------------------
    'end'
FROM change_00_srv  sln
LEFT OUTER JOIN change_00_clm       clm ON  UPPER(sln.claim_number) = UPPER(clm.claim_tcn_id)
-- LEFT OUTER JOIN change_payload     pay ON  UPPER(sln.claim_number) = UPPER(pay.claimid)
-- LEFT OUTER JOIN change_realy_passthrough pas ON  UPPER(sln.claim_number) = UPPER(pas.claimid)
-- LEFT OUTER JOIN change_plainout    pln ON  UPPER(sln.claim_number) = UPPER(pln.claim_number)

CROSS JOIN (SELECT EXPLODE(ARRAY(0, 1, 2, 3)) AS n) diag_explode
WHERE --sln.claim_number IN ( 'EP020520731301197') AND
---------- Exclude Dental
    COALESCE(clm.claim_type, 'X') NOT IN ('D')
    AND
---------- Diagnosis code explosion
        (
        ARRAY(sln.diagnosis_code_pointer_1,sln.diagnosis_code_pointer_2,sln.diagnosis_code_pointer_3,sln.diagnosis_code_pointer_4)[diag_explode.n] IS NOT NULL
         OR
            (
            COALESCE(sln.diagnosis_code_pointer_1,sln.diagnosis_code_pointer_2,sln.diagnosis_code_pointer_3,sln.diagnosis_code_pointer_4) IS NULL AND diag_explode.n = 0
            )
        )

--LIMIT 100
