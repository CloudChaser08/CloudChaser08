SELECT  
    monotonically_increasing_id()                                                           AS record_id,
    clm.claim_number                                                                        AS claim_id,
    pay.hvid                                                                                AS hvid,
    '08'                                                                                    AS model_version,
    clm.data_set                                                                            AS data_set,
    '24'                                                                                    AS data_feed,
    '34'                                                                                    AS data_vendor,
    /* patient_gender */
    CLEAN_UP_GENDER
        (
            CASE
                WHEN UPPER(SUBSTR(clm.patient_gender, 1, 1)) IN ('F', 'M')
                     THEN UPPER(SUBSTR(clm.patient_gender, 1, 1))
                WHEN UPPER(SUBSTR(pay.gender, 1, 1)) IN ('F', 'M')
                     THEN UPPER(SUBSTR(pay.gender, 1, 1)) ELSE 'U' 
            END
        )                                                                                    AS patient_gender,
    /* patient_age */
    CAP_AGE
        (
            VALIDATE_AGE
                (
                    COALESCE(clm.patient_age, pay.age),
                    CAST(EXTRACT_DATE(COALESCE(sln.service_from, clm.statement_from), '%Y%m%d') AS DATE),
                    COALESCE(clm.patient_yob, pay.yearofbirth)
                )
        )                                                                                    AS patient_age,
    /* patient_year_of_birth */
    CAP_YEAR_OF_BIRTH
        (
            COALESCE(clm.patient_age, pay.age),
            CAST(EXTRACT_DATE(COALESCE(sln.service_from, clm.statement_from), '%Y%m%d') AS DATE),
            COALESCE(clm.patient_yob, pay.yearofbirth)
        )                                                                                    AS patient_year_of_birth,
    MASK_ZIP_CODE(SUBSTR(COALESCE(clm.member_adr_zip, pay.threedigitzip), 1, 3))            AS patient_zip3,
    VALIDATE_STATE_CODE(UPPER(COALESCE(clm.member_adr_state, pay.state, '')))                AS patient_state,
    /* As per Reyna, load all claim types, but only expose */
    /* Professional (P) and Institutional (I) claims in the view. */
    clm.claim_type_code                                                                        AS claim_type,
    CAST(EXTRACT_DATE(clm.received_date, '%Y%m%d') AS DATE)                                    AS date_received,
    /* date_service */
    CAP_DATE
        (
            CAST(EXTRACT_DATE(COALESCE(sln.service_from, clm.statement_from), '%Y%m%d') AS DATE),
            esdt.gen_ref_1_dt,
            CAST('{VDR_FILE_DT}' AS DATE)
        )                                                                                   AS date_service,
    /* date_service_end */
    CAP_DATE
        (
            CAST(EXTRACT_DATE(COALESCE(sln.service_to, clm.statement_to), '%Y%m%d') AS DATE),
            esdt.gen_ref_1_dt,
            CAST('{VDR_FILE_DT}' AS DATE)
        )                                                                                   AS date_service_end,
    /* inst_date_admitted */
    CAP_DATE
        (
            CAST(EXTRACT_DATE(clm.admission_date, '%Y%m%d') AS DATE),
            esdt.gen_ref_1_dt,
            CAST('{VDR_FILE_DT}' AS DATE)
        )                                                                                   AS inst_date_admitted,
    clm.admit_type_code                                                                        AS inst_admit_type_std_id,
    clm.admit_src_code                                                                        AS inst_admit_source_std_id,
    clm.patient_status_cd                                                                    AS inst_discharge_status_std_id,
    /* inst_type_of_bill_std_id */
    CASE 
        WHEN clm.type_bill IS NULL
             THEN NULL
        WHEN COALESCE(clm.claim_type_code, 'X') <> 'I'
             THEN NULL
        WHEN SUBSTR(clm.type_bill, 1, 1) = '3'
             THEN CONCAT('X', SUBSTR(clm.type_bill, 2)) 
        ELSE clm.type_bill
    END                                                                                     AS inst_type_of_bill_std_id,
    /* inst_drg_std_id */
    CASE
        WHEN clm.drg_code IN ('283', '284', '285', '789')
             THEN NULL
        ELSE clm.drg_code
    END                                                                                     AS inst_drg_std_id,
    /* place_of_service_std_id */
    CASE
        WHEN sln.place_service IS NULL
             THEN NULL
        WHEN COALESCE(clm.claim_type_code, 'X') <> 'P'
             THEN NULL
        WHEN SUBSTR(CONCAT('00', sln.place_service), -2) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33')
             THEN '99'
        ELSE SUBSTR(CONCAT('00', sln.place_service), -2)
    END                                                                                     AS place_of_service_std_id,
    sln.line_number                                                                            AS service_line_number,
    /* diagnosis_code */
    CASE
        WHEN ARRAY
                (
                    sln.diagnosis_pointer_1,
                    sln.diagnosis_pointer_2,
                    sln.diagnosis_pointer_3,
                    sln.diagnosis_pointer_4,
                    sln.diagnosis_pointer_5,
                    sln.diagnosis_pointer_6,
                    sln.diagnosis_pointer_7,
                    sln.diagnosis_pointer_8
                )[diag_explode.n] IS NULL 
             THEN NULL
        ELSE CLEAN_UP_DIAGNOSIS_CODE
                (
                    ARRAY
                        (
                            clm.primary_diagnosis,
                            clm.diagnosis_code_2,
                            clm.diagnosis_code_3,
                            clm.diagnosis_code_4,
                            clm.diagnosis_code_5,
                            clm.diagnosis_code_6,
                            clm.diagnosis_code_7,
                            clm.diagnosis_code_8
                        )
                            [
                                CAST(-1 AS INTEGER) +
                                CAST
                                    (
                                        COALESCE
                                            (
                                                ARRAY
                                                    (
                                                        sln.diagnosis_pointer_1,
                                                        sln.diagnosis_pointer_2,
                                                        sln.diagnosis_pointer_3,
                                                        sln.diagnosis_pointer_4,
                                                        sln.diagnosis_pointer_5,
                                                        sln.diagnosis_pointer_6,
                                                        sln.diagnosis_pointer_7,
                                                        sln.diagnosis_pointer_8
                                                    )[diag_explode.n], '1'
                                            ) AS INTEGER
                                    )
                            ],
                    CASE 
                        WHEN clm.coding_type IS NULL 
                             THEN NULL 
                        WHEN clm.coding_type = '9' 
                             THEN '01' 
                        WHEN UPPER(clm.coding_type) = 'X' 
                             THEN '02' 
                        ELSE NULL 
                    END,
                    CAST
                        (
                            EXTRACT_DATE
                                (
                                    COALESCE
                                        (
                                            sln.service_from, 
                                            clm.statement_from, 
                                            sln.service_to, 
                                            clm.statement_to
                                        ), '%Y%m%d'
                                ) AS DATE
                        )
                )
    END                                                                                     AS diagnosis_code,
    /* diagnosis_code_qual */
    CASE
        WHEN ARRAY
                (
                    sln.diagnosis_pointer_1,
                    sln.diagnosis_pointer_2,
                    sln.diagnosis_pointer_3,
                    sln.diagnosis_pointer_4,
                    sln.diagnosis_pointer_5,
                    sln.diagnosis_pointer_6,
                    sln.diagnosis_pointer_7,
                    sln.diagnosis_pointer_8
                )[diag_explode.n] IS NULL 
             THEN NULL
        WHEN clm.coding_type IS NULL
             THEN NULL
        WHEN clm.coding_type = '9'
             THEN '01'
        WHEN UPPER(clm.coding_type) = 'X'
             THEN '02'
        ELSE NULL
    END                                                                                     AS diagnosis_code_qual,
    /* diagnosis_priority */
    CASE
        WHEN ARRAY
                (
                    sln.diagnosis_pointer_1,
                    sln.diagnosis_pointer_2,
                    sln.diagnosis_pointer_3,
                    sln.diagnosis_pointer_4,
                    sln.diagnosis_pointer_5,
                    sln.diagnosis_pointer_6,
                    sln.diagnosis_pointer_7,
                    sln.diagnosis_pointer_8
                )[diag_explode.n] IS NULL 
             THEN NULL
        ELSE CAST(1 AS INTEGER) + diag_explode.n
    END                                                                                     AS diagnosis_priority,
    /* admit_diagnosis_ind */
    CASE
        WHEN ARRAY
                (
                    sln.diagnosis_pointer_1,
                    sln.diagnosis_pointer_2,
                    sln.diagnosis_pointer_3,
                    sln.diagnosis_pointer_4,
                    sln.diagnosis_pointer_5,
                    sln.diagnosis_pointer_6,
                    sln.diagnosis_pointer_7,
                    sln.diagnosis_pointer_8
                )[diag_explode.n] IS NULL 
             THEN NULL
        WHEN clm.admit_diagnosis IS NULL
             THEN NULL
        WHEN CLEAN_UP_DIAGNOSIS_CODE
                (
                    clm.admit_diagnosis,
                    CASE 
                        WHEN clm.coding_type IS NULL 
                             THEN NULL 
                        WHEN clm.coding_type = '9' 
                             THEN '01' 
                        WHEN UPPER(clm.coding_type) = 'X' 
                             THEN '02' 
                        ELSE NULL 
                    END,
                    CAST
                        (
                            EXTRACT_DATE
                                (
                                    COALESCE
                                        (
                                            sln.service_from, 
                                            clm.statement_from, 
                                            sln.service_to, 
                                            clm.statement_to
                                        ), '%Y%m%d'
                                ) AS DATE
                        )
                ) =
           CLEAN_UP_DIAGNOSIS_CODE
                (
                    ARRAY
                        (
                            clm.primary_diagnosis,
                            clm.diagnosis_code_2,
                            clm.diagnosis_code_3,
                            clm.diagnosis_code_4,
                            clm.diagnosis_code_5,
                            clm.diagnosis_code_6,
                            clm.diagnosis_code_7,
                            clm.diagnosis_code_8
                        )
                            [
                                CAST(-1 AS INTEGER) +
                                CAST
                                    (
                                        COALESCE
                                            (
                                                ARRAY
                                                    (
                                                        sln.diagnosis_pointer_1,
                                                        sln.diagnosis_pointer_2,
                                                        sln.diagnosis_pointer_3,
                                                        sln.diagnosis_pointer_4,
                                                        sln.diagnosis_pointer_5,
                                                        sln.diagnosis_pointer_6,
                                                        sln.diagnosis_pointer_7,
                                                        sln.diagnosis_pointer_8
                                                    )[diag_explode.n], '1'
                                            ) AS INTEGER
                                    )
                            ],
                    CASE 
                        WHEN clm.coding_type IS NULL 
                             THEN NULL 
                        WHEN clm.coding_type = '9' 
                             THEN '01' 
                        WHEN UPPER(clm.coding_type) = 'X' 
                             THEN '02' 
                        ELSE NULL 
                    END,
                    CAST
                        (
                            EXTRACT_DATE
                                (
                                    COALESCE
                                        (
                                            sln.service_from, 
                                            clm.statement_from, 
                                            sln.service_to, 
                                            clm.statement_to
                                        ), '%Y%m%d'
                                ) AS DATE
                        )
                )
             THEN 'Y'
        ELSE 'N'
    END                                                                                     AS admit_diagnosis_ind,
    CLEAN_UP_PROCEDURE_CODE(sln.procedure)                                                    AS procedure_code,
    /* procedure_code_qual */
    CASE
        WHEN sln.procedure IS NULL
             THEN NULL
        ELSE sln.procedure_qual
    END                                                                                     AS procedure_code_qual,
    /* principal_proc_ind */
    CASE
        WHEN sln.procedure IS NULL
             THEN NULL
        WHEN clm.principal_procedure IS NULL
             THEN NULL
        WHEN CLEAN_UP_PROCEDURE_CODE(sln.procedure) = CLEAN_UP_PROCEDURE_CODE(clm.principal_procedure)
             THEN 'Y'
        ELSE 'N'
    END                                                                                     AS principal_proc_ind,
    sln.units                                                                                AS procedure_units_billed,
    SUBSTR(UPPER(sln.procedure_modifier_1), 1, 2)                                            AS procedure_modifier_1,
    SUBSTR(UPPER(sln.procedure_modifier_2), 1, 2)                                            AS procedure_modifier_2,
    SUBSTR(UPPER(sln.procedure_modifier_3), 1, 2)                                            AS procedure_modifier_3,
    SUBSTR(UPPER(sln.procedure_modifier_4), 1, 2)                                            AS procedure_modifier_4,
    sln.revenue_code                                                                        AS revenue_code,
    CLEAN_UP_NDC_CODE(sln.ndc)                                                                AS ndc_code,
    clm.type_coverage                                                                        AS medical_coverage_type,
    CAST(sln.line_charge AS FLOAT)                                                            AS line_charge,
    CAST(sln.line_allowed AS FLOAT)                                                            AS line_allowed,
    CAST(clm.total_charge AS FLOAT)                                                            AS total_charge,
    CAST(clm.total_allowed AS FLOAT)                                                        AS total_allowed,
    /* prov_rendering_npi */
    CLEAN_UP_NPI_CODE
        (
            CASE
                WHEN COALESCE(clm.claim_type_code, 'X') NOT IN ('I', 'P')
                     THEN NULL
                WHEN clm.claim_type_code = 'P'
                 AND SUBSTR(CONCAT('00', sln.place_service), -2) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
                     THEN NULL
                WHEN clm.claim_type_code = 'I'
                 AND SUBSTR(COALESCE(clm.type_bill, 'X'), 1, 1) IN ('3', 'X')
                     THEN NULL
                ELSE clm.attending_pr_npi
            END
        )                                                                                   AS prov_rendering_npi,
    /* prov_billing_npi */
    CLEAN_UP_NPI_CODE
        (
            CASE
                WHEN COALESCE(clm.claim_type_code, 'X') NOT IN ('I', 'P')
                     THEN NULL
                WHEN clm.claim_type_code = 'P'
                 AND SUBSTR(CONCAT('00', sln.place_service), -2) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
                     THEN NULL
                WHEN clm.claim_type_code = 'I'
                 AND SUBSTR(COALESCE(clm.type_bill, 'X'), 1, 1) IN ('3', 'X')
                     THEN NULL
                ELSE clm.billing_pr_npi
            END
        )                                                                                    AS prov_billing_npi,
    /* prov_referring_npi */
    CLEAN_UP_NPI_CODE
        (
            CASE
                WHEN COALESCE(clm.claim_type_code, 'X') NOT IN ('I', 'P')
                     THEN NULL
                WHEN clm.claim_type_code = 'P'
                 AND SUBSTR(CONCAT('00', sln.place_service), -2) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
                     THEN NULL
                WHEN clm.claim_type_code = 'I'
                 AND SUBSTR(COALESCE(clm.type_bill, 'X'), 1, 1) IN ('3', 'X')
                     THEN NULL
                ELSE clm.referring_pr_npi
            END
        )                                                                                    AS prov_referring_npi,
    /* prov_facility_npi */
    CLEAN_UP_NPI_CODE
        (
            CASE
                WHEN COALESCE(clm.claim_type_code, 'X') NOT IN ('I', 'P')
                     THEN NULL
                WHEN clm.claim_type_code = 'P'
                 AND SUBSTR(CONCAT('00', sln.place_service), -2) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
                     THEN NULL
                WHEN clm.claim_type_code = 'I'
                 AND SUBSTR(COALESCE(clm.type_bill, 'X'), 1, 1) IN ('3', 'X')
                     THEN NULL
                ELSE clm.facility_npi
            END
        )                                                                                    AS prov_facility_npi,
    clm.payer_id                                                                            AS payer_plan_id,
    clm.payer_name                                                                            AS payer_plan_name,
    /* prov_rendering_state_license */
    CASE
        WHEN COALESCE(clm.claim_type_code, 'X') NOT IN ('I', 'P')
             THEN NULL
        WHEN clm.claim_type_code = 'P'
         AND SUBSTR(CONCAT('00', sln.place_service), -2) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
             THEN NULL
        WHEN clm.claim_type_code = 'I'
         AND SUBSTR(COALESCE(clm.type_bill, 'X'), 1, 1) IN ('3', 'X')
             THEN NULL
        ELSE clm.rendering_state_lic
    END                                                                                        AS prov_rendering_state_license,
    /* prov_rendering_upin */
    CASE
        WHEN COALESCE(clm.claim_type_code, 'X') NOT IN ('I', 'P')
             THEN NULL
        WHEN clm.claim_type_code = 'P'
         AND SUBSTR(CONCAT('00', sln.place_service), -2) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
             THEN NULL
        WHEN clm.claim_type_code = 'I'
         AND SUBSTR(COALESCE(clm.type_bill, 'X'), 1, 1) IN ('3', 'X')
             THEN NULL
        ELSE clm.rendering_upin
    END                                                                                        AS prov_rendering_upin,
    /* prov_rendering_name_1 */
    CASE
        WHEN COALESCE(clm.claim_type_code, 'X') NOT IN ('I', 'P')
             THEN NULL
        WHEN clm.claim_type_code = 'P'
         AND SUBSTR(CONCAT('00', sln.place_service), -2) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
             THEN NULL
        WHEN clm.claim_type_code = 'I'
         AND SUBSTR(COALESCE(clm.type_bill, 'X'), 1, 1) IN ('3', 'X')
             THEN NULL
        ELSE clm.attending_name1
    END                                                                                        AS prov_rendering_name_1,
    /* prov_rendering_name_2 */
    CASE
        WHEN COALESCE(clm.claim_type_code, 'X') NOT IN ('I', 'P')
             THEN NULL
        WHEN clm.claim_type_code = 'P'
         AND SUBSTR(CONCAT('00', sln.place_service), -2) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
             THEN NULL
        WHEN clm.claim_type_code = 'I'
         AND SUBSTR(COALESCE(clm.type_bill, 'X'), 1, 1) IN ('3', 'X')
             THEN NULL
        ELSE clm.attending_name2
    END                                                                                        AS prov_rendering_name_2,
    clm.rendering_taxonomy                                                                    AS prov_rendering_std_taxonomy,
    clm.prov_specialty                                                                        AS prov_rendering_vendor_specialty,
    /* prov_billing_tax_id */
    CASE
        WHEN COALESCE(clm.claim_type_code, 'X') NOT IN ('I', 'P')
             THEN NULL
        WHEN clm.claim_type_code = 'P'
         AND SUBSTR(CONCAT('00', sln.place_service), -2) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
             THEN NULL
        WHEN clm.claim_type_code = 'I'
         AND SUBSTR(COALESCE(clm.type_bill, 'X'), 1, 1) IN ('3', 'X')
             THEN NULL
        ELSE clm.billing_pr_id
    END                                                                                        AS prov_billing_tax_id,
    /* prov_billing_ssn */
    CASE
        WHEN COALESCE(clm.claim_type_code, 'X') NOT IN ('I', 'P')
             THEN NULL
        WHEN clm.claim_type_code = 'P'
         AND SUBSTR(CONCAT('00', sln.place_service), -2) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
             THEN NULL
        WHEN clm.claim_type_code = 'I'
         AND SUBSTR(COALESCE(clm.type_bill, 'X'), 1, 1) IN ('3', 'X')
             THEN NULL
        ELSE clm.billing_ssn
    END                                                                                        AS prov_billing_ssn,
    /* prov_billing_state_license */
    CASE
        WHEN COALESCE(clm.claim_type_code, 'X') NOT IN ('I', 'P')
             THEN NULL
        WHEN clm.claim_type_code = 'P'
         AND SUBSTR(CONCAT('00', sln.place_service), -2) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
             THEN NULL
        WHEN clm.claim_type_code = 'I'
         AND SUBSTR(COALESCE(clm.type_bill, 'X'), 1, 1) IN ('3', 'X')
             THEN NULL
        ELSE clm.billing_state_lic
    END                                                                                        AS prov_billing_state_license,
    /* prov_billing_upin */
    CASE
        WHEN COALESCE(clm.claim_type_code, 'X') NOT IN ('I', 'P')
             THEN NULL
        WHEN clm.claim_type_code = 'P'
         AND SUBSTR(CONCAT('00', sln.place_service), -2) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
             THEN NULL
        WHEN clm.claim_type_code = 'I'
         AND SUBSTR(COALESCE(clm.type_bill, 'X'), 1, 1) IN ('3', 'X')
             THEN NULL
        ELSE clm.billing_upin
    END                                                                                        AS prov_billing_upin,
    /* prov_billing_name_1 */
    CASE
        WHEN COALESCE(clm.claim_type_code, 'X') NOT IN ('I', 'P')
             THEN NULL
        WHEN clm.claim_type_code = 'P'
         AND SUBSTR(CONCAT('00', sln.place_service), -2) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
             THEN NULL
        WHEN clm.claim_type_code = 'I'
         AND SUBSTR(COALESCE(clm.type_bill, 'X'), 1, 1) IN ('3', 'X')
             THEN NULL
        ELSE clm.billing_name1
    END                                                                                        AS prov_billing_name_1,
    /* prov_billing_name_2 */
    CASE
        WHEN COALESCE(clm.claim_type_code, 'X') NOT IN ('I', 'P')
             THEN NULL
        WHEN clm.claim_type_code = 'P'
         AND SUBSTR(CONCAT('00', sln.place_service), -2) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
             THEN NULL
        WHEN clm.claim_type_code = 'I'
         AND SUBSTR(COALESCE(clm.type_bill, 'X'), 1, 1) IN ('3', 'X')
             THEN NULL
        ELSE clm.billing_name2
    END                                                                                        AS prov_billing_name_2,
    /* prov_billing_address_1 */
    CASE
        WHEN COALESCE(clm.claim_type_code, 'X') NOT IN ('I', 'P')
             THEN NULL
        WHEN clm.claim_type_code = 'P'
         AND SUBSTR(CONCAT('00', sln.place_service), -2) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
             THEN NULL
        WHEN clm.claim_type_code = 'I'
         AND SUBSTR(COALESCE(clm.type_bill, 'X'), 1, 1) IN ('3', 'X')
             THEN NULL
        ELSE clm.billing_adr_line1
    END                                                                                        AS prov_billing_address_1,
    /* prov_billing_address_2 */
    CASE
        WHEN COALESCE(clm.claim_type_code, 'X') NOT IN ('I', 'P')
             THEN NULL
        WHEN clm.claim_type_code = 'P'
         AND SUBSTR(CONCAT('00', sln.place_service), -2) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
             THEN NULL
        WHEN clm.claim_type_code = 'I'
         AND SUBSTR(COALESCE(clm.type_bill, 'X'), 1, 1) IN ('3', 'X')
             THEN NULL
        ELSE clm.billing_adr_line2
    END                                                                                        AS prov_billing_address_2,
    /* prov_billing_city */
    CASE
        WHEN COALESCE(clm.claim_type_code, 'X') NOT IN ('I', 'P')
             THEN NULL
        WHEN clm.claim_type_code = 'P'
         AND SUBSTR(CONCAT('00', sln.place_service), -2) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
             THEN NULL
        WHEN clm.claim_type_code = 'I'
         AND SUBSTR(COALESCE(clm.type_bill, 'X'), 1, 1) IN ('3', 'X')
             THEN NULL
        ELSE clm.billing_adr_city
    END                                                                                        AS prov_billing_city,
    /* prov_billing_state */
    VALIDATE_STATE_CODE
        (
            CASE
                WHEN COALESCE(clm.claim_type_code, 'X') NOT IN ('I', 'P')
                     THEN NULL
                WHEN clm.claim_type_code = 'P'
                 AND SUBSTR(CONCAT('00', sln.place_service), -2) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
                     THEN NULL
                WHEN clm.claim_type_code = 'I'
                 AND SUBSTR(COALESCE(clm.type_bill, 'X'), 1, 1) IN ('3', 'X')
                     THEN NULL
                ELSE clm.billing_adr_state
            END
        )                                                                                   AS prov_billing_state,
    /* prov_billing_zip */
    CASE
        WHEN COALESCE(clm.claim_type_code, 'X') NOT IN ('I', 'P')
             THEN NULL
        WHEN clm.claim_type_code = 'P'
         AND SUBSTR(CONCAT('00', sln.place_service), -2) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
             THEN NULL
        WHEN clm.claim_type_code = 'I'
         AND SUBSTR(COALESCE(clm.type_bill, 'X'), 1, 1) IN ('3', 'X')
             THEN NULL
        ELSE clm.billing_adr_zip
    END                                                                                        AS prov_billing_zip,
    clm.billing_taxonomy                                                                    AS prov_billing_std_taxonomy,
    /* prov_referring_name_1 */
    CASE
        WHEN COALESCE(clm.claim_type_code, 'X') NOT IN ('I', 'P')
             THEN NULL
        WHEN clm.claim_type_code = 'P'
         AND SUBSTR(CONCAT('00', sln.place_service), -2) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
             THEN NULL
        WHEN clm.claim_type_code = 'I'
         AND SUBSTR(COALESCE(clm.type_bill, 'X'), 1, 1) IN ('3', 'X')
             THEN NULL
        ELSE clm.referring_name1
    END                                                                                        AS prov_referring_name_1,
    /* prov_referring_name_2 */
    CASE
        WHEN COALESCE(clm.claim_type_code, 'X') NOT IN ('I', 'P')
             THEN NULL
        WHEN clm.claim_type_code = 'P'
         AND SUBSTR(CONCAT('00', sln.place_service), -2) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
             THEN NULL
        WHEN clm.claim_type_code = 'I'
         AND SUBSTR(COALESCE(clm.type_bill, 'X'), 1, 1) IN ('3', 'X')
             THEN NULL
        ELSE clm.referring_name2
    END                                                                                        AS prov_referring_name_2,
    /* prov_facility_state_license */
    CASE
        WHEN COALESCE(clm.claim_type_code, 'X') NOT IN ('I', 'P')
             THEN NULL
        WHEN clm.claim_type_code = 'P'
         AND SUBSTR(CONCAT('00', sln.place_service), -2) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
             THEN NULL
        WHEN clm.claim_type_code = 'I'
         AND SUBSTR(COALESCE(clm.type_bill, 'X'), 1, 1) IN ('3', 'X')
             THEN NULL
        ELSE clm.facility_state_lic
    END                                                                                        AS prov_facility_state_license,
    /* prov_facility_name_1 */
    CASE
        WHEN COALESCE(clm.claim_type_code, 'X') NOT IN ('I', 'P')
             THEN NULL
        WHEN clm.claim_type_code = 'P'
         AND SUBSTR(CONCAT('00', sln.place_service), -2) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
             THEN NULL
        WHEN clm.claim_type_code = 'I'
         AND SUBSTR(COALESCE(clm.type_bill, 'X'), 1, 1) IN ('3', 'X')
             THEN NULL
        ELSE clm.facility_name1
    END                                                                                        AS prov_facility_name_1,
    /* prov_facility_name_2 */
    CASE
        WHEN COALESCE(clm.claim_type_code, 'X') NOT IN ('I', 'P')
             THEN NULL
        WHEN clm.claim_type_code = 'P'
         AND SUBSTR(CONCAT('00', sln.place_service), -2) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
             THEN NULL
        WHEN clm.claim_type_code = 'I'
         AND SUBSTR(COALESCE(clm.type_bill, 'X'), 1, 1) IN ('3', 'X')
             THEN NULL
        ELSE clm.facility_name2
    END                                                                                        AS prov_facility_name_2,
    /* prov_facility_address_1 */
    CASE
        WHEN COALESCE(clm.claim_type_code, 'X') NOT IN ('I', 'P')
             THEN NULL
        WHEN clm.claim_type_code = 'P'
         AND SUBSTR(CONCAT('00', sln.place_service), -2) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
             THEN NULL
        WHEN clm.claim_type_code = 'I'
         AND SUBSTR(COALESCE(clm.type_bill, 'X'), 1, 1) IN ('3', 'X')
             THEN NULL
        ELSE clm.facility_adr_line1
    END                                                                                        AS prov_facility_address_1,
    /* prov_facility_address_2 */
    CASE
        WHEN COALESCE(clm.claim_type_code, 'X') NOT IN ('I', 'P')
             THEN NULL
        WHEN clm.claim_type_code = 'P'
         AND SUBSTR(CONCAT('00', sln.place_service), -2) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
             THEN NULL
        WHEN clm.claim_type_code = 'I'
         AND SUBSTR(COALESCE(clm.type_bill, 'X'), 1, 1) IN ('3', 'X')
             THEN NULL
        ELSE clm.facility_adr_line2
    END                                                                                        AS prov_facility_address_2,
    /* prov_facility_city */
    CASE
        WHEN COALESCE(clm.claim_type_code, 'X') NOT IN ('I', 'P')
             THEN NULL
        WHEN clm.claim_type_code = 'P'
         AND SUBSTR(CONCAT('00', sln.place_service), -2) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
             THEN NULL
        WHEN clm.claim_type_code = 'I'
         AND SUBSTR(COALESCE(clm.type_bill, 'X'), 1, 1) IN ('3', 'X')
             THEN NULL
        ELSE clm.facility_adr_city
    END                                                                                        AS prov_facility_city,
    /* prov_facility_state */
    VALIDATE_STATE_CODE
        (
            CASE
                WHEN COALESCE(clm.claim_type_code, 'X') NOT IN ('I', 'P')
                     THEN NULL
                WHEN clm.claim_type_code = 'P'
                 AND SUBSTR(CONCAT('00', sln.place_service), -2) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
                     THEN NULL
                WHEN clm.claim_type_code = 'I'
                 AND SUBSTR(COALESCE(clm.type_bill, 'X'), 1, 1) IN ('3', 'X')
                     THEN NULL
                ELSE clm.facility_adr_state
            END
        )                                                                                   AS prov_facility_state,
    /* prov_facility_zip */
    CASE
        WHEN COALESCE(clm.claim_type_code, 'X') NOT IN ('I', 'P')
             THEN NULL
        WHEN clm.claim_type_code = 'P'
         AND SUBSTR(CONCAT('00', sln.place_service), -2) IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')
             THEN NULL
        WHEN clm.claim_type_code = 'I'
         AND SUBSTR(COALESCE(clm.type_bill, 'X'), 1, 1) IN ('3', 'X')
             THEN NULL
        ELSE clm.facility_adr_zip
    END                                                                                        AS prov_facility_zip,
    'navicure'                                                                                AS part_provider,
    /* part_best_date */
    CASE
        WHEN CAP_DATE
                (
                    CAST(EXTRACT_DATE(COALESCE(sln.service_from, clm.statement_from), '%Y%m%d') AS DATE),
                    ahdt.gen_ref_1_dt,
                    CAST('${VDR_FILE_DT}' AS DATE)
                ) IS NULL
             THEN '0_PREDATES_HVM_HISTORY'
        ELSE CONCAT
                (
                    SUBSTR(COALESCE(sln.service_from, clm.statement_from), 1, 4), '-',
                    SUBSTR(COALESCE(sln.service_from, clm.statement_from), 5, 2), '-01'
                )
    END                                                                                     AS part_best_date
 FROM waystar_dedup_lines sln 
 LEFT OUTER JOIN waystar_dedup_claims clm 
   ON clm.claim_number = sln.claim_number 
 LEFT OUTER JOIN waystar_payload pay 
   ON clm.hvjoinkey = pay.hvjoinkey
 LEFT OUTER JOIN ref_gen_ref esdt
   ON 1 = 1
  AND esdt.hvm_vdr_feed_id = 24
  AND esdt.gen_ref_domn_nm = 'EARLIEST_VALID_SERVICE_DATE'
 LEFT OUTER JOIN ref_gen_ref ahdt
   ON 1 = 1
  AND ahdt.hvm_vdr_feed_id = 24
  AND ahdt.gen_ref_domn_nm = 'HVM_AVAILABLE_HISTORY_START_DATE'
CROSS JOIN (SELECT EXPLODE(ARRAY(0, 1, 2, 3, 4, 5, 6, 7)) AS n) diag_explode
WHERE
---------- Diagnosis code explosion
    (
        ARRAY
            (
                sln.diagnosis_pointer_1,
                sln.diagnosis_pointer_2,
                sln.diagnosis_pointer_3,
                sln.diagnosis_pointer_4,
                sln.diagnosis_pointer_5,
                sln.diagnosis_pointer_6,
                sln.diagnosis_pointer_7,
                sln.diagnosis_pointer_8
            )[diag_explode.n] IS NOT NULL
     OR
        (
            COALESCE
                (
                    sln.diagnosis_pointer_1,
                    sln.diagnosis_pointer_2,
                    sln.diagnosis_pointer_3,
                    sln.diagnosis_pointer_4,
                    sln.diagnosis_pointer_5,
                    sln.diagnosis_pointer_6,
                    sln.diagnosis_pointer_7,
                    sln.diagnosis_pointer_8
                ) IS NULL
        AND diag_explode.n = 0
        )
    )
