SELECT
    clm.claim_tcn_id  AS claim_id,
    clm.pay_hvid                                                                                AS hvid,
    CURRENT_DATE()                                                                          AS created,
	'10'                                                                                    AS model_version,
    SPLIT(clm.input_file_name, '/')[SIZE(SPLIT(clm.input_file_name, '/')) - 1]              AS data_set,
	'219'                                                                                   AS data_feed,
	'576'                                                                                   AS data_vendor,
    CASE
        WHEN SUBSTR(UPPER(clm.patient_gender_code), 1, 1) IN ('F', 'M') THEN SUBSTR(UPPER(clm.patient_gender_code), 1, 1)
        WHEN SUBSTR(UPPER(clm.pln_patient_gender ), 1, 1) IN ('F', 'M') THEN SUBSTR(UPPER(clm.pln_patient_gender     ), 1, 1)
        WHEN SUBSTR(UPPER(clm.pay_gender         ), 1, 1) IN ('F', 'M') THEN SUBSTR(UPPER(clm.pay_gender             ), 1, 1)
        ELSE 'U'
    END                                                                                       AS patient_gender,
    -- UDF-cap_year_of_birth(age, date_service, year_of_birth)
    CAP_YEAR_OF_BIRTH
    (
    NULL,
    TO_DATE(COALESCE(clm.statement_to_date, min_max_dt.max_service_to_date), 'yyyyMMdd') ,
    COALESCE(clm.patient_birth_year, clm.member_birth_year, clm.pay_yearofbirth)
    )                                                                                         AS patient_year_of_birth,

    MASK_ZIP_CODE(COALESCE(clm.patient_zip3, clm.pay_threedigitzip))                              AS patient_zip3,
    VALIDATE_STATE_CODE(UPPER(COALESCE(clm.patient_state, clm.pay_state)))                        AS patient_state,

    ----------------------------
    clm.claim_type	                                                                          AS claim_type,
    COALESCE(TO_DATE(clm.received_date, 'yyyyMMdd'), CAST(clm.received_date AS DATE))         AS date_received,
    CASE
        WHEN TO_DATE(COALESCE(clm.statement_from_date, min_max_dt.min_service_from_date), 'yyyyMMdd')  < CAST('{EARLIEST_SERVICE_DATE}' AS DATE)
          OR TO_DATE(COALESCE(clm.statement_from_date, min_max_dt.min_service_from_date), 'yyyyMMdd')  > CAST('{VDR_FILE_DT}' AS DATE)
          THEN NULL
        ELSE TO_DATE(COALESCE(clm.statement_from_date ,min_max_dt.min_service_from_date), 'yyyyMMdd')
    END                                                                                      AS date_service,
    CASE
        WHEN TO_DATE(COALESCE(clm.statement_to_date,min_max_dt.max_service_to_date), 'yyyyMMdd')  < CAST('{EARLIEST_SERVICE_DATE}' AS DATE)
          OR TO_DATE(COALESCE(clm.statement_to_date,min_max_dt.max_service_to_date), 'yyyyMMdd') > CAST('{VDR_FILE_DT}' AS DATE)
          THEN NULL
        ELSE TO_DATE(COALESCE(clm.statement_to_date,min_max_dt.max_service_to_date), 'yyyyMMdd')
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
        WHEN UPPER(COALESCE(clm.claim_type, 'X')) <> 'I'       THEN NULL
        WHEN SUBSTR(COALESCE(clm.claim_type, 'X'), 1, 1) = '3' THEN CONCAT('X', SUBSTR(clm.claim_type, 2))
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
    -- NULL Diag
    ---------------------------------------------------------------------------------------------------------------------------
    CAST(NULL AS STRING)                                                                    AS diagnosis_code,
    CAST(NULL AS STRING)                                                                    AS diagnosis_code_qual,
	CAST(NULL AS STRING) 																	AS diagnosis_priority,
    CAST(NULL AS STRING)                                                                    AS admit_diagnosis_ind,
    ---------------------------------------------------------------------------------------------------------------------------
    -- Procedures (not in service line)
    ---------------------------------------------------------------------------------------------------------------------------
    CASE
        WHEN ARRAY
                (clm.other_icd_proc_code_1, clm.other_icd_proc_code_2, clm.other_icd_proc_code_3, clm.other_icd_proc_code_4, clm.other_icd_proc_code_5,
                clm.other_icd_proc_code_6, clm.other_icd_proc_code_7, clm.other_icd_proc_code_8, clm.other_icd_proc_code_9
                )[claim_explode.n] IS NULL
                THEN NULL
        ELSE ARRAY
                (
                clm.other_icd_proc_code_1, clm.other_icd_proc_code_2, clm.other_icd_proc_code_3, clm.other_icd_proc_code_4, clm.other_icd_proc_code_5,
                clm.other_icd_proc_code_6, clm.other_icd_proc_code_7, clm.other_icd_proc_code_8, clm.other_icd_proc_code_9
                )[claim_explode.n]
    END                                                                                     AS procedure_code,

    CAST(NULL AS STRING)                                AS procedure_code_qual,
	CAST(NULL AS STRING) 								AS principal_proc_ind,
	CAST(NULL AS FLOAT)									AS procedure_units_billed,
	CAST(NULL AS STRING)								AS procedure_modifier_1,
	CAST(NULL AS STRING)								AS procedure_modifier_2,
	CAST(NULL AS STRING)								AS procedure_modifier_3,
	CAST(NULL AS STRING)								AS procedure_modifier_4,
	CAST(NULL AS STRING)								AS revenue_code,
	CAST(NULL AS STRING)								AS ndc_code,

    clm.claim_filing_indicator_cd                       AS medical_coverage_type,
	CAST(NULL AS FLOAT)									AS line_charge,
    clm.total_claim_charge_amt	                        AS total_charge,
   ---------------------------------------------------------------------------------------------------------------------------

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
        WHEN COALESCE(claim_type, 'X') = 'I' THEN  clm.rendering_prov_org_name
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
    clm.pas_pcn                                                                     AS medical_claim_link_text,
    'change_dx'                                                                    AS part_provider,
    CASE
        WHEN TO_DATE(COALESCE(clm.statement_from_date, min_max_dt.min_service_from_date), 'yyyyMMdd')  < CAST('{AVAILABLE_START_DATE}' AS DATE)
          OR TO_DATE(COALESCE(clm.statement_from_date, min_max_dt.min_service_from_date), 'yyyyMMdd')  > CAST('{VDR_FILE_DT}' AS DATE)
          THEN '0_PREDATES_HVM_HISTORY'
        ELSE CONCAT
                (
                    SUBSTR(COALESCE(clm.statement_from_date, min_max_dt.min_service_from_date), 1,4), '-',
                    SUBSTR(COALESCE(clm.statement_from_date, min_max_dt.min_service_from_date), 5,2), '-01'
                )
    END                                                                         AS part_best_date,
    ---------------------------------------------------------------------------------------------------------------------------
    'end'

FROM change_00_clm clm
LEFT OUTER JOIN
    (
        SELECT * FROM
            (SELECT *, row_number()  OVER (PARTITION BY claim_number ORDER BY CAST(service_line_number  AS FLOAT)  ) as rownum
                FROM change_00_srv
            ) WHERE rownum = 1
    ) sln ON UPPER(clm.claim_tcn_id) = UPPER(sln.claim_number)


LEFT OUTER JOIN
/* Get the min and max dates for each claim_number. */
(
    SELECT
        line_1.claim_number,
        MIN(line_1.service_from_date)                                   AS min_service_from_date,
        MAX(COALESCE(line_1.service_to_date, line_1.service_from_date)) AS max_service_to_date
    FROM change_00_srv line_1
    GROUP BY 1
) min_max_dt    ON UPPER(clm.claim_tcn_id) = UPPER(min_max_dt.claim_number)
-- LEFT OUTER JOIN change_payload   pay ON  UPPER(clm.claim_tcn_id) = UPPER(pay.claimid)
-- LEFT OUTER JOIN change_passthrough pas ON  UPPER(clm.claim_tcn_id) = UPPER(pas.claimid)
-- LEFT OUTER JOIN change_plainout    pln ON  UPPER(clm.claim_tcn_id) = UPPER(pln.claim_number)

CROSS JOIN (SELECT EXPLODE(ARRAY(0, 1, 2, 3, 4, 5, 6, 7, 8)) AS n) claim_explode
WHERE
--sln.claim_number = 'EI022719262571197' -- 'EP120817737521197'
---------- Exclude Dental
    COALESCE(clm.claim_type, 'X') NOT IN ('D')
    AND
---------- Diagnosis code explosion from claim table (do not add rows for empty proc codes)
        (
        ARRAY(clm.other_icd_proc_code_1,
              clm.other_icd_proc_code_2,
              clm.other_icd_proc_code_3,
              clm.other_icd_proc_code_4,
              clm.other_icd_proc_code_5,
              clm.other_icd_proc_code_6,
              clm.other_icd_proc_code_7,
              clm.other_icd_proc_code_8,
              clm.other_icd_proc_code_9)[claim_explode.n] IS NOT NULL
        --  OR
        --     (
        --     COALESCE(clm.other_icd_proc_code_1,
        --              clm.other_icd_proc_code_2,
        --              clm.other_icd_proc_code_3,
        --              clm.other_icd_proc_code_4,
        --              clm.other_icd_proc_code_5,
        --              clm.other_icd_proc_code_6,
        --              clm.other_icd_proc_code_7,
        --              clm.other_icd_proc_code_8,
        --              clm.other_icd_proc_code_9) IS NULL AND claim_explode.n = 0
        --     )
        )
---------- EXCLUDE DENTAL claim
    AND COALESCE(clm.claim_type, 'X') NOT IN ('D')
----------- NOT EXIST IN already created table
    AND NOT EXISTS
    (
        SELECT 1
        FROM change_01_pvt norm_01
        WHERE clm.claim_tcn_id = norm_01.claim_id
          AND norm_01.procedure_code =
            ARRAY
                (
                clm.other_icd_proc_code_1,
                clm.other_icd_proc_code_2,
                clm.other_icd_proc_code_3,
                clm.other_icd_proc_code_4,
                clm.other_icd_proc_code_5,
                clm.other_icd_proc_code_6,
                clm.other_icd_proc_code_7,
                clm.other_icd_proc_code_8,
                clm.other_icd_proc_code_9
                )[claim_explode.n]

    )

--LIMIT 100
