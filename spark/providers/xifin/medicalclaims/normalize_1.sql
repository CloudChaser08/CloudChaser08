SELECT
    CONCAT(proc.client_id, '_', proc.accn_id)                                 AS claim_id,
    demo.hvid                                                                 AS hvid,
    proc.xifin_input_file_name                                                AS data_set,
    CASE
    WHEN SUBSTRING(TRIM(COALESCE(demo.sex, demo.gender, '')), 1, 1) IN ('F', 'M', 'U')
    THEN SUBSTRING(TRIM(COALESCE(demo.sex, demo.gender, '')), 1, 1)
    ELSE 'U'
    END                                                                       AS patient_gender,
    COALESCE(demo.pt_age, demo.age)                                           AS patient_age,
    COALESCE(YEAR(demo.dob), demo.yearOfBirth)                                AS patient_year_of_birth,
    SUBSTRING(TRIM(COALESCE(demo.pt_zipcode, demo.threedigitzip, '')), 1, 3)  AS patient_zip3,
    UPPER(TRIM(COALESCE(demo.pt_st_id, demo.state, '')))                      AS patient_state,
    EXTRACT_DATE(demo.load_date, '%m/%d/%Y', CAST({min_date} as date))        AS date_received,
    EXTRACT_DATE(demo.dos, '%m/%d/%Y', CAST({min_date} as date))              AS date_service,
    EXTRACT_DATE(demo.final_rpt_date, '%m/%d/%Y', CAST({min_date} as date))   AS date_service_end,
    EXTRACT_DATE(demo.admission_date, '%m/%d/%Y', CAST({min_date} as date))   AS inst_date_admitted,
    EXTRACT_DATE(demo.discharge_dt, '%m/%d/%Y', CAST({min_date} as date))     AS inst_date_discharged,
    ARRAY(
        proc.diag_code_1, proc.diag_code_2, proc.diag_code_3, proc.diag_code_4, diag.diag_code
        )[proc_diag_exploder.n]                                               AS diagnosis_code,
    CASE
    WHEN diag.diag_code IS NOT NULL AND proc_diag_exploder.n = 4
    THEN (
        CASE
        WHEN SUBSTRING(TRIM(diag.diagnosis_code_table), 1, 6) = 'ICD-9-'
        THEN '01'
        WHEN SUBSTRING(TRIM(diag.diagnosis_code_table), 1, 6) = 'ICD-10'
        THEN '02'
        END
        )
    END                                                                       AS diagnosis_code_qual,

-- use procedure proiority if it exists and is relevant to this row
    CASE
    WHEN ARRAY(
        proc.diag_code_1, proc.diag_code_2, proc.diag_code_3, proc.diag_code_4, diag.diag_code
        )[proc_diag_exploder.n] IS NULL THEN NULL
    WHEN proc_diag_exploder.n <= 3 AND ARRAY(
        proc.diag_code_1, proc.diag_code_2, proc.diag_code_3, proc.diag_code_4, diag.diag_code
        )[proc_diag_exploder.n] IS NOT NULL
    THEN proc_diag_exploder.n + 1

-- not procedure priority, offset priority by 4 to make sure we're
-- always greater than the max procedure priority. We'll fix any gaps
-- later.
    ELSE diag.diag_sequence + 4
    END                                                                       AS diagnosis_priority_unranked,
    proc.proc_code                                                            AS procedure_code,
    CASE
    WHEN proc.proc_code IS NOT NULL
        THEN 'HC'
    END                                                                       AS procedure_code_qual,
    proc.units_billed                                                         AS procedure_units_billed,
    proc.units_paid                                                           AS procedure_units_paid,
    COALESCE(proc.modifier_1, test.modifier_1)                                AS procedure_modifier_1,
    COALESCE(proc.modifier_2, test.modifier_2)                                AS procedure_modifier_2,
    COALESCE(proc.modifier_3, test.modifier_3)                                AS procedure_modifier_3,
    COALESCE(proc.modifier_4, test.modifier_4)                                AS procedure_modifier_4,
    proc.bill_price                                                           AS line_charge,
    demo.gross_price                                                          AS total_charge,
    demo.bill_price                                                           AS total_allowed,
    demo.ordering_npi                                                         AS prov_referring_npi,
    payor1.payor_id                                                           AS payer_vendor_id,
    payor1.payor_name                                                         AS payer_name,
    demo.primary_upin                                                         AS prov_rendering_upin,
    proc.billing_facility_id                                                  AS prov_billing_vendor_id,
    COALESCE(demo.ordering_upin, demo.referring_upin)                         AS prov_referring_upin,
    SUBSTRING(
        demo.ordering_phys_name, 1, LOCATE(',', demo.ordering_phys_name) - 1
        )                                                                     AS prov_referring_name_1,
    SUBSTRING(
        demo.ordering_phys_name, LOCATE(',', demo.ordering_phys_name) + 1, 999
        )                                                                     AS prov_referring_name_2,
    MD5(ARRAY(proc.place_of_svc, test.place_of_svc)[pos_explode.n])           AS prov_facility_vendor_id,
    payor2.payor_id                                                           AS cob_payer_vendor_id_1,
    payor2.payor_priority                                                     AS cob_payer_seq_code_1,
    payor3.payor_id                                                           AS cob_payer_vendor_id_2,
    payor3.payor_priority                                                     AS cob_payer_seq_code_2,
    proc.test_id                                                              AS vendor_test_id,
    test.test_name                                                            AS vendor_test_name,
    EXTRACT_DATE(demo.accounting_date, '%m/%d/%Y', CAST({min_date} AS DATE))  AS claim_transaction_date,
    CASE WHEN EXTRACT_DATE(demo.accounting_date, '%m/%d/%Y', CAST({min_date} AS DATE)) IS NOT NULL
        THEN 'DEMO.ACCOUNTING_DATE'
    END                                                                       AS claim_transaction_date_qual,
    ARRAY(
        demo.retro_bill_price, demo.retro_trade_discount_amount, demo.trade_discount_amount, demo.due_amt, demo.expect_price
        )[claim_transaction_amount_exploder.n]                                AS claim_transaction_amount,
    CASE
    WHEN claim_transaction_amount_exploder.n = 0 AND ARRAY(
        demo.retro_bill_price, demo.retro_trade_discount_amount, demo.trade_discount_amount, demo.due_amt, demo.expect_price
        )[claim_transaction_amount_exploder.n] IS NOT NULL
    THEN 'DEMO.RETRO_BILL_PRICE'
    WHEN claim_transaction_amount_exploder.n = 1 AND ARRAY(
        demo.retro_bill_price, demo.retro_trade_discount_amount, demo.trade_discount_amount, demo.due_amt, demo.expect_price
        )[claim_transaction_amount_exploder.n] IS NOT NULL
    THEN 'DEMO.RETRO_TRADE_DISCOUNT_AMOUNT'
    WHEN claim_transaction_amount_exploder.n = 2 AND ARRAY(
        demo.retro_bill_price, demo.retro_trade_discount_amount, demo.trade_discount_amount, demo.due_amt, demo.expect_price
        )[claim_transaction_amount_exploder.n] IS NOT NULL
    THEN 'DEMO.TRADE_DISCOUNT_AMOUNT'
    WHEN claim_transaction_amount_exploder.n = 3 AND ARRAY(
        demo.retro_bill_price, demo.retro_trade_discount_amount, demo.trade_discount_amount, demo.due_amt, demo.expect_price
        )[claim_transaction_amount_exploder.n] IS NOT NULL
    THEN 'DEMO.DUE_AMT'
    WHEN claim_transaction_amount_exploder.n = 4 AND ARRAY(
        demo.retro_bill_price, demo.retro_trade_discount_amount, demo.trade_discount_amount, demo.due_amt, demo.expect_price
        )[claim_transaction_amount_exploder.n] IS NOT NULL
    THEN 'DEMO.EXPECT_PRICE'
    END                                                                       AS claim_transaction_amount_qual,
    demo.status                                                               AS logical_delete_reason
FROM billed_procedures_complete proc
    LEFT OUTER JOIN diagnosis_complete diag 
        ON proc.accn_id = diag.accn_id
        AND proc.client_id = diag.client_id
        AND proc.test_id = diag.test_id
        AND 0 <> LENGTH(TRIM(COALESCE(diag.test_id, '')))
        AND COALESCE(diag.diag_code, 'dummy1') <> COALESCE(proc.diag_code_1, 'dummy2')
        AND COALESCE(diag.diag_code, 'dummy1') <> COALESCE(proc.diag_code_2, 'dummy2')
        AND COALESCE(diag.diag_code, 'dummy1') <> COALESCE(proc.diag_code_3, 'dummy2')
        AND COALESCE(diag.diag_code, 'dummy1') <> COALESCE(proc.diag_code_4, 'dummy2')
    LEFT OUTER JOIN tests_complete test
        ON proc.accn_id = test.accn_id
        AND proc.client_id = test.client_id
        AND proc.test_id = test.test_id
        AND 0 <> LENGTH(TRIM(COALESCE(test.test_id, '')))
        AND COALESCE(proc.proc_code, '') = COALESCE(test.proc_code, '')
        AND 0 <> LENGTH(TRIM(COALESCE(proc.proc_code, ''))) 
    LEFT OUTER JOIN demographics_complete demo
        ON proc.accn_id = demo.accn_id
        AND proc.client_id = demo.client_id
    LEFT OUTER JOIN payors_complete payor1
        ON proc.accn_id = payor1.accn_id
        AND proc.client_id = payor1.client_id
        AND payor1.payor_priority = 1
    LEFT OUTER JOIN payors_complete payor2
        ON proc.accn_id = payor2.accn_id
        AND proc.client_id = payor2.client_id
        AND payor2.payor_priority = 2
    LEFT OUTER JOIN payors_complete payor3
        ON proc.accn_id = payor3.accn_id
        AND proc.client_id = payor3.client_id
        AND payor3.payor_priority = 3
    CROSS JOIN proc_test_exploder pos_explode
    CROSS JOIN proc_diag_exploder
    CROSS JOIN claim_transaction_amount_exploder
WHERE COALESCE(demo.pt_country, 'dummy') = 'USA' AND (
        ARRAY(proc.place_of_svc, test.place_of_svc)[pos_explode.n] IS NOT NULL
        OR (
            COALESCE(proc.place_of_svc, test.place_of_svc) IS NULL AND pos_explode.n = 0
            )
        ) AND ((
            proc.place_of_svc = test.place_of_svc AND pos_explode.n = 0
            ) OR proc.place_of_svc != test.place_of_svc OR proc.place_of_svc IS NULL OR test.place_of_svc IS NULL
        ) AND (
        ARRAY(
            proc.diag_code_1, proc.diag_code_2, proc.diag_code_3, proc.diag_code_4, diag.diag_code
            )[proc_diag_exploder.n] IS NOT NULL
        OR (
            COALESCE(
                proc.diag_code_1, proc.diag_code_2, proc.diag_code_3, proc.diag_code_4, diag.diag_code
                ) IS NULL AND proc_diag_exploder.n = 0
            )
        ) AND (
        ARRAY(
            demo.retro_bill_price, demo.retro_trade_discount_amount, demo.trade_discount_amount, demo.due_amt, demo.expect_price
            )[claim_transaction_amount_exploder.n] IS NOT NULL
        OR (
            COALESCE(
                demo.retro_bill_price, demo.retro_trade_discount_amount, demo.trade_discount_amount, demo.due_amt, demo.expect_price
                ) IS NULL AND claim_transaction_amount_exploder.n = 0
            )
        )
