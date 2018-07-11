SELECT
    CONCAT(test.client_id, '_', test.accn_id)                                 AS claim_id,
    demo.hvid                                                                 AS hvid,
    test.xifin_input_file_name                                                AS data_set,
    CASE
    WHEN SUBSTRING(TRIM(COALESCE(demo.sex, demo.gender, '')), 1, 1) IN ('F', 'M', 'U')
    THEN SUBSTRING(TRIM(COALESCE(demo.sex, demo.gender, '')), 1, 1)
    ELSE 'U'
    END                                                                       AS patient_gender,
    COALESCE(demo.pt_age, demo.age)                                           AS patient_age,
    COALESCE(YEAR(demo.dob), demo.yearOfBirth)                                AS patient_year_of_birth,
    SUBSTRING(TRIM(COALESCE(demo.pt_zipcode, demo.threedigitzip, '')), 1, 3)  AS patient_zip3,
    UPPER(TRIM(COALESCE(demo.pt_st_id, demo.state, '')))                      AS patient_state,
    EXTRACT_DATE(demo.load_date, '%m/%d/%Y', CAST({min_date} AS DATE))        AS date_received,
    EXTRACT_DATE(demo.dos, '%m/%d/%Y', CAST({min_date} AS DATE))              AS date_service,
    EXTRACT_DATE(demo.final_rpt_date, '%m/%d/%Y', CAST({min_date} AS DATE))   AS date_service_end,
    EXTRACT_DATE(demo.admission_date, '%m/%d/%Y', CAST({min_date} AS DATE))   AS inst_date_admitted,
    EXTRACT_DATE(demo.discharge_dt, '%m/%d/%Y', CAST({min_date} AS DATE))     AS inst_date_discharged,
    MD5(test.place_of_svc)                                                    AS place_of_service_vendor_id,
    diag.diag_code                                                            AS diagnosis_code,
    CASE
    WHEN diag.diag_code IS NOT NULL
    THEN (
        CASE
        WHEN SUBSTRING(TRIM(diag.diagnosis_code_table), 1, 6) = 'ICD-9-'
        THEN '01'
        WHEN SUBSTRING(TRIM(diag.diagnosis_code_table), 1, 6) = 'ICD-10'
        THEN '02'
        END
        )
    END                                                                       AS diagnosis_code_qual,

-- offset diagnosis sequence to ensure any existing procedure sequence
-- number comes first when ranked
    CASE
    WHEN diag.diag_code IS NOT NULL
    THEN diag.diag_sequence + 4
    END                                                                       AS diagnosis_priority_unranked,
    test.proc_code                                                            AS procedure_code,
    CASE
    WHEN test.proc_code IS NOT NULL
    THEN 'HC'
    END                                                                       AS procedure_code_qual,
    test.modifier_1                                                           AS procedure_modifier_1,
    test.modifier_2                                                           AS procedure_modifier_2,
    test.modifier_3                                                           AS procedure_modifier_3,
    test.modifier_4                                                           AS procedure_modifier_4,
    demo.gross_price                                                          AS total_charge,
    demo.bill_price                                                           AS total_allowed,
    demo.ordering_npi                                                         AS prov_referring_npi,
    payor1.payor_id                                                           AS payer_vendor_id,
    payor1.payor_name                                                         AS payer_name,
    demo.primary_upin                                                         AS prov_rendering_upin,
    COALESCE(demo.ordering_upin, demo.referring_upin)                         AS prov_referring_upin,
    SUBSTRING(
        demo.ordering_phys_name, 1, LOCATE(',', demo.ordering_phys_name) - 1
        )                                                                     AS prov_referring_name_1,
    SUBSTRING(
        demo.ordering_phys_name, LOCATE(',', demo.ordering_phys_name) + 1, 999
        )                                                                     AS prov_referring_name_2,
    proc.billing_facility_id                                                  AS prov_facility_vendor_id,
    payor2.payor_id                                                           AS cob_payer_vendor_id_1,
    payor2.payor_priority                                                     AS cob_payer_seq_code_1,
    payor3.payor_id                                                           AS cob_payer_vendor_id_2,
    payor3.payor_priority                                                     AS cob_payer_seq_code_2,
    test.test_id                                                              AS vendor_test_id,
    test.test_name                                                            AS vendor_test_name,
    EXTRACT_DATE(demo.accounting_date, '%m/%d/%Y', CAST({min_date} AS DATE))  AS claim_transaction_date,
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
    END                                                                       AS claim_transaction_amount_qual
FROM ordered_tests_complete test
    LEFT OUTER JOIN diagnosis_complete diag ON test.accn_id = diag.accn_id
    AND test.client_id = diag.client_id
    AND test.test_id = diag.test_id
    AND  0 <> LENGTH(TRIM(COALESCE(diag.test_id, '')))
    LEFT OUTER JOIN (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY accn_id, client_id ORDER BY test_id) AS row_num
    FROM billed_procedures_complete
        ) proc
    ON test.accn_id = proc.accn_id
    AND test.client_id = proc.client_id
    AND proc.row_num = 1
    LEFT OUTER JOIN demographics_complete demo ON test.accn_id = demo.accn_id
    AND test.client_id = demo.client_id
    LEFT OUTER JOIN payors_complete payor1 ON test.accn_id = payor1.accn_id
    AND test.client_id = payor1.client_id
    AND payor1.payor_priority = 1
    LEFT OUTER JOIN payors_complete payor2 ON test.accn_id = payor2.accn_id
    AND test.client_id = payor2.client_id
    AND payor2.payor_priority = 2
    LEFT OUTER JOIN payors_complete payor3 ON test.accn_id = payor3.accn_id
    AND test.client_id = payor3.client_id
    AND payor3.payor_priority = 3
    CROSS JOIN claim_transaction_amount_exploder
WHERE NOT EXISTS (
    SELECT 1 FROM billed_procedures_complete pr
    WHERE test.accn_id = pr.accn_id
        AND  test.client_id = pr.client_id
        AND  test.test_id = pr.test_id
        AND 0 <> LENGTH(TRIM(COALESCE(pr.test_id, '')))
        AND 0 <> LENGTH(TRIM(COALESCE(test.test_id, '')))
        )
    AND NOT EXISTS (
    SELECT 1 FROM billed_procedures_complete pr
    WHERE  test.accn_id = pr.accn_id
        AND  test.client_id = pr.client_id
        AND test.proc_code = proc.proc_code
        AND 0 =  LENGTH(TRIM(COALESCE(test.test_id,'')))
        )
    AND demo.pt_country ='USA'
    AND (
        ARRAY(
            demo.retro_bill_price, demo.retro_trade_discount_amount, demo.trade_discount_amount, demo.due_amt,
            demo.expect_price
            )[claim_transaction_amount_exploder.n] IS NOT NULL
        OR (
            COALESCE(
                demo.retro_bill_price, demo.retro_trade_discount_amount, demo.trade_discount_amount, demo.due_amt,
                demo.expect_price
                ) IS NULL AND claim_transaction_amount_exploder.n = 0
            )
        )
