SELECT
    CONCAT(diag.client_id, '_', diag.accn_id)                                 AS claim_id,
    demo.hvid                                                                 AS hvid,
    diag.xifin_input_file_name                                                AS data_set,
    CASE
    WHEN SUBSTRING(TRIM(COALESCE(demo.sex, demo.gender, '')), 1, 1) IN ('F', 'M', 'U')
    THEN SUBSTRING(TRIM(COALESCE(demo.sex, demo.gender, '')), 1, 1)
    ELSE 'U'
    END                                                                       AS patient_gender,
    COALESCE(demo.pt_age, demo.age)                                           AS patient_age,
    COALESCE(YEAR(demo.dob), demo.yearOfBirth)                                AS patient_year_of_birth,
    SUBSTRING(TRIM(COALESCE(demo.pt_zipcode, demo.threedigitzip, '')), 1, 3)  AS patient_zip3,
    UPPER(TRIM(COALESCE(demo.pt_st_id, demo.state, '')))                      AS patient_state,
    EXTRACT_DATE(demo.load_date, '%m/%d/%Y')                                  AS date_received,
    EXTRACT_DATE(demo.dos, '%m/%d/%Y')                                        AS date_service,
    EXTRACT_DATE(demo.final_rpt_date, '%m/%d/%Y')                             AS date_service_end,
    EXTRACT_DATE(demo.admission_date, '%m/%d/%Y')                             AS inst_date_admitted,
    EXTRACT_DATE(demo.discharge_dt, '%m/%d/%Y')                               AS inst_date_discharged,
    MD5(proc.place_of_svc)                                                    AS place_of_service_vendor_id,
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
    diag.diag_sequence + 4                                                    AS diagnosis_priority_unranked,
    demo.gross_price                                                          AS total_charge,
    demo.bill_price                                                           AS total_allowed,
    demo.ordering_npi                                                         AS prov_referring_npi,
    CASE
    WHEN payor.payor_priority = 1 THEN payor.payor_id
    END                                                                       AS payer_vendor_id,
    CASE
    WHEN payor.payor_priority = 1 THEN payor.payor_name
    END                                                                       AS payer_name,
    demo.primary_upin                                                         AS prov_rendering_upin,
    COALESCE(demo.ordering_upin, demo.referring_upin)                         AS prov_referring_upin,
    SUBSTRING(
        demo.ordering_phys_name, 1, LOCATE(', ', demo.ordering_phys_name) - 1
        )                                                                     AS prov_referring_name_1,
    SUBSTRING(
        demo.ordering_phys_name, LOCATE(', ', demo.ordering_phys_name) + 1, 999
        )                                                                     AS prov_referring_name_2,
    proc.billing_facility_id                                                  AS prov_facility_vendor_id,
    CASE
    WHEN payor.payor_priority = 2 THEN payor.payor_id
    END                                                                       AS cob_payer_vendor_id_1,
    CASE
    WHEN payor.payor_priority = 2 THEN payor.payor_priority
    END                                                                       AS cob_payer_seq_code_1,
    CASE
    WHEN payor.payor_priority = 3 THEN payor.payor_id
    END                                                                       AS cob_payer_vendor_id_2,
    CASE
    WHEN payor.payor_priority = 3 THEN payor.payor_priority
    END                                                                       AS cob_payer_seq_code_2,
    diag.test_id                                                              AS vendor_test_id,
    EXTRACT_DATE(demo.accounting_date, '%m/%d/%Y')                            AS claim_transaction_date,
    ARRAY(
        demo.retro_bill_price, demo.retro_trade_discount_amount, demo.trade_discount_amount, demo.due_amt,
        demo.expect_price
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
FROM diagnosis_complete diag
    LEFT OUTER JOIN (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY accn_id, client_id ORDER BY test_id) AS row_num
    FROM billed_procedures_complete
        ) proc
    ON diag.accn_id = proc.accn_id
    AND diag.client_id = proc.client_id
    AND proc.row_num = 1
    LEFT OUTER JOIN demographics_complete demo ON diag.accn_id = demo.accn_id
    AND diag.client_id = demo.client_id
    LEFT OUTER JOIN payors_complete payor ON diag.accn_id = payor.accn_id
    AND diag.client_id = payor.client_id
    CROSS JOIN claim_transaction_amount_exploder
WHERE NOT EXISTS (
    SELECT 1 FROM billed_procedures_complete pr
    WHERE  diag.accn_id = pr.accn_id
        AND  diag.client_id = pr.client_id
        AND  diag.test_id = pr.test_id
        AND 0 <>  LENGTH(TRIM(COALESCE(diag.test_id,'')))
        AND 0 <> LENGTH(TRIM(COALESCE(pr.test_id, '')))
        )
    AND NOT EXISTS (
    SELECT 1 FROM ordered_tests_complete t
    WHERE  diag.accn_id = t.accn_id
        AND  diag.client_id = t.client_id
        AND  diag.test_id = t.test_id
        AND 0 <>  LENGTH(TRIM(COALESCE(diag.test_id,'')))
        AND 0 <> LENGTH(TRIM(COALESCE(t.test_id, '')))
        )
    AND NOT EXISTS (
    SELECT 1 FROM billed_procedures_complete pr
    WHERE  diag.accn_id = pr.accn_id
        AND  diag.client_id = pr.client_id
        AND 0 = LENGTH(TRIM(COALESCE(diag.test_id,'')))
        AND COALESCE(diag.diag_code, 'dummy1') <> COALESCE(proc.diag_code_1, 'dummy2')
        AND COALESCE(diag.diag_code, 'dummy1') <> COALESCE(proc.diag_code_2, 'dummy2')
        AND COALESCE(diag.diag_code, 'dummy1') <> COALESCE(proc.diag_code_3, 'dummy2')
        AND COALESCE(diag.diag_code, 'dummy1') <> COALESCE(proc.diag_code_4, 'dummy2')
        )
    AND demo.pt_country ='USA'
    AND (
        ARRAY(
            demo.retro_bill_price, demo.retro_trade_discount_amount, demo.trade_discount_amount, demo.due_amt,
            demo.expect_price
            )[claim_transaction_amount_exploder.n] IS NOT NULL
        OR (
            COALESCE(
                demo.retro_bill_price, demo.retro_trade_discount_amount, demo.trade_discount_amount,
                demo.due_amt, demo.expect_price
                ) IS NULL AND claim_transaction_amount_exploder.n = 0
            )
        )
