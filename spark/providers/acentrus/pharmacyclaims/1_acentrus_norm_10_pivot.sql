SELECT
    txn.row_level_id                                                                                 AS claim_id,
    SPLIT(txn.input_file_name, '/')[SIZE(SPLIT(txn.input_file_name, '/')) - 1]                       AS data_set,
    ---------------------------------------------------------------------------------------------------------------------------
    -- Diag
    ---------------------------------------------------------------------------------------------------------------------------
    CASE
        WHEN diag_explode.n = 0 THEN txn.icd9cm_primary_diagnosis_related_to_current_prescription_order
        WHEN diag_explode.n = 1 THEN txn.icd10cm_primary_diagnosis_related_to_current_prescription_order
    END                                                                                              AS diagnosis_code,

    CASE
        WHEN diag_explode.n = 0 THEN '01'
        WHEN diag_explode.n = 1 THEN '02'
    ELSE NULL
    END                                                                                              AS diagnosis_code_qual,
    ---------------------------------------------------------------------------------------------------------------------------
    CASE
        WHEN txn.primary_pharmacy_payor_id_code = '.' THEN NULL
    ELSE txn.primary_pharmacy_payor_id_code
    END                                                                                              AS primary_pharmacy_payor_id_code,

    CASE
        WHEN txn.primary_pharmacy_payor_bin = '.' THEN NULL
    ELSE txn.primary_pharmacy_payor_bin
    END                                                                                              AS primary_pharmacy_payor_bin,
    CASE
        WHEN txn.primary_pharmacy_payor_pcn = '.' THEN NULL
    ELSE txn.primary_pharmacy_payor_pcn
    END                                                                                              AS primary_pharmacy_payor_pcn,
    CASE
        WHEN txn.primary_pharmacy_payor_group_number = '.' THEN NULL
    ELSE txn.primary_pharmacy_payor_group_number
    END                                                                                              AS primary_pharmacy_payor_group_number,
    CASE
        WHEN txn.primary_pharmacy_payor_copay = '.' THEN NULL
    ELSE txn.primary_pharmacy_payor_copay
    END                                                                                              AS primary_pharmacy_payor_copay,
    CASE
        WHEN txn.primary_pharmacy_payor_out_of_pocket_oop_maximum = '.' THEN NULL
    ELSE txn.primary_pharmacy_payor_out_of_pocket_oop_maximum
    END                                                                                              AS primary_pharmacy_payor_out_of_pocket_oop_maximum,
    CASE
        WHEN txn.primary_pharmacy_payor_out_of_pocket_oop_amount_paid  = '.' THEN NULL
    ELSE txn.primary_pharmacy_payor_out_of_pocket_oop_amount_paid
    END                                                                                              AS primary_pharmacy_payor_out_of_pocket_oop_amount_paid,

    CASE
        WHEN txn.secondary_pharmacy_payor_id_code  = '.' THEN NULL
    ELSE txn.secondary_pharmacy_payor_id_code
    END                                                                                              AS secondary_pharmacy_payor_id_code,

    CASE
        WHEN txn.secondary_pharmacy_payor_bin  = '.' THEN NULL
    ELSE txn.secondary_pharmacy_payor_bin
    END                                                                                              AS secondary_pharmacy_payor_bin,
    CASE
        WHEN txn.secondary_pharmacy_payor_pcn = '.' THEN NULL
    ELSE txn.secondary_pharmacy_payor_pcn
    END                                                                                              AS secondary_pharmacy_payor_pcn,

     CASE
        WHEN txn.secondary_pharmacy_payor_group_number = '.' THEN NULL
    ELSE txn.secondary_pharmacy_payor_group_number
    END                                                                                              AS secondary_pharmacy_payor_group_number,

    CASE
        WHEN txn.secondary_pharmacy_payor_copay = '.' THEN NULL
    ELSE txn.secondary_pharmacy_payor_copay
    END                                                                                              AS secondary_pharmacy_payor_copay,

    CASE
        WHEN txn.secondary_pharmacy_payor_out_of_pocket_oop_maximum = '.' THEN NULL
    ELSE txn.secondary_pharmacy_payor_out_of_pocket_oop_maximum
    END                                                                                              AS secondary_pharmacy_payor_out_of_pocket_oop_maximum,
    CASE
        WHEN txn.secondary_pharmacy_payor_out_of_pocket_oop_amount_paid = '.' THEN NULL
    ELSE txn.secondary_pharmacy_payor_out_of_pocket_oop_amount_paid
    END                                                                                              AS secondary_pharmacy_payor_out_of_pocket_oop_amount_paid,
    'end'
FROM acentrus_norm_00_dedup txn

CROSS JOIN (SELECT EXPLODE(ARRAY(0, 1)) AS n) diag_explode
WHERE TRIM(UPPER(COALESCE(txn.row_level_id, 'empty')))  <> 'ROW LEVEL ID'
AND
---------- Diagnosis code explosion  (Do not explode if there is no diagnosis code)
         (
         ARRAY(txn.icd9cm_primary_diagnosis_related_to_current_prescription_order, txn.icd10cm_primary_diagnosis_related_to_current_prescription_order)[diag_explode.n] IS NOT NULL
--          OR
--             (
--             COALESCE(txn.icd9cm_primary_diagnosis_related_to_current_prescription_order, txn.icd10cm_primary_diagnosis_related_to_current_prescription_order) IS NULL AND diag_explode.n = 0
--             )
         )
