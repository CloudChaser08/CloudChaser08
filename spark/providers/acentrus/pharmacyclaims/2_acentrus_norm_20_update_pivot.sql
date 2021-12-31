SELECT
    txn.claim_id,
    txn.data_set,
    txn.diagnosis_code,
    txn.diagnosis_code_qual,
    txn.primary_pharmacy_payor_id_code                            AS payer_id, CAST(NULL AS STRING) AS payer_id_qual,
    txn.primary_pharmacy_payor_bin                                AS bin_number,
    txn.primary_pharmacy_payor_pcn                                AS processor_control_number,
    txn.primary_pharmacy_payor_copay                              AS copay_coinsurance,
    txn.primary_pharmacy_payor_out_of_pocket_oop_maximum          AS remaining_deductible,
    txn.primary_pharmacy_payor_out_of_pocket_oop_amount_paid      AS periodic_benefit_exceed,
    'end'

FROM acentrus_norm_10_pivot txn
WHERE
    ---------------------------------------------------------------------------------------------------------------------------
    -- No Payor Info (Primary and Secondary)
    ---------------------------------------------------------------------------------------------------------------------------
        txn.primary_pharmacy_payor_bin             IS NULL
    AND txn.primary_pharmacy_payor_pcn             IS NULL
    AND txn.primary_pharmacy_payor_group_number    IS NULL
    AND txn.primary_pharmacy_payor_copay           IS NULL
    AND txn.primary_pharmacy_payor_out_of_pocket_oop_maximum     IS NULL
    AND txn.primary_pharmacy_payor_out_of_pocket_oop_amount_paid IS NULL

    AND txn.secondary_pharmacy_payor_bin             IS NULL
    AND txn.secondary_pharmacy_payor_pcn             IS NULL
    AND txn.secondary_pharmacy_payor_group_number    IS NULL
    AND txn.secondary_pharmacy_payor_copay           IS NULL
    AND txn.secondary_pharmacy_payor_out_of_pocket_oop_maximum     IS NULL
    AND txn.secondary_pharmacy_payor_out_of_pocket_oop_amount_paid IS NULL
UNION ALL
SELECT
    txn.claim_id,
    txn.data_set,
    txn.diagnosis_code,
    txn.diagnosis_code_qual,
    txn.primary_pharmacy_payor_id_code                            AS payer_id, 'PRIMARY' AS payer_id_qual,
    txn.primary_pharmacy_payor_bin                                AS bin_number,
    txn.primary_pharmacy_payor_pcn                                AS processor_control_number,
    txn.primary_pharmacy_payor_copay                              AS copay_coinsurance,
    txn.primary_pharmacy_payor_out_of_pocket_oop_maximum          AS remaining_deductible,
    txn.primary_pharmacy_payor_out_of_pocket_oop_amount_paid      AS periodic_benefit_exceed,
    'end'

FROM acentrus_norm_10_pivot txn
WHERE
    ---------------------------------------------------------------------------------------------------------------------------
    -- Payor Info (Primary)
    ---------------------------------------------------------------------------------------------------------------------------
        txn.primary_pharmacy_payor_bin            IS NOT NULL
    OR txn.primary_pharmacy_payor_pcn             IS NOT NULL
    OR txn.primary_pharmacy_payor_group_number    IS NOT NULL
    OR txn.primary_pharmacy_payor_copay           IS NOT NULL
    OR txn.primary_pharmacy_payor_out_of_pocket_oop_maximum     IS NOT NULL
    OR txn.primary_pharmacy_payor_out_of_pocket_oop_amount_paid IS NOT NULL

UNION ALL
SELECT
    txn.claim_id,
    txn.data_set,
    txn.diagnosis_code,
    txn.diagnosis_code_qual,
    txn.secondary_pharmacy_payor_id_code                            AS payer_id, 'SECONDARY' AS payer_id_qual,
    txn.secondary_pharmacy_payor_bin                                AS bin_number,
    txn.secondary_pharmacy_payor_pcn                                AS processor_control_number,
    txn.secondary_pharmacy_payor_copay                              AS copay_coinsurance,
    txn.secondary_pharmacy_payor_out_of_pocket_oop_maximum          AS remaining_deductible,
    txn.secondary_pharmacy_payor_out_of_pocket_oop_amount_paid      AS periodic_benefit_exceed,
    'end'

FROM acentrus_norm_10_pivot txn
WHERE
    ---------------------------------------------------------------------------------------------------------------------------
    -- Payor Info (secondary)
    ---------------------------------------------------------------------------------------------------------------------------
       txn.secondary_pharmacy_payor_bin             IS NOT NULL
    OR txn.secondary_pharmacy_payor_pcn             IS NOT NULL
    OR txn.secondary_pharmacy_payor_group_number    IS NOT NULL
    OR txn.secondary_pharmacy_payor_copay           IS NOT NULL
    OR txn.secondary_pharmacy_payor_out_of_pocket_oop_maximum     IS NOT NULL
    OR txn.secondary_pharmacy_payor_out_of_pocket_oop_amount_paid IS NOT NULL
