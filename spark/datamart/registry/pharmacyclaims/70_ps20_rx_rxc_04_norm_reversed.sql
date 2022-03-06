SELECT
    txn.claim_id,
    txn.hvid,
    txn.created,
    txn.model_version,
    txn.data_set,
    txn.data_feed,
    txn.data_vendor,
    txn.patient_gender,
    txn.patient_year_of_birth,
    txn.date_service,
    txn.transaction_code_vendor,
    txn.ndc_code,
    txn.dispensed_quantity,
    txn.days_supply,
    ----- Added 2022-03-01
    txn.prov_prescribing_std_taxonomy,
    ---- Will be added after approval
    --txn.submitted_gross_due,
    txn.prov_dispensing_id,
    txn.prov_dispensing_qual,
	txn.prov_prescribing_id,
	txn.prov_prescribing_qual,
    'Reversed Claim' AS logical_delete_reason,
    txn.stg_file_date,
    txn.part_provider,
    txn.part_best_date
FROM ps20_rx_rxc_04_norm_pre_reversed txn
WHERE EXISTS
------------- Reversed Exist (claim_id is added to the below to avoid cardinality issue)
(
    SELECT 1 FROM
      ps20_rx_rxc_04_norm_reverse_status rev
        WHERE
              COALESCE(txn.hvid,'NONE')                                               = COALESCE(rev.hvid ,'')
         AND  COALESCE(txn.prov_prescribing_id, CONCAT('NONE', txn.hvid))             = COALESCE(rev.prov_prescribing_id, CONCAT('NONE', rev.hvid))
         AND  COALESCE(txn.date_service       , CONCAT('NONE', txn.hvid))             = COALESCE(rev.date_service       , CONCAT('NONE', rev.hvid))
         AND  COALESCE(txn.ndc_code           , CONCAT('NONE', txn.hvid))             = COALESCE(rev.ndc_code           , CONCAT('NONE', rev.hvid))

        ----------------- If both fields are not available skip the check----------------
         AND    CASE WHEN LENGTH(txn.dispensed_quantity)  > 0   AND LENGTH(rev.dispensed_quantity)  > 0
                    THEN  COALESCE(ABS(txn.dispensed_quantity), CONCAT('NONE', txn.hvid))   ELSE 0  END =
                CASE WHEN LENGTH(txn.dispensed_quantity)  > 0   AND LENGTH(rev.dispensed_quantity)  > 0
                    THEN  COALESCE(ABS(rev.dispensed_quantity), CONCAT('NONE', rev.hvid))   ELSE 0  END
---- Will be added after approval
--          AND    CASE WHEN LENGTH(txn.submitted_gross_due)  > 0   AND LENGTH(rev.submitted_gross_due)  > 0
--                     THEN  COALESCE(ABS(txn.submitted_gross_due), CONCAT('NONE', txn.hvid))   ELSE 0  END =
--                 CASE WHEN LENGTH(txn.submitted_gross_due)  > 0   AND LENGTH(rev.submitted_gross_due)  > 0
--                     THEN  COALESCE(ABS(rev.submitted_gross_due), CONCAT('NONE', rev.hvid))   ELSE 0  END

         AND UPPER(COALESCE(rev.logical_delete_reason,'')) = 'REVERSAL'
         AND txn.row_num         =  rev.row_num

        -----------------
        AND txn.logical_delete_reason IS NULL

)
-- ------------ Not Already Reversed
AND NOT EXISTS
(
    SELECT 1 FROM
      ps20_rx_rxc_04_norm_reverse_status rev
        WHERE
              COALESCE(txn.hvid,'NONE')                                               = COALESCE(rev.hvid ,'')
         AND  COALESCE(txn.prov_prescribing_id, CONCAT('NONE', txn.hvid))             = COALESCE(rev.prov_prescribing_id, CONCAT('NONE', rev.hvid))
         AND  COALESCE(txn.date_service       , CONCAT('NONE', txn.hvid))             = COALESCE(rev.date_service       , CONCAT('NONE', rev.hvid))
         AND  COALESCE(txn.ndc_code           , CONCAT('NONE', txn.hvid))             = COALESCE(rev.ndc_code           , CONCAT('NONE', rev.hvid))

        ----------------- If both fields are not available skip the check----------------
         AND    CASE WHEN LENGTH(txn.dispensed_quantity)  > 0   AND LENGTH(rev.dispensed_quantity)  > 0
                    THEN  COALESCE(ABS(txn.dispensed_quantity), CONCAT('NONE', txn.hvid))   ELSE 0  END =
                CASE WHEN LENGTH(txn.dispensed_quantity)  > 0   AND LENGTH(rev.dispensed_quantity)  > 0
                    THEN  COALESCE(ABS(rev.dispensed_quantity), CONCAT('NONE', rev.hvid))   ELSE 0  END
---- Will be added after approval
--           AND    CASE WHEN LENGTH(txn.submitted_gross_due)  > 0   AND LENGTH(rev.submitted_gross_due)  > 0
--                      THEN  COALESCE(ABS(txn.submitted_gross_due), CONCAT('NONE', txn.hvid))   ELSE 0  END =
--                  CASE WHEN LENGTH(txn.submitted_gross_due)  > 0   AND LENGTH(rev.submitted_gross_due)  > 0
--                      THEN  COALESCE(ABS(rev.submitted_gross_due), CONCAT('NONE', rev.hvid))   ELSE 0  END

        AND UPPER(COALESCE(logical_delete_reason,'')) = 'REVERSED CLAIM'
        AND txn.row_num = 9

)