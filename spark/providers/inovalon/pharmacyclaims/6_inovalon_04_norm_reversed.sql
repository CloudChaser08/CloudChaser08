SELECT
--    record_id, --- (remove)
    txn.claim_id,
    txn.hvid,
    txn.created,
    txn.model_version,
    txn.data_set,
    txn.data_feed,
    txn.data_vendor,
    txn.patient_gender,
    txn.patient_age,
    txn.patient_year_of_birth,
    txn.patient_zip3,
    txn.patient_state,
    txn.date_service,
    txn.transaction_code_vendor,
    txn.ndc_code,
    txn.dispensed_quantity,
    txn.days_supply,
    txn.pharmacy_npi,
    txn.prov_prescribing_npi,
    txn.prov_prescribing_name_1,
    txn.prov_prescribing_address_1,
    txn.prov_prescribing_address_2,
    txn.prov_prescribing_city,
    txn.prov_prescribing_state,
    txn.prov_prescribing_zip,
    txn.prov_prescribing_std_taxonomy,
    txn.prov_prescribing_vendor_specialty,
--  txn.copay_coinsurance,      
    txn.submitted_gross_due,
    txn.paid_gross_due,
	txn.prov_prescribing_id,
	txn.prov_prescribing_qual,  
    'Reversed Claim' AS logical_delete_reason,
    txn.part_provider,
    txn.part_best_date
FROM inovalon_04_norm_pre_reversed txn
WHERE EXISTS
------------- Reversed Exist (claim_id is added to the below to avoid cardinality issue)
(
    SELECT 1 FROM
      inovalon_04_norm_reverse_status rev
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
                    
         AND    CASE WHEN LENGTH(txn.submitted_gross_due)  > 0   AND LENGTH(rev.submitted_gross_due)  > 0   
                    THEN  COALESCE(ABS(txn.submitted_gross_due), CONCAT('NONE', txn.hvid))   ELSE 0  END = 
                CASE WHEN LENGTH(txn.submitted_gross_due)  > 0   AND LENGTH(rev.submitted_gross_due)  > 0   
                    THEN  COALESCE(ABS(rev.submitted_gross_due), CONCAT('NONE', rev.hvid))   ELSE 0  END 
                    
          AND    CASE WHEN       LENGTH(txn.paid_gross_due)  > 0   AND LENGTH(rev.paid_gross_due)  > 0
                     THEN  COALESCE(ABS(txn.paid_gross_due), CONCAT('NONE', txn.hvid))   ELSE 0  END = 
                 CASE WHEN       LENGTH(txn.paid_gross_due)  > 0   AND LENGTH(rev.paid_gross_due)  > 0   
                     THEN  COALESCE(ABS(rev.paid_gross_due), CONCAT('NONE', rev.hvid))   ELSE 0  END 
         
         AND UPPER(COALESCE(rev.logical_delete_reason,'')) = 'REVERSAL'
         AND txn.row_num         =  rev.row_num 
        
        -----------------
        AND txn.logical_delete_reason IS NULL

)
-- ------------ Not Already Reversed 
AND NOT EXISTS
(
    SELECT 1 FROM
      inovalon_04_norm_reverse_status rev
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
                    
          AND    CASE WHEN LENGTH(txn.submitted_gross_due)  > 0   AND LENGTH(rev.submitted_gross_due)  > 0   
                     THEN  COALESCE(ABS(txn.submitted_gross_due), CONCAT('NONE', txn.hvid))   ELSE 0  END = 
                 CASE WHEN LENGTH(txn.submitted_gross_due)  > 0   AND LENGTH(rev.submitted_gross_due)  > 0   
                     THEN  COALESCE(ABS(rev.submitted_gross_due), CONCAT('NONE', rev.hvid))   ELSE 0  END 

          AND    CASE WHEN LENGTH(txn.paid_gross_due)  > 0   AND LENGTH(rev.paid_gross_due)  > 0
                     THEN  COALESCE(ABS(txn.paid_gross_due), CONCAT('NONE', txn.hvid))   ELSE 0  END = 
                 CASE WHEN LENGTH(txn.paid_gross_due)  > 0   AND LENGTH(rev.paid_gross_due)  > 0   
                     THEN  COALESCE(ABS(rev.paid_gross_due), CONCAT('NONE', rev.hvid))   ELSE 0  END 
        ------------ We cannot use row number here as the row number for reverse can be 1 from history
        ------------ and we are not considering REVERSED txn from history, it will falsly reversed it again.
        AND UPPER(COALESCE(logical_delete_reason,'')) = 'REVERSED CLAIM'
        AND txn.row_num = 9

)

