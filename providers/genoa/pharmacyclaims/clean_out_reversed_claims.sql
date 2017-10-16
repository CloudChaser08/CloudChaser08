INSERT INTO normalized_claims SELECT * FROM pharmacyclaims_common_model;

-- Clean out reversals based on claims that appear in the current batch of data
UPDATE normalized_claims
SET logical_delete_reason = 'Reversed Claim'
FROM pharmacyclaims_common_model AS p
WHERE UPPER(normalized_claims.transaction_code_std) = 'B1'
    AND UPPER(normalized_claims.response_code_std) = 'P'
    AND normalized_claims.pharmacy_npi = p.pharmacy_npi
    AND normalized_claims.rx_number = p.rx_number
    AND normalized_claims.fill_number = p.fill_number
    AND COALESCE(normalized_claims.product_service_id, '') = COALESCE(p.product_service_id, '')
    AND COALESCE(normalized_claims.procedure_code, '') = COALESCE(p.procedure_code, '')
    AND COALESCE(normalized_claims.ndc_code, '') = COALESCE(p.ndc_code, '')
    AND normalized_claims.bin_number = p.bin_number
    AND COALESCE(normalized_claims.processor_control_number, '') = COALESCE(p.processor_control_number, '')
    AND normalized_claims.date_service = p.date_service
    AND UPPER(p.transaction_code_std) = 'B2'
    AND normalized_claims.logical_delete_reason IS NULL
    AND (normalized_claims.date_authorized < p.date_authorized 
        OR (normalized_claims.date_authorized = p.date_authorized AND normalized_claims.time_authorized <= p.time_authorized));
