DROP TABLE IF EXISTS reversed;
CREATE TABLE reversed AS 
SELECT concat_ws(':', normalized_claims.record_id, normalized_claims.data_set) as record_id FROM
(SELECT * FROM normalized_claims UNION ALL SELECT *, '-' as part_best_date FROM pharmacyclaims_common_model) as normalized_claims
INNER JOIN pharmacyclaims_common_model AS p
ON UPPER(normalized_claims.transaction_code_std) = 'B1'
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
;
