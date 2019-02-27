SELECT clms.*,
    split(input_file_name, '/')[size(split(input_file_name, '/')) - 1] as data_set
FROM waystar_claims clms
LEFT JOIN
    (
        SELECT claim_number,
            max(regexp_extract(input_file_name, 'claims_po_([^_])*[^/]*', 1)) max_extract_date
        FROM waystar_claims
        GROUP BY claim_number
    ) mx
    ON mx.max_extract_date = regexp_extract(clms.input_file_name, 'claims_po_([^_])*[^/]*', 1)
WHERE mx.claim_number is not NULL
