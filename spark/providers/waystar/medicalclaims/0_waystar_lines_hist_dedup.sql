SELECT lns.*
FROM lines lns
LEFT JOIN
    (
        SELECT claim_number,
            max(regexp_extract(input_file_name, 'claims_pt_([^_])*[^/]*', 1)) max_extract_date
        FROM lines
        GROUP BY claim_number
    ) mx
    ON mx.max_extract_date = regexp_extract(lns.input_file_name, 'claims_pt_([^_])*[^/]*', 1)
        AND mx.claim_number = lns.claim_number
WHERE mx.claim_number is not NULL
