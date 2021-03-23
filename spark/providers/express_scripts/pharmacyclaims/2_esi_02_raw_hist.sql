SELECT
    --date_of_service,
    pharmacy_claim_id, pharmacy_claim_ref_id
FROM
    _temp_rev_transaction
WHERE
    pharmacy_claim_ref_id <> 'whA4dkmHELckamkOJvaI/De+Ru9qgWK6Cc72fHZPvL0='
GROUP BY
    1, 2