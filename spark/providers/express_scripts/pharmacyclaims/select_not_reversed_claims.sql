-- whA4dkmHELckamkOJvaI/De+Ru9qgWK6Cc72fHZPvL0= is a blank reference id
SELECT {table_name}.*
FROM {table_name}
LEFT JOIN transactions ON claim_id = pharmacy_claim_ref_id
WHERE pharmacy_claim_ref_id IS NULL
