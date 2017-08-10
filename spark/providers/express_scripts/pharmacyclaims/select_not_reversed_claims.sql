-- whA4dkmHELckamkOJvaI/De+Ru9qgWK6Cc72fHZPvL0= is a blank reference id
SELECT *
FROM {table_name}
WHERE claim_id NOT IN (
    SELECT pharmacy_claim_ref_id from transactions WHERE pharmacy_claim_ref_id != 'whA4dkmHELckamkOJvaI/De+Ru9qgWK6Cc72fHZPvL0='
)
