-- whA4dkmHELckamkOJvaI/De+Ru9qgWK6Cc72fHZPvL0= is the hash of NULL for this express scripts data
DELETE FROM normalized_claims WHERE claim_id IN (SELECT pharmacy_claim_id FROM express_scripts_rx_raw WHERE pharmacy_claim_ref_id <> 'whA4dkmHELckamkOJvaI/De+Ru9qgWK6Cc72fHZPvL0=');
