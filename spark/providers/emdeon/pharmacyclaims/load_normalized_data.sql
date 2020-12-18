-- Load normalized data into table for cleaning out reversals
ALTER TABLE normalized_claims ADD PARTITION (best_date={best_date}) LOCATION {date_path}
