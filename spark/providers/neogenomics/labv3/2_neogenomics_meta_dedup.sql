SELECT DISTINCT bse.*
 FROM neogenomics_meta_pivot  bse
WHERE NOT EXISTS 
    (
        SELECT 1 
         FROM neogenomics_meta_pivot ltr
        WHERE bse.test_orderid_hashed = ltr.test_orderid_hashed 
          AND bse.vendor_file_date < ltr.vendor_file_date
    ) 
  AND 0 <> LENGTH(TRIM(COALESCE(bse.test_orderid_hashed, ''))) 
  AND 'NULL' <> TRIM(UPPER(bse.test_orderid_hashed))
