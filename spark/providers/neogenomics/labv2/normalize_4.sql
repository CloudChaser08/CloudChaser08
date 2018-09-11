SELECT DISTINCT 
    bse.* 
 FROM t3 bse 
WHERE NOT EXISTS 
    (
        SELECT 1 
         FROM t3 ltr 
        WHERE bse.test_orderid_hashed = ltr.test_orderid_hashed 
          AND bse.result_name = ltr.result_name
          AND bse.vendor_file_date > ltr.vendor_file_date
    )
