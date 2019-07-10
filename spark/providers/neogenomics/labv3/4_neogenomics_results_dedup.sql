SELECT DISTINCT bse.*
 FROM neogenomics_results_dated  bse
WHERE NOT EXISTS
    (
        SELECT 1 
         FROM neogenomics_results_dated ltr
        WHERE bse.test_orderid_hashed = ltr.test_orderid_hashed 
          AND bse.result_name         = ltr.result_name
          AND bse.vendor_file_date    < ltr.vendor_file_date
    )
