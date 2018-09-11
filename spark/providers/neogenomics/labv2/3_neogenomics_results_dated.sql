SELECT  
    header,
    source,
    test_orderid_hashed,
    result_code,
    result_name,
    result_comments,
    result_reference_range,
    result_units_of_measure,
    result_value,
    sql_column_name,
    -- Replace CURRENT_DATE with the vendor's file date.
    CURRENT_DATE AS vendor_file_date
 FROM results
