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
    EXTRACT_DATE(REGEXP_EXTRACT(input_file_name, '(..../../..)/results', 1), '%Y/%m/%d') AS vendor_file_date 
FROM results
