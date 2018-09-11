SELECT DISTINCT
    hvid,
    claimid,
    personid,
    patientid,
    threedigitzip,
    yearofbirth,
    gender,
    age,
    state,
    EXTRACT_DATE(REGEXP_EXTRACT(input_file_name, '(..../../..)/[^/]*$', 1), '%Y/%m/%d') AS vendor_file_date
 FROM matching_payload
