SELECT 
    hvid,
    claimid,
    personid,
    patientid,
    threedigitzip,
    yearofbirth,
    gender,
    age,
    state,
    vendor_file_date,
    LAG(vendor_file_date, 1, CAST('1900-01-01' AS DATE)) 
        OVER (PARTITION BY personid ORDER BY vendor_file_date)
        AS prev_vendor_file_date
FROM neogenomics_payload_dated
