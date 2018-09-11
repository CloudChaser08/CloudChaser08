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
    -- Replace CURRENT_DATE with the vendor's file date.
    CURRENT_DATE AS vendor_file_date
 FROM matching_payload
