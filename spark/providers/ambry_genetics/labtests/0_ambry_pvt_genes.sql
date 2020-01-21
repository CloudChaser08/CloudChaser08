SELECT
    deidentified_id,
    patient_last_name,
    patient_first_name,
    date_of_birth,
    gender,
    patient_zip,
    patient_state,
    order_date,
    EXPLODE(SPLIT(txn.genes_tested, ','))           AS genes_tested,
    contact_name,
    npi,
    organization_name,
    organization_address,
    contact_email,
    office_phone,
    organization_state,
    organization_zip,
    icd_10_codes,
    hvjoinkey
FROM txn
    WHERE UPPER(COALESCE(txn.deidentified_id, '')) <> 'DEIDENTIFIED ID' 
    /* Eliminate non-U.S. transactions, for privacy. */ 
    AND UPPER(COALESCE(txn.organization_state, '')) <> 'INTERNATIONAL'
