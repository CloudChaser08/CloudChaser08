SELECT
    claim_id,
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
    hvjoinkey,
    input_file_name
FROM ambry_si txn
