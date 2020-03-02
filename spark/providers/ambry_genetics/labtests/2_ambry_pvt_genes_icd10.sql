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
    genes_tested,
    contact_name,
    npi,
    organization_name,
    organization_address,
    contact_email,
    office_phone,
    organization_state,
    organization_zip,
    EXPLODE(SPLIT(icd_10_codes, ',')) icd_10_codes, 
    hvjoinkey
FROM ambry_pvt_genes txn


