SELECT
    CONCAT(deidentified_id, '_',EXTRACT_DATE(order_date, '%m/%d/%Y'), '_',row_num ) AS claim_id,
    *
FROM
(
    SELECT
        deidentified_id,
        patient_last_name,
        patient_first_name,
        date_of_birth,
        gender,
        patient_zip,
        patient_state,
        order_date,
        txn.genes_tested,
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
        ROW_NUMBER() OVER (
                    PARTITION BY 
                        deidentified_id,
                        order_date
                    ORDER BY 
                        deidentified_id,
                        order_date
                ) AS row_num,
        input_file_name
                
    FROM txn
        WHERE UPPER(COALESCE(txn.deidentified_id, '')) <> 'DEIDENTIFIED ID' 
        /* Eliminate non-U.S. transactions, for privacy. */ 
        AND UPPER(COALESCE(txn.organization_state, '')) <> 'INTERNATIONAL'
)


