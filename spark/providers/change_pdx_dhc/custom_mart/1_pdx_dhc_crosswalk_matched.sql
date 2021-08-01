SELECT
    crs.concat_unique_fields,
    COALESCE(CAST(crs.date_filled as INTEGER),0) as date_filled,
    COALESCE(CAST(alt.date_delivered_fulfilled as INTEGER),0) as date_delivered_fulfilled,
    TRIM(crs.rx_number) as rx_number,
    TRIM(crs.cob_count) as cob_count,
    TRIM(crs.pharmacy_ncpdp_number) as pharmacy_ncpdp_number,
    TRIM(crs.claim_indicator) as claim_indicator,
    TRIM(crs.ndc_code) as ndc_code,
    TRIM(crs.datavant_token1) as datavant_token1,
    TRIM(crs.datavant_token2) as datavant_token2,
    crs.input_file_name
FROM
    crs
JOIN pdx_rx_txn_alltime alt
    ON COALESCE(TRIM(crs.date_filled),'19000101') = COALESCE(TRIM(alt.date_filled),'19000101')
    AND COALESCE(TRIM(crs.rx_number),'XXX') = COALESCE(TRIM(alt.prescription_number_filler),'XXX')
    AND COALESCE(CAST(TRIM(crs.cob_count) AS INTEGER), 9999) = COALESCE(CAST(TRIM(alt.coordination_of_benefits_counter) AS INTEGER), 9999)
    AND COALESCE(TRIM(crs.pharmacy_ncpdp_number),'XXX') = COALESCE(TRIM(alt.pharmacy_ncpdp_number), 'XXX')
    AND COALESCE(TRIM(crs.claim_indicator), 'XXX') = COALESCE(TRIM(alt.claim_indicator),'XXX')
    AND COALESCE(TRIM(crs.ndc_code),'XXX') = COALESCE(TRIM(alt.dispensed_ndc_number),'XXX')
--limit 1
