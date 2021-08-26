SELECT
    claim_number,
    date_filled,
    rx_number,
    cob_count,
    pharmacy_ncpdp_number,
    claim_indicator,
    ndc_code,
    date_delivered,
    datavant_token1,
    datavant_token2,
    tokenencryptionkey,
    max(input_file_name) AS input_file_name
FROM
    crs
GROUP BY
    claim_number,
    date_filled,
    rx_number,
    cob_count,
    pharmacy_ncpdp_number,
    claim_indicator,
    ndc_code,
    date_delivered,
    datavant_token1,
    datavant_token2,
    tokenencryptionkey
