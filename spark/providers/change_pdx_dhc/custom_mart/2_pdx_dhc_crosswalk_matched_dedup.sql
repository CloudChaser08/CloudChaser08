SELECT
    concat_unique_fields,date_filled,
    date_delivered_fulfilled,
    rx_number,
    cob_count,
    pharmacy_ncpdp_number,
    claim_indicator,
    ndc_code,
    datavant_token1,
    datavant_token2,
    max(input_file_name) AS input_file_name
FROM pdx_dhc_crosswalk_matched
GROUP BY
    concat_unique_fields,
    date_filled,
    date_delivered_fulfilled,
    rx_number,
    cob_count,
    pharmacy_ncpdp_number,
    claim_indicator,
    ndc_code,
    datavant_token1,
    datavant_token2
--limit 1
