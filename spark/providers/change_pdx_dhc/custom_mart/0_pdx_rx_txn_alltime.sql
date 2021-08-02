SELECT
    date_delivered_fulfilled,
    date_filled,
    prescription_number_filler,
    pharmacy_ncpdp_number,
    coordination_of_benefits_counter,
    claim_indicator,
    dispensed_ndc_number
FROM
    pdx_rx_txn_alltime_dw
GROUP by
    1,2,3,4,5,6,7