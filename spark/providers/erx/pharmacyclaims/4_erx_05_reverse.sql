SELECT
    txn.*,
    ROW_NUMBER() OVER (PARTITION BY 
            txn.rx_number,
            txn.pharmacy_npi,
            txn.ndc_code,
            txn.bin_number,
            txn.fill_number             
    ORDER BY 
            txn.rx_number,
            txn.pharmacy_npi,
            txn.ndc_code,
            txn.bin_number,
            txn.fill_number,
            txn.date_authorized,
            txn.time_authorized
    ) AS row_num
FROM erx_04_comb txn
WHERE UPPER(txn.logical_delete_reason) = 'REVERSAL'
