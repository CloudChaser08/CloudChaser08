SELECT
txn.*,
      ROW_NUMBER() OVER (PARTITION BY 
                txn.rx_number,
                txn.date_service,
                txn.bin_number,
                txn.processor_control_number,
                txn.pharmacy_other_id,
                txn.fill_number,
                txn.dispensed_quantity,
                txn.days_supply,
                transaction_code_std,
                txn.date_authorized,
                txn.time_authorized                
        ORDER BY 
                txn.rx_number,
                txn.date_service,
                txn.bin_number,
                txn.processor_control_number,
                txn.pharmacy_other_id,
                txn.fill_number,
                txn.dispensed_quantity,
                txn.days_supply,
                transaction_code_std,
                txn.date_authorized,
                txn.time_authorized
        ) AS row_num
FROM inmar_03_comb_hist_cf txn
WHERE UPPER(txn.logical_delete_reason) = 'REVERSAL'
--LIMIT 10
