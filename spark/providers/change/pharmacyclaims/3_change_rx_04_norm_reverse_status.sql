SELECT
    hist.record_id
FROM
    change_rx_03_comb_hist_cf AS hist
    INNER JOIN change_rx_03_comb_hist_cf AS cf
    ON UPPER(hist.transaction_code_std) = 'B1'
        AND UPPER(hist.response_code_std) = 'P'
        AND hist.pharmacy_npi = cf.pharmacy_npi
        AND hist.rx_number = cf.rx_number
        AND hist.fill_number = cf.fill_number
        AND COALESCE(hist.product_service_id, '') = COALESCE(cf.product_service_id, '')
        AND COALESCE(hist.procedure_code, '') = COALESCE(cf.procedure_code, '')
        AND COALESCE(hist.ndc_code, '') = COALESCE(cf.ndc_code, '')
        AND hist.bin_number = cf.bin_number
        AND COALESCE(hist.processor_control_number, '') = COALESCE(cf.processor_control_number, '')
        AND hist.date_service = cf.date_service
        AND UPPER(cf.transaction_code_std) = 'B2'
        AND hist.logical_delete_reason IS NULL
        AND (hist.date_authorized < cf.date_authorized
            OR (hist.date_authorized = cf.date_authorized AND hist.time_authorized <= cf.time_authorized))
GROUP BY
    hist.record_id
