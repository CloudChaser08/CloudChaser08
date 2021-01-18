SELECT
    DISTINCT
        m.record_id
FROM
    change_rx_03_comb_hist_cf AS m
    INNER JOIN change_rx_03_comb_hist_cf AS s
    ON UPPER(s.transaction_code_std) = 'B2'
        AND m.pharmacy_npi = s.pharmacy_npi
        AND m.rx_number = s.rx_number
        AND m.fill_number = s.fill_number
        AND COALESCE(m.product_service_id, '') = COALESCE(s.product_service_id, '')
        AND COALESCE(m.procedure_code, '') = COALESCE(s.procedure_code, '')
        AND COALESCE(m.ndc_code, '') = COALESCE(s.ndc_code, '')
        AND m.bin_number = s.bin_number
        AND COALESCE(m.processor_control_number, '') = COALESCE(s.processor_control_number, '')
        AND m.date_service = s.date_service
        AND (m.date_authorized < s.date_authorized
            OR (m.date_authorized = s.date_authorized AND m.time_authorized <= s.time_authorized))
WHERE
    m.logical_delete_reason IS NULL
    AND UPPER(m.transaction_code_std) = 'B1'
    AND UPPER(m.response_code_std) = 'P'
