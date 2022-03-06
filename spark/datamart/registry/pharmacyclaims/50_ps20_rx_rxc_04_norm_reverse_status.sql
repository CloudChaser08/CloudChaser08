SELECT
*,
ROW_NUMBER() OVER (
            PARTITION BY
                src.hvid                    ,
                src.prov_prescribing_id     ,
                src.date_service            ,
                src.ndc_code                ,
                src.days_supply             ,
                ABS(src.dispensed_quantity)
                --ABS(src.submitted_gross_due)

            ORDER BY
                src.hvid                    ,
                src.prov_prescribing_id     ,
                src.date_service            ,
                src.ndc_code                ,
                src.days_supply             ,
                ABS(src.dispensed_quantity)
                --ABS(src.submitted_gross_due)

        ) AS row_num
FROM  ps20_rx_rxc_03_norm_comb_hist_cf src
WHERE UPPER(COALESCE(logical_delete_reason,'')) = 'REVERSAL'
