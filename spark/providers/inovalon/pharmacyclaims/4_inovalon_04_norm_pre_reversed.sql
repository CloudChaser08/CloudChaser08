SELECT 
*,
ROW_NUMBER() OVER (
            PARTITION BY 
                src.hvid                    ,       
                src.prov_prescribing_id     ,  
                src.date_service            ,
                src.ndc_code                ,           
                src.days_supply             ,          
                ABS(src.dispensed_quantity) ,   
                ABS(src.submitted_gross_due),        
                ABS(src.copay_coinsurance)  ,         
                ABS(src.paid_gross_due) 
            ORDER BY 
                src.hvid                    ,       
                src.prov_prescribing_id     ,  
                src.date_service            ,
                src.ndc_code                ,           
                src.days_supply             ,          
                ABS(src.dispensed_quantity) ,   
                ABS(src.submitted_gross_due),        
                ABS(src.copay_coinsurance)  ,         
                ABS(src.paid_gross_due)     ,
                CASE 
                    WHEN UPPER(transaction_code_vendor) = 'INITIAL PAY CLAIM'            THEN '1'
                    WHEN UPPER(transaction_code_vendor) = 'ADJUSTMENT TO ORIGINAL CLAIM' THEN '2'
                    WHEN UPPER(transaction_code_vendor) = 'UNKNOWN'                      THEN '3'                    
                    WHEN UPPER(transaction_code_vendor) IS NULL                          THEN '4'
                    --- Note: here the logical_delete_reason used correctly there is no typo. Becuase I wanted to add REVERSED txn to check if the reverse txn is already REVERSED
                    WHEN UPPER(logical_delete_reason)  = 'REVERSED CLAIM'                THEN '9'                    
                ELSE 5
                END                 
        ) AS row_num
FROM  inovalon_03_norm_comb_hist_cf src
WHERE UPPER(COALESCE(logical_delete_reason,'')) <> 'REVERSAL'
