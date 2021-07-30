SELECT 
txn.concat_unique_fields                                                AS concat_unique_fields,
/* date_service */
CAP_DATE
    (
        CAST
            (
                EXTRACT_DATE
                    (
                        CASE
                            WHEN txn.date_filled >= txn.date_delivered_fulfilled 
                             AND txn.date_filled <= '{VDR_FILE_DT}' 
                                 THEN txn.date_filled 
                            WHEN txn.date_delivered_fulfilled >= txn.date_filled 
                             AND txn.date_delivered_fulfilled <= '{VDR_FILE_DT}' 
                                 THEN txn.date_delivered_fulfilled 
                            WHEN txn.date_filled >= txn.date_delivered_fulfilled 
                             AND txn.date_filled > '{VDR_FILE_DT}' 
                             AND txn.date_delivered_fulfilled <= '{VDR_FILE_DT}' 
                                 THEN txn.date_delivered_fulfilled 
                            WHEN txn.date_delivered_fulfilled >= txn.date_filled 
                             AND txn.date_delivered_fulfilled > '{VDR_FILE_DT}' 
                             AND txn.date_filled <= '{VDR_FILE_DT}' 
                                 THEN txn.date_filled 
                            WHEN txn.date_delivered_fulfilled > '{VDR_FILE_DT}' 
                             AND txn.date_filled > '{VDR_FILE_DT}' 
                                 THEN txn.date_delivered_fulfilled 
                            ELSE NULL
                            END,
                            '%Y%m%d'
                        ) AS DATE
                ),
        CAST('{EARLIEST_SERVICE_DATE}' AS DATE),
        CAST('{VDR_FILE_DT}' AS DATE)
	)																					AS date_service,

/* rx_number */
MD5(txn.rx_number)														AS rx_number,
txn.cob_count                                                           AS cob_count,
txn.pharmacy_ncpdp_number                                               AS pharmacy_other_id,

UPPER(txn.claim_indicator)                                              AS response_code_vendor,
CLEAN_UP_NDC_CODE(txn.ndc_code)	                                        AS ndc_code,
/* vender tokens */
txn.datavant_token1,
txn.datavant_token2,
'pdx'                                                                   AS part_provider,
/* part_best_date */
CASE WHEN 0 = LENGTH
                (
                    TRIM
                        (
                            COALESCE
                                (
                                    CAP_DATE
                                        (
                                            CAST
                                                (
                                                    EXTRACT_DATE
                                                        (
                                                            CASE
                                                                WHEN date_filled >= date_delivered_fulfilled
                                                                    AND date_filled <= '{VDR_FILE_DT}'
                                                                        THEN date_filled
                                                                WHEN date_delivered_fulfilled >= date_filled
                                                                    AND date_delivered_fulfilled <= '{VDR_FILE_DT}'
                                                                        THEN date_delivered_fulfilled
                                                                WHEN date_filled >= date_delivered_fulfilled
                                                                    AND date_filled > '{VDR_FILE_DT}'
                                                                    AND date_delivered_fulfilled <= '{VDR_FILE_DT}'
                                                                        THEN date_delivered_fulfilled
                                                                WHEN date_delivered_fulfilled >= date_filled
                                                                    AND date_delivered_fulfilled > '{VDR_FILE_DT}'
                                                                    AND date_filled <= '{VDR_FILE_DT}'
                                                                        THEN date_filled
                                                                WHEN date_delivered_fulfilled > '{VDR_FILE_DT}'
                                                                    AND date_filled > '{VDR_FILE_DT}'
                                                                        THEN date_delivered_fulfilled
                                                                ELSE NULL
                                                            END,
                                                            '%Y%m%d'
                                                        ) AS DATE
                                                ),
                                            CAST('{VDR_FILE_DT}' AS DATE)
                                        )
                                            , ''
                                )
                        )
                )
            THEN '0_PREDATES_HVM_HISTORY'
        ELSE CONCAT
            (
                SUBSTR
                    (
                        CASE
                            WHEN date_filled >= date_delivered_fulfilled
                                AND date_filled <= '{VDR_FILE_DT}'
                                    THEN date_filled
                            WHEN date_delivered_fulfilled >= date_filled
                                AND date_delivered_fulfilled <= '{VDR_FILE_DT}'
                                    THEN date_delivered_fulfilled
                            WHEN date_filled >= date_delivered_fulfilled
                                AND date_filled > '{VDR_FILE_DT}'
                                AND date_delivered_fulfilled <= '{VDR_FILE_DT}'
                                    THEN date_delivered_fulfilled
                            WHEN date_delivered_fulfilled >= date_filled
                                AND date_delivered_fulfilled > '{VDR_FILE_DT}'
                                AND date_filled <= '{VDR_FILE_DT}'
                                    THEN date_filled
                            WHEN date_delivered_fulfilled > '{VDR_FILE_DT}'
                                AND date_filled > '{VDR_FILE_DT}'
                                    THEN date_delivered_fulfilled
                            ELSE NULL
                        END, 1, 4
                    ), '-',
                SUBSTR
                    (
                        CASE
                            WHEN date_filled >= date_delivered_fulfilled
                                AND date_filled <= '{VDR_FILE_DT}'
                                    THEN date_filled
                            WHEN date_delivered_fulfilled >= date_filled
                                AND date_delivered_fulfilled <= '{VDR_FILE_DT}'
                                    THEN date_delivered_fulfilled
                            WHEN date_filled >= date_delivered_fulfilled
                                AND date_filled > '{VDR_FILE_DT}'
                                AND date_delivered_fulfilled <= '{VDR_FILE_DT}'
                                    THEN date_delivered_fulfilled
                            WHEN date_delivered_fulfilled >= date_filled
                                AND date_delivered_fulfilled > '{VDR_FILE_DT}'
                                AND date_filled <= '{VDR_FILE_DT}'
                                    THEN date_filled
                            WHEN date_delivered_fulfilled > '{VDR_FILE_DT}'
                                AND date_filled > '{VDR_FILE_DT}'
                                    THEN date_delivered_fulfilled
                            ELSE NULL
                        END, 5, 2
                    /* Add the day portion of the date, to match the historical part_best_date. */
                    ), '-01'
            )
    END                                                                              AS part_best_date
from pdx_crosswalk_dedup txn