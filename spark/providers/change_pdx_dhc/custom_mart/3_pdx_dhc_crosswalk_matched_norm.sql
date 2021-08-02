SELECT
    txn.concat_unique_fields                                                AS concat_unique_fields,
    /* date_service */
    CAP_DATE
        (
            CAST
                (
                    EXTRACT_DATE
                        (
                            CAST(
                                CASE
                                    WHEN txn.date_filled >= txn.date_delivered_fulfilled
                                     AND txn.date_filled <= CAST(REPLACE('{VDR_FILE_DT}','-','') as INTEGER)
                                         THEN txn.date_filled
                                    WHEN txn.date_delivered_fulfilled >= txn.date_filled
                                     AND txn.date_delivered_fulfilled <= CAST(REPLACE('{VDR_FILE_DT}','-','') as INTEGER)
                                         THEN txn.date_delivered_fulfilled
                                    WHEN txn.date_filled >= txn.date_delivered_fulfilled
                                     AND txn.date_filled > CAST(REPLACE('{VDR_FILE_DT}','-','') as INTEGER)
                                     AND txn.date_delivered_fulfilled <= CAST(REPLACE('{VDR_FILE_DT}','-','') as INTEGER)
                                         THEN txn.date_delivered_fulfilled
                                    WHEN txn.date_delivered_fulfilled >= txn.date_filled
                                     AND txn.date_delivered_fulfilled > CAST(REPLACE('{VDR_FILE_DT}','-','') as INTEGER)
                                     AND txn.date_filled <= CAST(REPLACE('{VDR_FILE_DT}','-','') as INTEGER)
                                         THEN txn.date_filled
                                    WHEN txn.date_delivered_fulfilled > CAST(REPLACE('{VDR_FILE_DT}','-','') as INTEGER)
                                     AND txn.date_filled > CAST(REPLACE('{VDR_FILE_DT}','-','') as INTEGER)
                                         THEN txn.date_delivered_fulfilled
                                    ELSE NULL
                                    END
                                    AS STRING),
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
    txn.datavant_token2
from
    pdx_dhc_crosswalk_matched_dedup txn
--limit 1
