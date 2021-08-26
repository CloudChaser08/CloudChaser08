SELECT
    txn.claim_number                                                                AS claim_id,
    txn.tokenencryptionkey                                                          AS concat_unique_fields,
    /* vender tokens */
    txn.datavant_token1                                                             AS token1,
    txn.datavant_token2                                                             AS token2,
    CAST(Null as STRING)                                                            AS hvid,
    /* date_service */
    CAP_DATE
        (
            CAST
                (
                    EXTRACT_DATE
                        (
                            CAST(
                                CASE
                                    WHEN txn.date_filled >= txn.date_delivered
                                     AND txn.date_filled <= CAST(REPLACE('{VDR_FILE_DT}','-','') as INTEGER)
                                         THEN txn.date_filled
                                    WHEN txn.date_delivered >= txn.date_filled
                                     AND txn.date_delivered <= CAST(REPLACE('{VDR_FILE_DT}','-','') as INTEGER)
                                         THEN txn.date_delivered
                                    WHEN txn.date_filled >= txn.date_delivered
                                     AND txn.date_filled > CAST(REPLACE('{VDR_FILE_DT}','-','') as INTEGER)
                                     AND txn.date_delivered <= CAST(REPLACE('{VDR_FILE_DT}','-','') as INTEGER)
                                         THEN txn.date_delivered
                                    WHEN txn.date_delivered >= txn.date_filled
                                     AND txn.date_delivered > CAST(REPLACE('{VDR_FILE_DT}','-','') as INTEGER)
                                     AND txn.date_filled <= CAST(REPLACE('{VDR_FILE_DT}','-','') as INTEGER)
                                         THEN txn.date_filled
                                    WHEN txn.date_delivered > CAST(REPLACE('{VDR_FILE_DT}','-','') as INTEGER)
                                     AND txn.date_filled > CAST(REPLACE('{VDR_FILE_DT}','-','') as INTEGER)
                                         THEN txn.date_delivered
                                    ELSE NULL
                                    END
                                    AS STRING),
                                '%Y%m%d'
                            ) AS DATE
                    ),
            CAST('{EARLIEST_SERVICE_DATE}' AS DATE),
            CAST('{VDR_FILE_DT}' AS DATE)
        )																	    AS date_service,

    /* rx_number */
    MD5(txn.rx_number)														    AS rx_number,
    txn.cob_count                                                               AS cob_count,
    txn.pharmacy_ncpdp_number                                                   AS pharmacy_other_id,

    UPPER(txn.claim_indicator)                                                  AS response_code_vendor,
    CLEAN_UP_NDC_CODE(txn.ndc_code)	                                            AS ndc_code,
    CURRENT_DATE()                                                              AS created,
    CAST('PDX' as STRING)                                                       AS data_vendor
FROM
    pdx_dhc_crosswalk_dedup txn
