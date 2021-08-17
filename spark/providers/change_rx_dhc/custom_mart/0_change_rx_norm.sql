SELECT
    txn.rx_transaction_id as claim_id
    , CAST(Null as STRING) as concat_unique_fields
    , txn.token_1 as token1
    , txn.token_2 as token2
    , CAST(Null as STRING) as hvid
    , CAST(Null as DATE) as date_service
    , CAST(Null as STRING) as rx_number
    , CAST(Null as STRING) as cob_count
    , CAST(Null as STRING) as pharmacy_other_id
    , CAST(Null as STRING) as response_code_vendor
    , CAST(Null as STRING) as ndc_code
    , CURRENT_DATE() as created
    , CAST('Change' as STRING) as data_vendor
FROM txn
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13
