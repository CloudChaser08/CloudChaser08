SELECT
        CAST(Null as STRING) as claim_id
        , CAST(Null as STRING) as concat_unique_fields
        , txn.upk_key_1 as token1
        , txn.upk_key_2 as token2
        , txn.hashed_hvid as hvid
        , CAST(Null as DATE) as date_service
        , CAST(Null as STRING) as rx_number
        , CAST(Null as STRING) as cob_count
        , CAST(Null as STRING) as pharmacy_other_id
        , CAST(Null as STRING) as response_code_vendor
        , CAST(Null as STRING) as ndc_code
        , CURRENT_DATE() as created
        , CAST('Private Source 20' as STRING) as data_vendor

FROM inovalon_norm_join as txn
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13