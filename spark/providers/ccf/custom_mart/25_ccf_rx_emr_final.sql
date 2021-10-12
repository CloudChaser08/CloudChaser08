SELECT
    MONOTONICALLY_INCREASING_ID()    AS record_id,
    *
FROM  ccf_rx_emr_dedupe
--limit 10
