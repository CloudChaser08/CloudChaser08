SELECT
    MONOTONICALLY_INCREASING_ID()    AS record_id,
    *
FROM  ccf_diagnosis_emr_dedupe
--limit 10
