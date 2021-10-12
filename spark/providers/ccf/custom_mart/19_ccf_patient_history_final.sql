SELECT
    MONOTONICALLY_INCREASING_ID()    AS record_id,
    *
FROM  ccf_patient_history_dedupe
--limit 10
