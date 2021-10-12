SELECT
    MONOTONICALLY_INCREASING_ID()    AS record_id,
    *
FROM  ccf_encounter_crf_dedupe
--limit 10
