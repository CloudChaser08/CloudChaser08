SELECT
    MONOTONICALLY_INCREASING_ID()    AS record_id,
    *
FROM  ccf_patient_problem_dedupe
--limit 10
