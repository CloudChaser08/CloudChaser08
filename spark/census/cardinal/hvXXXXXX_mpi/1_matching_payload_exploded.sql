-- explode() ignores NULL values
-- Explicitly union rows where topCandidates is NULL with the exploded rows
-- where topCandidates was not NULL
SELECT
    row_id,
    hvid,
    claimid,
    matchstatus,
    explode(topcandidates) as candidate
FROM matching_payload_w_row_id
WHERE topcandidates IS NOT NULL

UNION ALL

SELECT
    row_id,
    hvid,
    claimid,
    matchstatus,
    NULL as candidate
FROM matching_payload_w_row_id
WHERE topcandidates IS NULL
