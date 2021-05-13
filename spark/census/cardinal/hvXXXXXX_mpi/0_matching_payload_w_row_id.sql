SELECT
    monotonically_increasing_id() as row_id,
    hvid,
    claimid,
    matchstatus,
    topcandidates
FROM matching_payload
