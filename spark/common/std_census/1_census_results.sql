SELECT
    obfuscate_hvid(hvid, CONCAT('hvid', {opp_id}))                  AS hvid,
    claimId                                                         AS rowid
FROM matching_payload
