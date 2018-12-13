SELECT
    obfuscate_hvid(hvid, CONCAT('hvid', {salt}))                    AS hvid,
    claimId                                                         AS rowid
FROM matching_payload
