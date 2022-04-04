SELECT
    claimId                                               AS rowid,
    slightly_obfuscate_hvid(hvid, CONCAT('hvid', {salt})) AS hvid
FROM matching_payload
