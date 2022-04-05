SELECT
    claimId                                               AS rowid,
    slightly_obfuscate_hvid(cast(hvid as integer), CONCAT('hvid', {salt})) AS hvid
FROM matching_payload
