SELECT
    -- Salt used in previous Kantar project
    obfuscate_hvid(hvid, 'hvidKAN')                                 AS hvid,
    claimId                                                         AS rowid
FROM matching_payload
