SELECT
    -- Salt used in previous Alnylam project
    obfuscate_hvid(hvid, 'hvid265')                                 AS hvid,
    claimId                                                         AS rowid
FROM matching_payload
