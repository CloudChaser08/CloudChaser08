SELECT
    CASE
        WHEN usedFlexibleMatching = TRUE AND matchStatus = 'multi_match'
            NULL                                                        AS hvid
        ELSE
            obfuscate_hvid(hvid, CONCAT('hvid', {salt}))                AS hvid
    END
claimId                                                                 AS rowid
FROM matching_payload
