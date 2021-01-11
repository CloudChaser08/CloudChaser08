SELECT
    claimId                                                                 AS rowid,
    -- if matched using flexible matching AND matchStatus is multi_match, null out the HVID
    CASE flexibleMatchingUsed
        WHEN TRUE THEN
            CASE matchStatus
                WHEN 'multi_match' THEN
                    NULL
                ELSE
                    obfuscate_hvid(hvid, CONCAT('hvid', {salt}))
            END
        ELSE
            obfuscate_hvid(hvid, CONCAT('hvid', {salt}))
    END                                                                    AS hvid
FROM matching_payload
