SELECT
    obfuscate_hvid(mp.hvid, CONCAT('hvid', {salt})) as hvid
FROM records r
    INNER JOIN matching_payload mp ON r.hvJoinKey = mp.hvJoinKey
WHERE
    r.sub_study_consent_indicator = 'Y'
    AND r.sub_study_withdraw_indicator <> 'Y'
    AND r.consent_indicator = 'Y'
    AND r.consent_withdraw_indicator <> 'Y'
