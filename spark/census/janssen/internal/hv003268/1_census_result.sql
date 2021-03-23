SELECT
    mp.hvid AS hvid
FROM records r
    INNER JOIN matching_payload mp ON r.hvJoinKey = mp.hvJoinKey
WHERE
    r.sub_study_consent_indicator = 'Y'
    AND r.sub_study_withdraw_indicator <> 'Y'
    AND r.consent_indicator = 'Y'
    AND r.consent_withdraw_indicator <> 'Y'
