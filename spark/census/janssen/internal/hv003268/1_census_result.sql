SELECT
    mp.hvid AS hvid,
    r.subject_number as subject_number,
    {study_id} as study
FROM records r
    INNER JOIN matching_payload mp ON r.hvJoinKey = mp.hvJoinKey
WHERE
    r.sub_study_consent_indicator = 'Y'
    AND r.sub_study_withdraw_indicator <> 'Y'
    AND r.consent_indicator = 'Y'
    AND r.consent_withdraw_indicator <> 'Y'
