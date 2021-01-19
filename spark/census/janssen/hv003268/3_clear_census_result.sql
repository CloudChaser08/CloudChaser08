SELECT
    sht.study_id AS STUDY_ID,
    ect.subject_id as EC_SUBJECT_ID,
    sht.subject_id as SH_SUBJECT_ID,
    sht.subject_number as SH_SUBJECT_NUMBER,
    '' as EC_SITE_ID,
    '' as SH_SITE_ID,
    '' as EC_SITE_NUMBER,
    '' as SH_SITE_NUMBER,
    ect.consent_indicator as EC_CONSENT_INDICATOR,
    ect.pii_consent_indicator as EC_PII_CONSENT_INDICATOR,
    shm.hvid as HVID
FROM econsent ect
    INNER JOIN econsent_matching_payload_pruned ecm ON ect.hv_join_key = ecm.hvJoinKey
    INNER JOIN studyhub_matching_payload_pruned shm ON ecm.hvid = shm.eConsentHVID
    INNER JOIN studyhub sht ON shm.hvJoinKey = sht.hv_join_key
