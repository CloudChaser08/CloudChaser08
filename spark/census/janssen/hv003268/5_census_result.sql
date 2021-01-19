SELECT
    STUDY_ID,
    EC_SUBJECT_ID,
    SH_SUBJECT_ID,
    SH_SUBJECT_NUMBER,
    EC_SITE_ID,
    SH_SITE_ID,
    EC_SITE_NUMBER,
    SH_SITE_NUMBER,
    EC_CONSENT_INDICATOR,
    EC_PII_CONSENT_INDICATOR,
    obfuscate_hvid(hvid, CONCAT('hvid', {salt})) as HVID
FROM clear_census_result
