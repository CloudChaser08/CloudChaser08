-- as specified in https://healthverity.atlassian.net/wiki/spaces/PRIV/pages/770670638/BMS+Zeposia+-+Technical+Specification#1b.-Post-Census-LASH-ingest-files
SELECT
    c.consentee_name AS consentee_name,
    c.consentee_type AS consentee_type,
    obfuscate_hvid(p.hvid, {salt}) as consenter_id,
    c.caregiver_id as caregiver_id,
    c.consenter_region as region,
    -- LASH sends us the invalid Patient Consent value, but consent expects patient instead
    regexp_replace(c.consenter_type, 'Patient Consent', 'patient') as consenter_type,
    c.franchise as franchise,
    c.brand as brand,
    c.program as program_name,
    c.service as service_name,
    c.channel as channel,
    c.consent_type as consent_type,
    -- Our input dates are in the format MM/DD/YYYY, but consent requires YYYY-MM-DD HH:mm:SS
    -- timestamp is a time of day in format HH:mm:SS which is pulled from batch ID
    regexp_replace(c.transaction_date, '(?<month>[0-9]{{2}})/(?<day>[0-9]{{2}})/(?<year>[0-9]{{4}})', '$3-$1-$2 {timestamp}') as transaction_date,
    regexp_replace(c.expiry_date, '(?<month>[0-9]{{2}})/(?<day>[0-9]{{2}})/(?<year>[0-9]{{4}})', '$3-$1-$2 {timestamp}') as expiry_date,
    c.status as status,
    c.document_id as document_id,
    '' as document_location,
    '' as document_signature,
    c.source_system as system,
    c.hub_id as hub_id
FROM matching_payload p
    INNER JOIN consenter_preferences c ON p.hvjoinkey = c.hvjoinkey
