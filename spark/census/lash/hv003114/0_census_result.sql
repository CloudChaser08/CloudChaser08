-- as specified in https://healthverity.atlassian.net/wiki/spaces/PRIV/pages/770670638/BMS+Zeposia+-+Technical+Specification#1b.-Post-Census-LASH-ingest-files
SELECT
    c.consentee_name AS consentee_name,
    c.consentee_type AS consentee_type,
    obfuscate_hvid(p.hvid, {salt}) as consenter_id,
    c.caregiver_id as caregiver_id,
    c.consenter_region as region,
    c.consenter_type as consenter_type,
    c.franchise as franchise,''
    c.brand as brand,
    c.program as program_name,
    c.service as service_name,
    c.channel as channel,
    c.consent_type as consent_type,
    c.transaction_date as transaction_date,
    c.expiry_date as expiry_date,
    c.status as status,
    c.document_id as document_id,
    c.source_system as system,
    c.hub_id as hub_id
FROM matching_payload p
    INNER JOIN consenter_preferences c ON p.hvjoinkey = c.hvjoinkey
