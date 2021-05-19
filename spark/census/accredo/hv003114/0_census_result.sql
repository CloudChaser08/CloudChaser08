SELECT
    obfuscate_hvid(p.hvid, {salt}) AS consenter_id,
    p.personId AS spid
FROM matching_payload p