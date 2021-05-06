SELECT
    obfuscate_hvid(p.hvid, {salt}) AS consenter_id,
    p.personId AS sp_id
FROM matching_payload p