SELECT
    obfuscate_hvid(p.hvid, {salt}) AS hvid,
    p.personId AS spid
FROM matching_payload p