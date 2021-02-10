SELECT
    obfuscate_hvid(p.hvid, {salt}) as hvid,
    c.*
FROM matching_payload p
    INNER JOIN consenter_preferences c ON p.hvjoinkey = c.hvjoinkey
