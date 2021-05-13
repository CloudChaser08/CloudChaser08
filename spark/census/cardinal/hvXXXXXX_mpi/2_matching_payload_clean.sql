SELECT
    row_id,
    hvid,
    claimid,
    matchstatus,
    to_json(collect_list(candidate)) as candidates
FROM (
    SELECT
        row_id,
        hvid,
        claimid,
        matchstatus,
        map("hvid",
            cast(slightly_obfuscate_hvid(cast(round(candidate[0]) as integer), 'Cardinal_MPI-0') as string),
            "confidence",
            round(candidate[1], 2)
        ) as candidate
    FROM matching_payload_exploded
) x
GROUP BY row_id, hvid, claimid, matchstatus
