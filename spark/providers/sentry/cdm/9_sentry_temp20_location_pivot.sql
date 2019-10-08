SELECT
    claim_id,
    date_time,
    COALESCE
        (
            LEAD(date_time) OVER (PARTITION BY claim_id ORDER BY claim_id, date_time),
            '9999-12-31'
        )                                                                                                       AS next_loc_dt,
    CONCAT_WS(', ', COLLECT_LIST(location_code))
        AS loc_grp
FROM
(
    -- Retrieve the unique set of claim/date/location combinations.
    SELECT DISTINCT
        claim_id,
        date_time,
        location_code
     FROM loc
    WHERE 0 <> LENGTH(TRIM(COALESCE(location_code, '')))
)
GROUP BY 1, 2
