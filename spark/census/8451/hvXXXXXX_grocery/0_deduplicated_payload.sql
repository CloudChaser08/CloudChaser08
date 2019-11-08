SELECT *
FROM
(
    --TODO: USE THE FOLLOWING LINE WHEN YOU GO TO AUTOMATE THIS METHOD
    -- SELECT ROW_NUMBER() OVER (PARTITION BY join_keys ORDER BY hvid DESC) as row_num, *
    SELECT ROW_NUMBER() OVER (PARTITION BY claimId ORDER BY hvid DESC) as row_num, *
    FROM matching_payload
    HAVING row_num = 1
) mp
