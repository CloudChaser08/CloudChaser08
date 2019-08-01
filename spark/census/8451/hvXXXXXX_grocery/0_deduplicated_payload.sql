SELECT *
FROM
(
    SELECT ROW_NUMBER() OVER (PARTITION BY join_keys) as row_num, *
    FROM matching_payload
    HAVING row_num = 1
) mp
