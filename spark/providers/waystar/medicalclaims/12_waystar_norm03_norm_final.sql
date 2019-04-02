SELECT
monotonically_increasing_id()                                                           AS record_id,
current_date()                                                                          AS created,
*
FROM (
    SELECT * FROM waystar_norm03_norm_final_1
    UNION ALL
    SELECT * FROM waystar_norm03_norm_final_2
    UNION ALL
    SELECT * FROM waystar_norm03_norm_final_3
    UNION ALL
    SELECT * FROM waystar_norm03_norm_final_4
)