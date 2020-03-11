select MONOTONICALLY_INCREASING_ID() AS record_id, * from (
    SELECT * FROM erx_02_hist
    UNION ALL
    SELECT  * FROM erx_03_norm
)
