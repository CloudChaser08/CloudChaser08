select
    MONOTONICALLY_INCREASING_ID() AS record_id,
    uf.*
FROM (
    SELECT h.* FROM change_rx_01_hist h
    UNION ALL
    SELECT c.* FROM change_rx_02_norm_cf c
) uf

