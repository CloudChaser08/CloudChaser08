select
    MONOTONICALLY_INCREASING_ID() AS record_id,
    uf.*
FROM (
    SELECT h.* FROM esi_01_hist h
    UNION ALL
    SELECT c.* FROM esi_03_norm_cf c
) uf
