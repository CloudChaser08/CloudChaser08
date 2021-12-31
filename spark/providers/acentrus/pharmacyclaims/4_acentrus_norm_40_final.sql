SELECT  MONOTONICALLY_INCREASING_ID()  AS record_id ,*
FROM
(
    SELECT * FROM acentrus_norm_30_pre_final
)
