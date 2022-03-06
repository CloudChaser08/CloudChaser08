SELECT
    MONOTONICALLY_INCREASING_ID()  AS record_id, *
FROM
(SELECT
    'pregnancy'                AS registry_type,
    txn.hvid,
    txn.created,
    txn.model_version,
    txn.data_set,
    txn.data_feed,
    txn.data_vendor,
    txn.patient_gender,
    txn.race,
    txn.patient_year_of_birth,
    CAST(EXTRACT_DATE('{VDR_FILE_DT}', '%Y-%m-%d') AS DATE) AS stg_file_date,
    txn.part_provider,
    txn.part_best_date
FROM inv_norm_50_dx_cf_expl txn
GROUP BY
    registry_type,
    txn.hvid,
    txn.created,
    txn.model_version,
    txn.data_set,
    txn.data_feed,
    txn.data_vendor,
    txn.patient_gender,
    txn.race,
    txn.patient_year_of_birth,
    txn.part_provider,
    txn.part_best_date
)
