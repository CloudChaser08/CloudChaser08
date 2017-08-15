DROP VIEW IF EXISTS default.events;
CREATE VIEW default.events (
    record_id,
    hvid,
    created,
    model_version,
    data_set,
    data_feed,
    data_vendor,
    source_version,
    patient_age,
    patient_year_of_birth,
    patient_zip3,
    patient_state,
    patient_gender,
    source_record_id,
    source_record_qual,
    source_record_date, 
    event,
    event_val,
    event_val_uom,
    event_date,
    part_provider,
    part_best_date
) AS SELECT 
    record_id,
    hvid,
    created,
    model_version,
    data_set,
    data_feed,
    data_vendor,
    source_version,
    patient_age,
    patient_year_of_birth,
    patient_zip3,
    patient_state,
    patient_gender,
    source_record_id,
    source_record_qual,
    source_record_date, 
    event,
    event_val,
    event_val_uom,
    event_date,
    part_provider,
    CASE WHEN part_best_date IN ('NULL', '0_PREDATES_FULL_HISTORICAL')
    THEN '0_PREDATES_FULL_HISTORICAL'
    ELSE part_best_date
    END as part_best_date
FROM events_v4
UNION ALL
SELECT 
    record_id,
    hvid,
    created,
    model_version,
    data_set,
    data_feed,
    data_vendor,
    source_version,
    patient_age,
    patient_year_of_birth,
    patient_zip,
    patient_state,
    patient_gender,
    source_record_id,
    source_record_qual,
    source_record_date, 
    event,
    event_val,
    NULL as event_val_uom,
    event_date,
    part_provider,
    CASE WHEN part_best_date IN ('NULL', '0_PREDATES_FULL_HISTORICAL')
    THEN '0_PREDATES_FULL_HISTORICAL'
    ELSE part_best_date
    END as part_best_date
FROM events_old;
