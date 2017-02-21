INSERT INTO event_common_model
SELECT
    monotonically_increasing_id(),  -- record_id
    mp.hvid,                        -- hvid
    current_timestamp(),            -- created
    '1',                            -- model_version
    {set},                          -- data_set
    {feed},                         -- data_feed
    {vendor},                       -- data_vendor
    NULL,                           -- source_version
    cap_age(mp.age),                -- patient_age
    cap_year_of_birth(
        mp.age,
        date_start.formatted,
        mp.yearOfBirth
        ),                          -- patient_year_of_birth
    mp.threeDigitZip,               -- patient_zip
    mp.state,                       -- patient_state
    NULL,                           -- patient_gender
    t.id,                           -- source_record_id
    'OBITUARY',                     -- source_record_qual
    record_entry_date.formatted,    -- source_record_date
    'PATIENT_DECEASED',             -- event
    concat(
        substring(mp.deathMonth, 3, 6),
        '-',
        substring(mp.deathMonth, 0, 2)
        ),                          -- event_val
    NULL                            -- event_date
FROM transactional_raw t
    INNER JOIN matching_payload mp ON t.hv_join_key = mp.hvJoinKey
    INNER JOIN dates record_entry_date ON date_start.date = REGEXP_REPLACE(t.date_of_record_entry,'/','')
