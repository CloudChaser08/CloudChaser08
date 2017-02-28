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
        CAST(CONCAT(
                SUBSTRING(mp.deathMonth, 3, 6),
                '-',
                SUBSTRING(mp.deathMonth, 0, 2),
                '-01'
                ) AS date),
        mp.yearOfBirth
        ),                          -- patient_year_of_birth
    mp.threeDigitZip,               -- patient_zip
    mp.state,                       -- patient_state
    NULL,                           -- patient_gender
    t.id,                           -- source_record_id
    'OBITUARY',                     -- source_record_qual
    NULL,                           -- source_record_date
    'DEATH',                        -- event
    NULL,                           -- event_val
    CAST(CONCAT(
            SUBSTRING(mp.deathMonth, 3, 6),
            '-',
            SUBSTRING(mp.deathMonth, 0, 2),
            '-01'
            ) AS date)              -- event_date
FROM transactional_raw t
    LEFT JOIN matching_payload mp ON t.hv_join_key = mp.hvJoinKey
