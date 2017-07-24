INSERT INTO event_common_model
SELECT
    NULL,                              -- record_id
    p.hvid,                            -- hvid
    current_date(),                    -- created
    '2',                               -- model_version
    NULL,                              -- data_set
    NULL,                              -- data_feed
    NULL,                              -- data_vendor 
    NULL,                              -- source_version
    cap_age(p.age),                    -- patient_age 
    cap_year_of_birth(
        p.age,
        current_date(),
        p.yearOfBirth),                -- patient_year_of_birth
    p.threeDigitZip,                   -- patient_zip3
    p.state,                           -- patient_state
    p.gender,                          -- patient_gender
    p.claimid,                         -- source_record_id 
    'CLAIM',                           -- source_record_qual
    NULL,                              -- source_record_date
    t.studioVertical,                  -- event
    t.visitClassName,                  -- event_val
    t.visitTypeGroupName,              -- event_val_uom
    t.visitClassDate,                  -- event_date
    'Private Source 133',              -- part_provider
    current_date()                     -- part_processdate
    
FROM transactional_mindbody t
LEFT JOIN matching_payload p
ON t.hv_linking_id = p.hvJoinKey
