INSERT INTO event_common_model
SELECT
    NULL,                              -- record_id
    p.hvid,                            -- hvid
    NULL,                              -- created
    '3',                               -- model_version
    NULL,                              -- data_set
    NULL,                              -- data_feed
    NULL,                              -- data_vendor 
    NULL,                              -- source_version
    p.age,                             -- patient_age 
    p.yearOfBirth,                     -- patient_year_of_birth
    p.threeDigitZip,                   -- patient_zip3
    UPPER(p.state),                    -- patient_state
    p.gender,                          -- patient_gender
    t.claim_id,                        -- source_record_id 
    NULL,                              -- source_record_qual
    NULL,                              -- source_record_date
    t.studioVertical,                  -- event
    t.visitClassName,                  -- event_val
    t.visitTypeGroupName,              -- event_val_uom
    extract_date(
        t.visitClassDate,
        '%m/%d/%Y',
        CAST({min_date} AS DATE),
        CAST({max_date} AS DATE)
    )                                  -- event_date
    
FROM transactional_mindbody t
LEFT JOIN matching_payload p
ON t.hv_linking_id = p.hvJoinKey
