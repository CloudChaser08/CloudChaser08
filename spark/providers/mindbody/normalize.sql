INSERT INTO event_common_model
SELECT
    monotonically_increasing_id(),    -- record_id
    p.hvid                            -- hvid
    GETDATE()                         -- created
    NULL                              -- model_version
    {set}                             -- data_set
    {feed}                            -- data_feed
    {vendor}                          -- data_vendor
    NULL                              -- source_version
    NULL                              -- patient_id
    NULL                              -- patient_member_id
    NULL                              -- patient_group_id
    NULL                              -- patient_group_name
    NULL                              -- patient_email
    NULL                              -- patient_phone
    NULL                              -- patient_ssn
    t.first_name                      -- patient_first_name
    t.last_name                       -- patient_last_name
    p.age                             -- patient_age
    t.birthdate                       -- patient_date_of_birth
    p.yearOfBirth                     -- patient_year_of_birth
    t.zip                             -- patient_zip
    p.threeDigitZip                   -- patient_zip3
    NULL                              -- patient_address
    t.city                            -- patient_city
    p.state                           -- patient_state
    p.gender                          -- patient_gender
    -- TODO: NOT SURE ABOUT THESE vvv
    p.claim_id                        -- source_record_id 
    'CLAIM'                           -- source_record_qual
    -- TODO: NOT SURE ABOUT THESE ^^^
    t.sessionStart                    -- source_record_date
    t.studioVertical                  -- event
    t.visitTypeName                   -- event_val
    t.visitTypeGroupName              -- event_val_uom
    t.sessionStart                    -- event_date
    {vendor}                          -- part_provider
    GETDATE()                         -- part_processdate
    
FROM mindbody_transactional t
LEFT JOIN mindbody_payload p
ON t.hv_join_key = p.hvJoinKey
