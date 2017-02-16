INSERT INTO emr_common_model
SELECT
    monotonically_increasing_id(),  -- record_id
    mp.hvid,                        -- hvid
    current_timestamp(),            -- created
    '1',                            -- model_version
    {set},                          -- data_set
    {feed},                         -- data_feed
    {vendor},                       -- data_vendor
    NULL,                           -- source_version
    NULL,                           -- claim_type
    t.id,                           -- claim_id
    NULL,                           -- claim_qual
    NULL,                           -- claim_date
    NULL,                           -- claim_error_ind
    cap_age(mp.age),                -- patient_age
    cap_year_of_birth(
        mp.age,
        t.date_of_record_entry,
        mp.yearOfBirth
        ),                          -- patient_year_of_birth
    concat(
        substring(mp.deathMonth, 0, 2),
        '-',
        substring(mp.deathMonth, 2, 6)
        ),                          -- patient_date_of_death
    mp.threeDigitZip,               -- patient_zip
    mp.state,                       -- patient_state
    'Y',                            -- patient_deceased_flag
    mp.gender,                      -- patient_gender
    NULL,                           -- patient_race
    NULL,                           -- patient_ethnicity
    NULL,                           -- provider_client_id_qual
    NULL,                           -- provider_client_id
    NULL,                           -- provider_rendering_id_qual
    NULL,                           -- provider_rendering_id
    NULL,                           -- provider_referring_id_qual
    NULL,                           -- provider_referring_id
    NULL,                           -- provider_billing_id_qual
    NULL,                           -- provider_biling_id
    NULL,                           -- provider_facility_id_qual
    NULL,                           -- provider_facility_id
    NULL,                           -- provider_ordering_id_qual
    NULL,                           -- provider_ordering_id
    NULL,                           -- provider_lab_id_qual
    NULL,                           -- provider_lab_id
    NULL,                           -- provider_pharmacy_id_qual
    NULL,                           -- provider_pharmacy_id
    NULL,                           -- provider_prescriber_id_qual
    NULL,                           -- provider_prescriber_id
    NULL,                           -- payer_id_qual
    NULL,                           -- payer_id
    NULL,                           -- payer_type
    NULL,                           -- payer_parent
    NULL,                           -- payer_name
    NULL,                           -- plan_name
    NULL,                           -- encounter_id
    NULL,                           -- encounter_id_qual
    NULL,                           -- description
    NULL,                           -- description_qual
    t.date_of_record_entry,         -- date_start
    NULL,                           -- date_end
    NULL,                           -- date_qual
    NULL,                           -- diagnosis_code
    NULL,                           -- diagnosis_code_qual
    NULL,                           -- diagnosis_code_priority
    NULL,                           -- procedure_code
    NULL,                           -- procedure_code_qual
    NULL,                           -- procedure_code_modifier
    NULL,                           -- procedure_code_priority
    NULL,                           -- ndc_code
    NULL,                           -- ndc_code_qual
    NULL,                           -- loinc_code
    NULL,                           -- other_code
    NULL,                           -- other_code_qual
    NULL,                           -- other_code_modifier
    NULL,                           -- other_code_mod_qual
    NULL,                           -- other_code_priority
    NULL,                           -- type
    NULL,                           -- type_qual
    NULL,                           -- category
    NULL,                           -- category_qual
    NULL,                           -- panel
    NULL,                           -- panel_qual
    NULL,                           -- specimen
    NULL,                           -- specimen_qual
    NULL,                           -- method
    NULL,                           -- method_qual
    NULL,                           -- result
    NULL,                           -- result_qual
    NULL,                           -- reason
    NULL,                           -- reason_qual
    NULL,                           -- ref_range
    NULL,                           -- ref_range_qual
    NULL,                           -- abnormal
    NULL,                           -- abnormal_qual
    NULL,                           -- uom
    NULL,                           -- uom_qual
    NULL,                           -- severity
    NULL,                           -- severity_qual
    NULL,                           -- status
    NULL,                           -- status_qual
    NULL,                           -- units
    NULL,                           -- units_qual
    NULL,                           -- qty_dispensed
    NULL,                           -- qty_dispensed_qual
    NULL,                           -- days_supply
    NULL,                           -- elapsed_days
    NULL,                           -- num_refills
    NULL,                           -- route
    NULL,                           -- sig
    NULL,                           -- frequency_units
    NULL,                           -- frequency_times
    NULL,                           -- frequency_uom
    NULL,                           -- dose
    NULL,                           -- dose_qual
    NULL,                           -- strength
    NULL,                           -- form
    NULL,                           -- sample
    NULL,                           -- unverified
    NULL                            -- electronicrx
FROM transactional_raw t
    INNER JOIN matching_payload mp ON t.hv_join_key = mp.hvJoinKey
