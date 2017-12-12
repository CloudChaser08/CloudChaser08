INSERT INTO medicalclaims_common_model
SELECT
    NULL,                                       -- record_id
    t.ediclaim_id,                              -- claim_id
    'TBD',                                      -- hvid
    NULL,                                       -- created
    '2',                                        -- model_version
    NULL,                                       -- data_set
    NULL,                                       -- data_feed
    NULL,                                       -- data_vendor
    NULL,                                       -- source_version
    NULL,                                       -- patient_gender
    NULL,                                       -- patient_age
    NULL,                                       -- patient_year_of_birth
    NULL,                                       -- patient_zip3
    NULL,                                       -- patient_state
    'P',                                        -- claim_type
    NULL,                                       -- date_received
    extract_date(
        t.dateservicestart,
        {min_date},
        {max_date}
    ),                                          -- date_service
    NULL,                                       -- date_service_end
    NULL,                                       -- inst_date_admitted
    NULL,                                       -- inst_date_discharged
    NULL,                                       -- inst_admit_type_std_id
    NULL,                                       -- inst_admit_type_vendor_id
    NULL,                                       -- inst_admit_type_vendor_desc
    NULL,                                       -- inst_admit_source_std_id
    NULL,                                       -- inst_admit_source_vendor_id
    NULL,                                       -- inst_admit_source_vendor_desc
    NULL,                                       -- inst_discharge_status_std_id
    NULL,                                       -- inst_discharge_status_vendor_id
    NULL,                                       -- inst_discharge_status_vendor_desc
    NULL,                                       -- inst_type_of_bill_std_id
    NULL,                                       -- inst_type_of_bill_vendor_id
    NULL,                                       -- inst_type_of_bill_vendor_desc
    NULL,                                       -- inst_drg_std_id
    NULL,                                       -- inst_drg_vendor_id
    NULL,                                       -- inst_drg_vendor_desc
    t.facilitycode,                             -- place_of_service_std_id **TODO: mask as usual? **
    NULL,                                       -- place_of_service_vendor_id
    NULL,                                       -- place_of_service_vendor_desc
    t.linesequencenumber,                       -- service_line_number
    ARRAY(
        t.linkeddiagnosisone, t.linkeddiagnosistwo,
        t.linkeddiagnosisthree, t.linkeddiagnosisfour,
        NULL)[sl_explode.n],                    -- diagnosis_code
    NULL,                                       -- diagnosis_code_qual
    CASE
        WHEN ARRAY(t.linkeddiagnosisone, t.linkeddiagnosistwo,
            t.linkeddiagnosisthree, t.linkeddiagnosisfour,
            NULL)[sl_explode.n] IS NOT NULL
            THEN sl_explode.n
        ELSE NULL 
    END,                                        -- diagnosis_priority
    NULL,                                       -- admit_diagnosis_ind
    t.procedurecode,                            -- procedure_code
    t.procedurecodequalifier,                   -- procedure_code_qual
    NULL,                                       -- principal_proc_ind
    t.submittedunits,                           -- procedure_units
    t.proceduremodifierone,                     -- procedure_modifier_1
    t.proceduremodifiertwo,                     -- procedure_modifier_2
    t.proceduremodifierthree,                   -- procedure_modifier_3
    t.proceduremodifierfour,                    -- procedure_modifier_4
    NULL,                                       -- revenue_code
    NULL,                                       -- ndc_code
    NULL,                                       -- medical_coverage_type
    t.submittedcharge,                          -- line_charge
    NULL,                                       -- line_allowed
    t.submittedchargetotal,                     -- total_charge
    NULL,                                       -- total_allowed
    CASE
        WHEN t.renderingprovideridqualifier = 'XX' AND
             11 = LENGTH(TRIM(COALESCE(t.renderingprovidernpid, '')))
             THEN COALESCE(t.renderingproviderid, t.renderingprovidernpid)
        WHEN t.renderingprovideridqualifier = 'XX' 
             THEN t.renderingproviderid
        ELSE NULL
    END,                                        -- prov_rendering_npi
    CASE
        WHEN t.billprovideridqualifier = 'XX' AND
             11 = LENGTH(TRIM(COALESCE(t.billprovidernpid, '')))
             THEN COALESCE(t.billproviderid, t.billprovidernpid)
        WHEN t.billprovideridqualifier = 'XX'
             THEN t.billproviderid
        ELSE NULL
    END,                                        -- prov_billing_npi
    CASE
        WHEN t.referringprovideridqualifier = 'XX'
             THEN t.referringproviderid
        ELSE NULL
    END,                                        -- prov_referring_npi
    CASE
        WHEN t.servicefacilityidqualifier = 'XX'
             THEN t.servicefacilityid
        ELSE NULL
    END,                                        -- prov_facility_npi
    t.payerid,                                  -- payer_vendor_id
    t.payername,                                -- payer_name
    NULL,                                       -- payer_parent_name
    NULL,                                       -- payer_org_name
    NULL,                                       -- payer_plan_id
    NULL,                                       -- payer_plan_name
    NULL,                                       -- payer_type
    CASE
        WHEN t.renderingprovideridqualifier <> 'XX'
             AND 0 <> LENGTH(TRIM(COALESCE(t.renderingproviderid, '')))
             THEN t.renderingproviderid
        ELSE NULL
    END,                                        -- prov_rendering_vendor_id
    NULL,                                       -- prov_rendering_tax_id
    NULL,                                       -- prov_rendering_dea_id
    NULL,                                       -- prov_rendering_ssn
    NULL,                                       -- prov_rendering_state_license
    NULL,                                       -- prov_rendering_upin
    NULL,                                       -- prov_rendering_commercial_id
    t.renderingprovidername,                    -- prov_rendering_name_1
    NULL,                                       -- prov_rendering_name_2
    NULL,                                       -- prov_rendering_address_1
    NULL,                                       -- prov_rendering_address_2
    NULL,                                       -- prov_rendering_city
    NULL,                                       -- prov_rendering_state
    NULL,                                       -- prov_rendering_zip
    t.renderingprovidertaxonomycode,            -- prov_rendering_std_taxonomy
    NULL,                                       -- prov_rendering_vendor_specialty
    CASE
        WHEN t.billprovideridqualifier <> 'XX'
             AND 0 <> LENGTH(TRIM(COALESCE(t.billproviderid, '')))
             THEN t.billproviderid
        ELSE NULL
    END,                                        -- prov_billing_vendor_id
    NULL,                                       -- prov_billing_tax_id
    NULL,                                       -- prov_billing_dea_id
    NULL,                                       -- prov_billing_ssn
    NULL,                                       -- prov_billing_state_license
    NULL,                                       -- prov_billing_upin
    NULL,                                       -- prov_billing_commercial_id
    t.billprovidername,                         -- prov_billing_name_1
    NULL,                                       -- prov_billing_name_2
    NULL,                                       -- prov_billing_address_1
    NULL,                                       -- prov_billing_address_2
    NULL,                                       -- prov_billing_city
    NULL,                                       -- prov_billing_state
    NULL,                                       -- prov_billing_zip
    t.billprovidertaxonomycode,                 -- prov_billing_std_taxonomy
    NULL,                                       -- prov_billing_vendor_specialty
    CASE
        WHEN t.referringprovideridqualifier <> 'XX'
             AND 0 <> LENGTH(TRIM(COALESCE(t.referringproviderid, '')))
             THEN referringproviderid
        ELSE NULL
    END,                                        -- prov_referring_vendor_id
    NULL,                                       -- prov_referring_tax_id
    NULL,                                       -- prov_referring_dea_id
    NULL,                                       -- prov_referring_ssn
    NULL,                                       -- prov_referring_state_license
    NULL,                                       -- prov_referring_upin
    NULL,                                       -- prov_referring_commercial_id
    t.referringprovidername,                    -- prov_referring_name_1
    NULL,                                       -- prov_referring_name_2
    NULL,                                       -- prov_referring_address_1
    NULL,                                       -- prov_referring_address_2
    NULL,                                       -- prov_referring_city
    NULL,                                       -- prov_referring_state
    NULL,                                       -- prov_referring_zip
    NULL,                                       -- prov_referring_std_taxonomy
    NULL,                                       -- prov_referring_vendor_specialty
    CASE
        WHEN t.servicefacilityidqualifier <> 'XX'
             AND 0 <> LENGTH(TRIM(COALESCE(t.servicefacilityid, '')))
             THEN t.servicefacilityid
        ELSE NULL
    END,                                        -- prov_facility_vendor_id
    NULL,                                       -- prov_facility_tax_id
    NULL,                                       -- prov_facility_dea_id
    NULL,                                       -- prov_facility_ssn
    NULL,                                       -- prov_facility_state_license
    NULL,                                       -- prov_facility_upin
    NULL,                                       -- prov_facility_commercial_id
    t.servicefacilityname,                      -- prov_facility_name_1
    NULL,                                       -- prov_facility_name_2
    t.servicefacilityaddress,                   -- prov_facility_address_1
    NULL,                                       -- prov_facility_address_2
    t.servicefacilitycity,                      -- prov_facility_city
    t.servicefacilitystate,                     -- prov_facility_state
    t.servicefacilityzip,                       -- prov_facility_zip
    NULL,                                       -- prov_facility_std_taxonomy
    NULL,                                       -- prov_facility_vendor_specialty
    NULL,                                       -- cob_payer_vendor_id_1
    NULL,                                       -- cob_payer_seq_code_1
    NULL,                                       -- cob_payer_hpid_1
    NULL,                                       -- cob_payer_claim_filing_ind_code_1
    NULL,                                       -- cob_ins_type_code_1
    NULL,                                       -- cob_payer_vendor_id_2
    NULL,                                       -- cob_payer_seq_code_2
    NULL,                                       -- cob_payer_hpid_2
    NULL,                                       -- cob_payer_claim_filing_ind_code_2
    NULL                                        -- cob_ins_type_code_2
FROM transactional_cardinal_pms t
    CROSS JOIN service_line_exploder sl_explode
WHERE
    -- Filter out cases from explosion where diagnosis_code is NULL
    (  
        ARRAY(t.linkeddiagnosisone, t.linkeddiagnosistwo,
            t.linkeddiagnosisthree, t.linkeddiagnosisfour)[sl_explode.n]
        IS NOT NULL 
    ) 
    OR 
    -- If all are NULL, include one row w/ NULL diagnosis_code (SELECT DISTINCT)
    ( 
        COALESCE(t.linkeddiagnosisone, t.linkeddiagnosistwo,
        t.linkeddiagnosisthree, t.linkeddiagnosisfour) IS NULL
        AND
        sl_explode.n = 0
    )
;
