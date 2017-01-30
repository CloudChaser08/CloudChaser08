-- populate rows for each service line, for both institutional and professional
INSERT INTO medicalclaims_common_model (
        claim_id,
        hvid,
        source_version,
        patient_gender,
        patient_age,
        patient_year_of_birth,
        patient_zip3,
        patient_state,
        claim_type,
        date_received,
        date_service,
        date_service_end,
        inst_admit_type_std_id,
        inst_admit_source_std_id,
        inst_discharge_status_std_id,
        inst_type_of_bill_std_id,
        inst_drg_std_id,
        place_of_service_std_id,
        service_line_number,
        diagnosis_code,
        diagnosis_code_qual,
        diagnosis_priority,
        admit_diagnosis_ind,
        procedure_code,
        procedure_code_qual,
        principal_proc_ind,
        procedure_units,
        procedure_modifier_1,
        procedure_modifier_2,
        procedure_modifier_3,
        procedure_modifier_4,
        revenue_code,
        ndc_code,
        medical_coverage_type,
        line_charge,
        total_charge,
        prov_rendering_npi,
        prov_billing_npi,
        prov_referring_npi,
        prov_facility_npi,
        payer_vendor_id,
        payer_name,
        payer_type,
        prov_rendering_name_1,
        prov_rendering_name_2,
        prov_rendering_address_1,
        prov_rendering_address_2,
        prov_rendering_city,
        prov_rendering_state,
        prov_rendering_zip,
        prov_rendering_std_taxonomy,
        prov_billing_tax_id,
        prov_billing_ssn,
        prov_billing_state_license,
        prov_billing_upin,
        prov_billing_name_1,
        prov_billing_name_2,
        prov_billing_address_1,
        prov_billing_address_2,
        prov_billing_city,
        prov_billing_state,
        prov_billing_zip,
        prov_billing_std_taxonomy,
        prov_referring_name_1,
        prov_referring_name_2,
        prov_referring_address_1,
        prov_referring_address_2,
        prov_referring_city,
        prov_referring_state,
        prov_referring_zip,
        prov_referring_std_taxonomy,
        prov_facility_name_1,
        prov_facility_name_2,
        prov_facility_address_1,
        prov_facility_address_2,
        prov_facility_city,
        prov_facility_state,
        prov_facility_zip,
        prov_facility_std_taxonomy,
        cob_payer_seq_code_1,
        cob_payer_hpid_1,
        cob_payer_claim_filing_ind_code_1,
        cob_ins_type_code_1,
        cob_payer_seq_code_2,
        cob_payer_hpid_2,
        cob_payer_claim_filing_ind_code_2,
        cob_ins_type_code_2
        ) 
SELECT DISTINCT
    header.ClaimId,                                -- claim_id
    COALESCE(mp.parentid, mp.hvid),                -- hvid
    1,                                             -- source_version
    mp.gender,                                     -- patient_gender
    mp.age,                                        -- patient_age
    mp.yearOfBirth,                                -- patient_year_of_birth
    mp.threeDigitZip,                              -- patient_zip3
    mp.state,                                      -- patient_state
    CASE
    WHEN header.Type = 'Institutional'
    THEN 'I'
    WHEN header.Type = 'Professional'
    THEN 'P'
    END,                                           -- claim_type
    process_date.formatted,                        -- date_received
    CASE 
    WHEN svc_start_date.formatted IS NOT NULL 
    THEN svc_start_date.formatted
    WHEN start_date.formatted IS NOT NULL
    THEN start_date.formatted
    ELSE (
    SELECT MIN(clean.formatted) 
    FROM transactional_serviceline sl2
        LEFT JOIN dates clean ON sl2.ServiceStart = clean.formatted
    WHERE sl2.ClaimId = serviceline.ClaimId
        )
    END,                                           -- date_service
    CASE 
    WHEN svc_start_date.formatted IS NOT NULL 
    THEN serviceline.ServiceEnd
    WHEN start_date.formatted IS NOT NULL
    THEN header.EndDate
    ELSE (
    SELECT MIN(clean.formatted) 
    FROM transactional_serviceline sl2
        LEFT JOIN dates clean ON sl2.ServiceEnd = clean.formatted
    WHERE sl2.ClaimId = serviceline.ClaimId
        )
    END,                                           -- date_service_end
    CASE 
    WHEN header.Type = 'Institutional'
    THEN header.AdmissionType
    END,                                           -- inst_admit_type_std_id
    CASE 
    WHEN header.Type = 'Institutional'
    THEN header.AdmissionSource
    END,                                           -- inst_admit_source_std_id
    CASE 
    WHEN header.Type = 'Institutional'
    THEN header.DischargeStatus
    END,                                           -- inst_discharge_status_std_id
    CASE 
    WHEN header.Type <> 'Institutional'
    THEN NULL
    WHEN header.InstitutionalType IS NOT NULL
    THEN header.InstitutionalType || header.ClaimFrequencyCode
    END,                                           -- inst_type_of_bill_std_id
    CASE 
    WHEN header.Type = 'Institutional'
    THEN header.DrgCode
    END,                                           -- inst_drg_std_id
    CASE 
    WHEN header.Type <> 'Professional'
    THEN NULL
    WHEN ServiceLine.PlaceOfService IS NOT NULL 
    THEN ServiceLine.PlaceOfService
    ELSE header.InstitutionalType
    END,                                           -- place_of_service_std_id
    serviceline.SequenceNumber,                    -- service_line_number
    diagnosis.DiagnosisCode,                       -- diagnosis_code
    CASE
    WHEN diagnosis.type LIKE 'A%'
    THEN '02'
    WHEN diagnosis.type LIKE 'B%'
    THEN '01'
    END,                                           -- diagnosis_code_qual
    CASE 
    WHEN diagnosis.SequenceNumber = serviceline.diagnosiscodepointer1
    THEN '1'
    WHEN diagnosis.SequenceNumber = serviceline.diagnosiscodepointer2
    THEN '2'
    WHEN diagnosis.SequenceNumber = serviceline.diagnosiscodepointer3
    THEN '3'
    WHEN diagnosis.SequenceNumber = serviceline.diagnosiscodepointer4
    THEN '4'
    END,                                           -- diagnosis_priority
    CASE
    WHEN header.type <> 'Institutional'
    THEN NULL
    WHEN diagnosis.diagnosiscode = header.admissiondiagnosis
    THEN 'Y'
    ELSE 'N'
    END,                                           -- admit_diagnosis_ind
    CASE 
    WHEN header.Type = 'Professional'
    THEN serviceline.ProcedureCode 
    WHEN header.Type = 'Institutional'
    AND serviceline.procedurecode IS NOT NULL 
    THEN serviceline.procedurecode
    WHEN header.Type = 'Institutional'
    AND proc.procedurecode IS NOT NULL
    THEN proc.procedurecode
    END,                                           -- procedure_code
    serviceline.qualifier,                         -- procedure_code_qual
    CASE 
    WHEN header.type <> 'Institutional'
    THEN NULL
    WHEN proc.procedurecode = serviceline.procedurecode
    AND proc.sequencenumber = '1'
    THEN 'Y'
    ELSE 'N'
    END,                                           -- principal_proc_ind
    serviceline.amount,                            -- procedure_units
    serviceline.modifier1,                         -- procedure_modifier_1
    serviceline.modifier2,                         -- procedure_modifier_2
    serviceline.modifier3,                         -- procedure_modifier_3
    serviceline.modifier4,                         -- procedure_modifier_4
    serviceline.revenuecode,                       -- revenue_code
    serviceline.drugcode,                          -- ndc_code
    payer1.claimfileindicator,                     -- medical_coverage_type
    serviceline.linecharge,                        -- line_charge
    header.totalcharge,                            -- total_charge
    CASE
    WHEN rendering.npi IS NOT NULL
    THEN rendering.npi
    WHEN rendering_claim.npi IS NOT NULL
    THEN rendering_claim.npi
    WHEN supervising.npi IS NOT NULL
    THEN supervising.npi
    WHEN supervising_claim.npi IS NOT NULL
    THEN supervising_claim.npi
    WHEN operating.npi IS NOT NULL
    THEN operating.npi
    WHEN operating_claim.npi IS NOT NULL
    THEN operating_claim.npi
    WHEN purchased.npi IS NOT NULL
    THEN purchased.npi
    WHEN purchased_claim.npi IS NOT NULL
    THEN purchased_claim.npi
    WHEN other.npi IS NOT NULL
    THEN other.npi
    WHEN other_claim.npi IS NOT NULL
    THEN other_claim.npi
    END,                                           -- prov_rendering_npi
    billing.npi,                                   -- prov_billing_npi
    COALESCE(referring.npi, referring_claim.npi),  -- prov_referring_npi
    CASE
    WHEN servicelocation.npi IS NOT NULL
    THEN servicelocation.npi
    WHEN servicelocation_claim.npi IS NOT NULL
    THEN servicelocation_claim.npi
    WHEN ambulancedropoff.npi IS NOT NULL
    THEN ambulancedropoff.npi
    WHEN ambulancedropoff_claim.npi IS NOT NULL
    THEN ambulancedropoff_claim.npi
    END,                                           -- prov_facility_npi
    payer1.sourcepayerid,                          -- payer_vendor_id
    payer1.name,                                   -- payer_name
    payer1.payerclassification,                    -- payer_type
    CASE
    WHEN rendering.npi IS NOT NULL
    THEN rendering.lastname
    WHEN rendering_claim.npi IS NOT NULL
    THEN rendering_claim.lastname
    WHEN supervising.npi IS NOT NULL
    THEN supervising.lastname
    WHEN supervising_claim.npi IS NOT NULL
    THEN supervising_claim.lastname
    WHEN operating.npi IS NOT NULL
    THEN operating.lastname
    WHEN operating_claim.npi IS NOT NULL
    THEN operating_claim.lastname
    WHEN purchased.npi IS NOT NULL
    THEN purchased.lastname
    WHEN purchased_claim.npi IS NOT NULL
    THEN purchased_claim.lastname
    WHEN other.npi IS NOT NULL
    THEN other.lastname
    WHEN other_claim.npi IS NOT NULL
    THEN other_claim.lastname
    END,                                           -- prov_rendering_name_1
    CASE
    WHEN rendering.npi IS NOT NULL
    THEN rendering.firstname
    WHEN rendering_claim.npi IS NOT NULL
    THEN rendering_claim.firstname
    WHEN supervising.npi IS NOT NULL
    THEN supervising.firstname
    WHEN supervising_claim.npi IS NOT NULL
    THEN supervising_claim.firstname
    WHEN operating.npi IS NOT NULL
    THEN operating.firstname
    WHEN operating_claim.npi IS NOT NULL
    THEN operating_claim.firstname
    WHEN purchased.npi IS NOT NULL
    THEN purchased.firstname
    WHEN purchased_claim.npi IS NOT NULL
    THEN purchased_claim.firstname
    WHEN other.npi IS NOT NULL
    THEN other.firstname
    WHEN other_claim.npi IS NOT NULL
    THEN other_claim.firstname
    END,                                           -- prov_rendering_name_2
    CASE
    WHEN rendering.npi IS NOT NULL
    THEN rendering.addr1
    WHEN rendering_claim.npi IS NOT NULL
    THEN rendering_claim.addr1
    WHEN supervising.npi IS NOT NULL
    THEN supervising.addr1
    WHEN supervising_claim.npi IS NOT NULL
    THEN supervising_claim.addr1
    WHEN operating.npi IS NOT NULL
    THEN operating.addr1
    WHEN operating_claim.npi IS NOT NULL
    THEN operating_claim.addr1
    WHEN purchased.npi IS NOT NULL
    THEN purchased.addr1
    WHEN purchased_claim.npi IS NOT NULL
    THEN purchased_claim.addr1
    WHEN other.npi IS NOT NULL
    THEN other.addr1
    WHEN other_claim.npi IS NOT NULL
    THEN other_claim.addr1
    END,                                           -- prov_rendering_address_1
    CASE
    WHEN rendering.npi IS NOT NULL
    THEN rendering.addr2
    WHEN rendering_claim.npi IS NOT NULL
    THEN rendering_claim.addr2
    WHEN supervising.npi IS NOT NULL
    THEN supervising.addr2
    WHEN supervising_claim.npi IS NOT NULL
    THEN supervising_claim.addr2
    WHEN operating.npi IS NOT NULL
    THEN operating.addr2
    WHEN operating_claim.npi IS NOT NULL
    THEN operating_claim.addr2
    WHEN purchased.npi IS NOT NULL
    THEN purchased.addr2
    WHEN purchased_claim.npi IS NOT NULL
    THEN purchased_claim.addr2
    WHEN other.npi IS NOT NULL
    THEN other.addr2
    WHEN other_claim.npi IS NOT NULL
    THEN other_claim.addr2
    END,                                           -- prov_rendering_address_2
    CASE
    WHEN rendering.npi IS NOT NULL
    THEN rendering.city
    WHEN rendering_claim.npi IS NOT NULL
    THEN rendering_claim.city
    WHEN supervising.npi IS NOT NULL
    THEN supervising.city
    WHEN supervising_claim.npi IS NOT NULL
    THEN supervising_claim.city
    WHEN operating.npi IS NOT NULL
    THEN operating.city
    WHEN operating_claim.npi IS NOT NULL
    THEN operating_claim.city
    WHEN purchased.npi IS NOT NULL
    THEN purchased.city
    WHEN purchased_claim.npi IS NOT NULL
    THEN purchased_claim.city
    WHEN other.npi IS NOT NULL
    THEN other.city
    WHEN other_claim.npi IS NOT NULL
    THEN other_claim.city
    END,                                           -- prov_rendering_city
    CASE
    WHEN rendering.npi IS NOT NULL
    THEN rendering.state
    WHEN rendering_claim.npi IS NOT NULL
    THEN rendering_claim.state
    WHEN supervising.npi IS NOT NULL
    THEN supervising.state
    WHEN supervising_claim.npi IS NOT NULL
    THEN supervising_claim.state
    WHEN operating.npi IS NOT NULL
    THEN operating.state
    WHEN operating_claim.npi IS NOT NULL
    THEN operating_claim.state
    WHEN purchased.npi IS NOT NULL
    THEN purchased.state
    WHEN purchased_claim.npi IS NOT NULL
    THEN purchased_claim.state
    WHEN other.npi IS NOT NULL
    THEN other.state
    WHEN other_claim.npi IS NOT NULL
    THEN other_claim.state
    END,                                           -- prov_rendering_state
    CASE
    WHEN rendering.npi IS NOT NULL
    THEN rendering.zip
    WHEN rendering_claim.npi IS NOT NULL
    THEN rendering_claim.zip
    WHEN supervising.npi IS NOT NULL
    THEN supervising.zip
    WHEN supervising_claim.npi IS NOT NULL
    THEN supervising_claim.zip
    WHEN operating.npi IS NOT NULL
    THEN operating.zip
    WHEN operating_claim.npi IS NOT NULL
    THEN operating_claim.zip
    WHEN purchased.npi IS NOT NULL
    THEN purchased.zip
    WHEN purchased_claim.npi IS NOT NULL
    THEN purchased_claim.zip
    WHEN other.npi IS NOT NULL
    THEN other.zip
    WHEN other_claim.npi IS NOT NULL
    THEN other_claim.zip
    END,                                           -- prov_rendering_zip
    CASE
    WHEN rendering.npi IS NOT NULL
    THEN rendering.taxonomy
    WHEN rendering_claim.npi IS NOT NULL
    THEN rendering_claim.taxonomy
    WHEN supervising.npi IS NOT NULL
    THEN supervising.taxonomy
    WHEN supervising_claim.npi IS NOT NULL
    THEN supervising_claim.taxonomy
    WHEN operating.npi IS NOT NULL
    THEN operating.taxonomy
    WHEN operating_claim.npi IS NOT NULL
    THEN operating_claim.taxonomy
    WHEN purchased.npi IS NOT NULL
    THEN purchased.taxonomy
    WHEN purchased_claim.npi IS NOT NULL
    THEN purchased_claim.taxonomy
    WHEN other.npi IS NOT NULL
    THEN other.taxonomy
    WHEN other_claim.npi IS NOT NULL
    THEN other_claim.taxonomy
    END,                                           -- prov_rendering_std_taxonomy
    billing.taxid,                                 -- prov_billing_tax_id
    billing.ssn,                                   -- prov_billing_ssn
    billing.stlic,                                 -- prov_billing_state_license
    billing.upin,                                  -- prov_billing_upin
    billing.lastname,                              -- prov_billing_name_1
    billing.firstname,                             -- prov_billing_name_2
    billing.addr1,                                 -- prov_billing_address_1
    billing.addr2,                                 -- prov_billing_address_2
    billing.city,                                  -- prov_billing_city
    billing.state,                                 -- prov_billing_state
    billing.zip,                                   -- prov_billing_zip
    billing.taxonomy,                              -- prov_billing_std_taxonomy
    CASE
    WHEN referring.npi IS NOT NULL
    THEN referring.lastname
    WHEN referring_claim.npi IS NOT NULL
    THEN referring_claim.lastname
    END,                                           -- prov_referring_name_1
    CASE
    WHEN referring.npi IS NOT NULL
    THEN referring.firstname
    WHEN referring_claim.npi IS NOT NULL
    THEN referring_claim.firstname
    END,                                           -- prov_referring_name_2
    CASE
    WHEN referring.npi IS NOT NULL
    THEN referring.addr1
    WHEN referring_claim.npi IS NOT NULL
    THEN referring_claim.addr1
    END,                                           -- prov_referring_address_1
    CASE
    WHEN referring.npi IS NOT NULL
    THEN referring.addr2
    WHEN referring_claim.npi IS NOT NULL
    THEN referring_claim.addr2
    END,                                           -- prov_referring_address_2
    CASE
    WHEN referring.npi IS NOT NULL
    THEN referring.city
    WHEN referring_claim.npi IS NOT NULL
    THEN referring_claim.city
    END,                                           -- prov_referring_city
    CASE
    WHEN referring.npi IS NOT NULL
    THEN referring.state
    WHEN referring_claim.npi IS NOT NULL
    THEN referring_claim.state
    END,                                           -- prov_referring_state
    CASE
    WHEN referring.npi IS NOT NULL
    THEN referring.zip
    WHEN referring_claim.npi IS NOT NULL
    THEN referring_claim.zip
    END,                                           -- prov_referring_zip
    CASE
    WHEN referring.npi IS NOT NULL
    THEN referring.taxonomy
    WHEN referring_claim.npi IS NOT NULL
    THEN referring_claim.taxonomy
    END,                                           -- prov_referring_std_taxonomy
    CASE
    WHEN servicelocation.npi IS NOT NULL
    THEN servicelocation.lastname
    WHEN servicelocation_claim.npi IS NOT NULL
    THEN servicelocation_claim.lastname
    WHEN ambulancedropoff.npi IS NOT NULL
    THEN ambulancedropoff.lastname
    WHEN ambulancedropoff_claim.npi IS NOT NULL
    THEN ambulancedropoff_claim.lastname
    END,                                           -- prov_facility_name_1
    CASE
    WHEN servicelocation.npi IS NOT NULL
    THEN servicelocation.firstname
    WHEN servicelocation_claim.npi IS NOT NULL
    THEN servicelocation_claim.firstname
    WHEN ambulancedropoff.npi IS NOT NULL
    THEN ambulancedropoff.firstname
    WHEN ambulancedropoff_claim.npi IS NOT NULL
    THEN ambulancedropoff_claim.firstname
    END,                                           -- prov_facility_name_2
    CASE
    WHEN servicelocation.npi IS NOT NULL
    THEN servicelocation.addr1
    WHEN servicelocation_claim.npi IS NOT NULL
    THEN servicelocation_claim.addr1
    WHEN ambulancedropoff.npi IS NOT NULL
    THEN ambulancedropoff.addr1
    WHEN ambulancedropoff_claim.npi IS NOT NULL
    THEN ambulancedropoff_claim.addr1
    END,                                           -- prov_facility_address_1
    CASE
    WHEN servicelocation.npi IS NOT NULL
    THEN servicelocation.addr2
    WHEN servicelocation_claim.npi IS NOT NULL
    THEN servicelocation_claim.addr2
    WHEN ambulancedropoff.npi IS NOT NULL
    THEN ambulancedropoff.addr2
    WHEN ambulancedropoff_claim.npi IS NOT NULL
    THEN ambulancedropoff_claim.addr2
    END,                                           -- prov_facility_address_2
    CASE
    WHEN servicelocation.npi IS NOT NULL
    THEN servicelocation.city
    WHEN servicelocation_claim.npi IS NOT NULL
    THEN servicelocation_claim.city
    WHEN ambulancedropoff.npi IS NOT NULL
    THEN ambulancedropoff.city
    WHEN ambulancedropoff_claim.npi IS NOT NULL
    THEN ambulancedropoff_claim.city
    END,                                           -- prov_facility_city
    CASE
    WHEN servicelocation.npi IS NOT NULL
    THEN servicelocation.state
    WHEN servicelocation_claim.npi IS NOT NULL
    THEN servicelocation_claim.state
    WHEN ambulancedropoff.npi IS NOT NULL
    THEN ambulancedropoff.state
    WHEN ambulancedropoff_claim.npi IS NOT NULL
    THEN ambulancedropoff_claim.state
    END,                                           -- prov_facility_state
    CASE
    WHEN servicelocation.npi IS NOT NULL
    THEN servicelocation.zip
    WHEN servicelocation_claim.npi IS NOT NULL
    THEN servicelocation_claim.zip
    WHEN ambulancedropoff.npi IS NOT NULL
    THEN ambulancedropoff.zip
    WHEN ambulancedropoff_claim.npi IS NOT NULL
    THEN ambulancedropoff_claim.zip
    END,                                           -- prov_facility_zip
    CASE
    WHEN servicelocation.npi IS NOT NULL
    THEN servicelocation.taxonomy
    WHEN servicelocation_claim.npi IS NOT NULL
    THEN servicelocation_claim.taxonomy
    WHEN ambulancedropoff.npi IS NOT NULL
    THEN ambulancedropoff.taxonomy
    WHEN ambulancedropoff_claim.npi IS NOT NULL
    THEN ambulancedropoff_claim.taxonomy
    END,                                           -- prov_facility_std_taxonomy
    payer2.sequencenumber,                         -- cob_payer_seq_code_1
    payer2.payerid,                                -- cob_payer_hpid_1
    payer2.claimfileindicator,                     -- cob_payer_claim_filing_ind_code_1
    payer2.payerclassification,                    -- cob_ins_type_code_1
    payer3.sequencenumber,                         -- cob_payer_seq_code_2
    payer3.payerid,                                -- cob_payer_hpid_2
    payer3.claimfileindicator,                     -- cob_payer_claim_filing_ind_code_2
    payer3.payerclassification                     -- cob_ins_type_code_2
FROM transactional_header header 
    LEFT JOIN matching_payload mp ON header.claimid = mp.claimid
    LEFT JOIN transactional_serviceline serviceline ON header.claimid = serviceline.claimid
    LEFT JOIN transactional_billing billing ON header.claimid = billing.claimid

    -- providers
    LEFT JOIN transactional_servicelineaffiliation rendering ON serviceline.servicelineid = rendering.servicelineid 
    AND serviceline.claimid = rendering.claimid
    AND rendering.type = 'Rendering'
    LEFT JOIN transactional_claimaffiliation rendering_claim ON header.claimid = rendering_claim.claimid 
    AND rendering_claim.type = 'Rendering'
    LEFT JOIN transactional_servicelineaffiliation referring ON serviceline.servicelineid = referring.servicelineid 
    AND serviceline.claimid = referring.claimid
    AND referring.type = 'Referring'
    LEFT JOIN transactional_claimaffiliation referring_claim ON header.claimid = referring_claim.claimid 
    AND referring_claim.type = 'Referring'
    LEFT JOIN transactional_servicelineaffiliation servicelocation ON serviceline.servicelineid = servicelocation.servicelineid 
    AND serviceline.claimid = servicelocation.claimid
    AND servicelocation.type = 'ServiceLocation'
    LEFT JOIN transactional_claimaffiliation servicelocation_claim ON header.claimid = servicelocation_claim.claimid 
    AND servicelocation_claim.type = 'ServiceLocation'
    LEFT JOIN transactional_servicelineaffiliation ambulancedropoff ON serviceline.servicelineid = ambulancedropoff.servicelineid 
    AND serviceline.claimid = ambulancedropoff.claimid
    AND ambulancedropoff.type = 'AmbulanceDropOff'
    LEFT JOIN transactional_claimaffiliation ambulancedropoff_claim ON header.claimid = ambulancedropoff_claim.claimid 
    AND ambulancedropoff_claim.type = 'AmbulanceDropOff'
    LEFT JOIN transactional_servicelineaffiliation supervising ON serviceline.servicelineid = supervising.servicelineid 
    AND serviceline.claimid = supervising.claimid
    AND supervising.type = 'Supervising'
    LEFT JOIN transactional_claimaffiliation supervising_claim ON header.claimid = supervising_claim.claimid 
    AND supervising_claim.type = 'Supervising'
    LEFT JOIN transactional_servicelineaffiliation operating ON serviceline.servicelineid = operating.servicelineid 
    AND serviceline.claimid = operating.claimid
    AND operating.type = 'Operating'
    LEFT JOIN transactional_claimaffiliation operating_claim ON header.claimid = operating_claim.claimid 
    AND operating_claim.type = 'Operating'
    LEFT JOIN transactional_servicelineaffiliation purchased ON serviceline.servicelineid = purchased.servicelineid 
    AND serviceline.claimid = purchased.claimid
    AND purchased.type = 'Purchased'
    LEFT JOIN transactional_claimaffiliation purchased_claim ON header.claimid = purchased_claim.claimid 
    AND purchased_claim.type = 'Purchased'
    LEFT JOIN transactional_servicelineaffiliation other ON bserviceline.servicelineid = other.servicelineid 
    AND serviceline.claimid = other.claimid
    AND other.type = 'Other'
    LEFT JOIN transactional_claimaffiliation other_claim ON header.claimid = other_claim.claimid 
    AND other_claim.type = 'Other'

    -- payers
    LEFT JOIN transactional_payer payer1 ON header.claimid = payer1.claimid
    AND payer1.sequencenumber = '1'
    LEFT JOIN transactional_payer payer2 ON header.claimid = payer2.claimid
    AND payer2.sequencenumber = '2'
    LEFT JOIN transactional_payer payer3 ON header.claimid = payer3.claimid
    AND payer3.sequencenumber = '3'

    -- diag/proc
    LEFT JOIN transactional_diagnosis diagnosis ON diagnosis.claimid = header.claimid
    AND header.Type = 'Professional'
    AND (
        serviceline.diagnosiscodepointer1 = diagnosis.sequencenumber 
        OR serviceline.diagnosiscodepointer2 = diagnosis.sequencenumber 
        OR serviceline.diagnosiscodepointer3 = diagnosis.sequencenumber 
        OR serviceline.diagnosiscodepointer4 = diagnosis.sequencenumber 
        )
    LEFT JOIN transactional_procedure proc ON proc.claimid = header.claimid

    -- clean dates
    LEFT JOIN dates process_date ON header.ProcessDate = process_date.formatted
    LEFT JOIN dates svc_start_date ON serviceline.ServiceStart = svc_start_date.formatted
    LEFT JOIN dates svc_end_date ON serviceline.ServiceEnd = svc_end_date.formatted
    LEFT JOIN dates start_date ON header.StartDate = start_date.formatted
    LEFT JOIN dates end_date ON header.EndDate = end_date.formatted
;

-- insert rows for professional claims that do not correspond to a service line
INSERT INTO medicalclaims_common_model (
        claim_id,
        hvid,
        source_version,
        patient_gender,
        patient_age,
        patient_year_of_birth,
        patient_zip3,
        patient_state,
        claim_type,
        date_received,
        date_service,
        date_service_end,
        place_of_service_std_id,
        diagnosis_code,
        diagnosis_code_qual,
        medical_coverage_type,
        total_charge,
        prov_rendering_npi,
        prov_billing_npi,
        prov_referring_npi,
        prov_facility_npi,
        payer_vendor_id,
        payer_name,
        payer_type,
        prov_rendering_name_1,
        prov_rendering_name_2,
        prov_rendering_address_1,
        prov_rendering_address_2,
        prov_rendering_city,
        prov_rendering_state,
        prov_rendering_zip,
        prov_rendering_std_taxonomy,
        prov_billing_tax_id,
        prov_billing_ssn,
        prov_billing_state_license,
        prov_billing_upin,
        prov_billing_name_1,
        prov_billing_name_2,
        prov_billing_address_1,
        prov_billing_address_2,
        prov_billing_city,
        prov_billing_state,
        prov_billing_zip,
        prov_billing_std_taxonomy,
        prov_referring_name_1,
        prov_referring_name_2,
        prov_referring_address_1,
        prov_referring_address_2,
        prov_referring_city,
        prov_referring_state,
        prov_referring_zip,
        prov_referring_std_taxonomy,
        prov_facility_name_1,
        prov_facility_name_2,
        prov_facility_address_1,
        prov_facility_address_2,
        prov_facility_city,
        prov_facility_state,
        prov_facility_zip,
        prov_facility_std_taxonomy,
        cob_payer_seq_code_1,
        cob_payer_hpid_1,
        cob_payer_claim_filing_ind_code_1,
        cob_ins_type_code_1,
        cob_payer_seq_code_2,
        cob_payer_hpid_2,
        cob_payer_claim_filing_ind_code_2,
        cob_ins_type_code_2
        ) 
SELECT DISTINCT
    header.ClaimId,                  -- claim_id
    COALESCE(mp.parentid, mp.hvid),  -- hvid
    1,                               -- source_version
    mp.gender,                       -- patient_gender
    mp.age,                          -- patient_age
    mp.yearOfBirth,                  -- patient_year_of_birth
    mp.threeDigitZip,                -- patient_zip3
    mp.state,                        -- patient_state
    'P',                             -- claim_type
    process_date.formatted,          -- date_received
    CASE 
    WHEN start_date.formatted IS NOT NULL
    THEN start_date.formatted
    ELSE (
    SELECT MIN(clean.formatted) 
    FROM transactional_serviceline sl2
        LEFT JOIN dates clean ON sl2.ServiceStart = clean.formatted
    WHERE sl2.ClaimId = header.ClaimId
        )
    END,                             -- date_service
    CASE 
    WHEN start_date.formatted IS NOT NULL
    THEN header.EndDate
    ELSE (
    SELECT MIN(clean.formatted) 
    FROM transactional_serviceline sl2
        LEFT JOIN dates clean ON sl2.ServiceEnd = clean.formatted
    WHERE sl2.ClaimId = header.ClaimId
        )
    END,                             -- date_service_end
    header.InstitutionalType,        -- place_of_service_std_id
    diagnosis.DiagnosisCode,         -- diagnosis_code
    CASE
    WHEN diagnosis.type LIKE 'A%'
    THEN '02'
    WHEN diagnosis.type LIKE 'B%'
    THEN '01'
    END,                             -- diagnosis_code_qual
    payer1.claimfileindicator,       -- medical_coverage_type
    header.totalcharge,              -- total_charge
    CASE
    WHEN rendering_claim.npi IS NOT NULL
    THEN rendering_claim.npi
    WHEN supervising_claim.npi IS NOT NULL
    THEN supervising_claim.npi
    WHEN operating_claim.npi IS NOT NULL
    THEN operating_claim.npi
    WHEN purchased_claim.npi IS NOT NULL
    THEN purchased_claim.npi
    WHEN other_claim.npi IS NOT NULL
    THEN other_claim.npi
    END,                             -- prov_rendering_npi
    billing.npi,                     -- prov_billing_npi
    referring_claim.npi,             -- prov_referring_npi
    CASE
    WHEN servicelocation_claim.npi IS NOT NULL
    THEN servicelocation_claim.npi
    WHEN ambulancedropoff_claim.npi IS NOT NULL
    THEN ambulancedropoff_claim.npi
    END,                             -- prov_facility_npi
    payer1.sourcepayerid,            -- payer_vendor_id
    payer1.name,                     -- payer_name
    payer1.payerclassification,      -- payer_type
    CASE
    WHEN rendering_claim.npi IS NOT NULL
    THEN rendering_claim.lastname
    WHEN supervising_claim.npi IS NOT NULL
    THEN supervising_claim.lastname
    WHEN operating_claim.npi IS NOT NULL
    THEN operating_claim.lastname
    WHEN purchased_claim.npi IS NOT NULL
    THEN purchased_claim.lastname
    WHEN other_claim.npi IS NOT NULL
    THEN other_claim.lastname
    END,                             -- prov_rendering_name_1
    CASE
    WHEN rendering_claim.npi IS NOT NULL
    THEN rendering_claim.firstname
    WHEN supervising_claim.npi IS NOT NULL
    THEN supervising_claim.firstname
    WHEN operating_claim.npi IS NOT NULL
    THEN operating_claim.firstname
    WHEN purchased_claim.npi IS NOT NULL
    THEN purchased_claim.firstname
    WHEN other_claim.npi IS NOT NULL
    THEN other_claim.firstname
    END,                             -- prov_rendering_name_2
    CASE
    WHEN rendering_claim.npi IS NOT NULL
    THEN rendering_claim.addr1
    WHEN supervising_claim.npi IS NOT NULL
    THEN supervising_claim.addr1
    WHEN operating_claim.npi IS NOT NULL
    THEN operating_claim.addr1
    WHEN purchased_claim.npi IS NOT NULL
    THEN purchased_claim.addr1
    WHEN other_claim.npi IS NOT NULL
    THEN other_claim.addr1
    END,                             -- prov_rendering_address_1
    CASE
    WHEN rendering_claim.npi IS NOT NULL
    THEN rendering_claim.addr2
    WHEN supervising_claim.npi IS NOT NULL
    THEN supervising_claim.addr2
    WHEN operating_claim.npi IS NOT NULL
    THEN operating_claim.addr2
    WHEN purchased_claim.npi IS NOT NULL
    THEN purchased_claim.addr2
    WHEN other_claim.npi IS NOT NULL
    THEN other_claim.addr2
    END,                             -- prov_rendering_address_2
    CASE
    WHEN rendering_claim.npi IS NOT NULL
    THEN rendering_claim.city
    WHEN supervising_claim.npi IS NOT NULL
    THEN supervising_claim.city
    WHEN operating_claim.npi IS NOT NULL
    THEN operating_claim.city
    WHEN purchased_claim.npi IS NOT NULL
    THEN purchased_claim.city
    WHEN other_claim.npi IS NOT NULL
    THEN other_claim.city
    END,                             -- prov_rendering_city
    CASE
    WHEN rendering_claim.npi IS NOT NULL
    THEN rendering_claim.state
    WHEN supervising_claim.npi IS NOT NULL
    THEN supervising_claim.state
    WHEN operating_claim.npi IS NOT NULL
    THEN operating_claim.state
    WHEN purchased_claim.npi IS NOT NULL
    THEN purchased_claim.state
    WHEN other_claim.npi IS NOT NULL
    THEN other_claim.state
    END,                             -- prov_rendering_state
    CASE
    WHEN rendering_claim.npi IS NOT NULL
    THEN rendering_claim.zip
    WHEN supervising_claim.npi IS NOT NULL
    THEN supervising_claim.zip
    WHEN operating_claim.npi IS NOT NULL
    THEN operating_claim.zip
    WHEN purchased_claim.npi IS NOT NULL
    THEN purchased_claim.zip
    WHEN other_claim.npi IS NOT NULL
    THEN other_claim.zip
    END,                             -- prov_rendering_zip
    CASE
    WHEN rendering_claim.npi IS NOT NULL
    THEN rendering_claim.taxonomy
    WHEN supervising_claim.npi IS NOT NULL
    THEN supervising_claim.taxonomy
    WHEN operating_claim.npi IS NOT NULL
    THEN operating_claim.taxonomy
    WHEN purchased_claim.npi IS NOT NULL
    THEN purchased_claim.taxonomy
    WHEN other_claim.npi IS NOT NULL
    THEN other_claim.taxonomy
    END,                             -- prov_rendering_std_taxonomy
    billing.taxid,                   -- prov_billing_tax_id
    billing.ssn,                     -- prov_billing_ssn
    billing.stlic,                   -- prov_billing_state_license
    billing.upin,                    -- prov_billing_upin
    billing.lastname,                -- prov_billing_name_1
    billing.firstname,               -- prov_billing_name_2
    billing.addr1,                   -- prov_billing_address_1
    billing.addr2,                   -- prov_billing_address_2
    billing.city,                    -- prov_billing_city
    billing.state,                   -- prov_billing_state
    billing.zip,                     -- prov_billing_zip
    billing.taxonomy,                -- prov_billing_std_taxonomy
    referring_claim.lastname,        -- prov_referring_name_1
    referring_claim.firstname,       -- prov_referring_name_2
    referring_claim.addr1,           -- prov_referring_address_1
    referring_claim.addr2,           -- prov_referring_address_2
    referring_claim.city,            -- prov_referring_city
    referring_claim.state,           -- prov_referring_state
    referring_claim.zip,             -- prov_referring_zip
    referring_claim.taxonomy,        -- prov_referring_std_taxonomy
    CASE
    WHEN servicelocation_claim.npi IS NOT NULL
    THEN servicelocation_claim.lastname
    WHEN ambulancedropoff_claim.npi IS NOT NULL
    THEN ambulancedropoff_claim.lastname
    END,                             -- prov_facility_name_1
    CASE
    WHEN servicelocation_claim.npi IS NOT NULL
    THEN servicelocation_claim.firstname
    WHEN ambulancedropoff_claim.npi IS NOT NULL
    THEN ambulancedropoff_claim.firstname
    END,                             -- prov_facility_name_2
    CASE
    WHEN servicelocation_claim.npi IS NOT NULL
    THEN servicelocation_claim.addr1
    WHEN ambulancedropoff_claim.npi IS NOT NULL
    THEN ambulancedropoff_claim.addr1
    END,                             -- prov_facility_address_1
    CASE
    WHEN servicelocation_claim.npi IS NOT NULL
    THEN servicelocation_claim.addr2
    WHEN ambulancedropoff_claim.npi IS NOT NULL
    THEN ambulancedropoff_claim.addr2
    END,                             -- prov_facility_address_2
    CASE
    WHEN servicelocation_claim.npi IS NOT NULL
    THEN servicelocation_claim.city
    WHEN ambulancedropoff_claim.npi IS NOT NULL
    THEN ambulancedropoff_claim.city
    END,                             -- prov_facility_city
    CASE
    WHEN servicelocation_claim.npi IS NOT NULL
    THEN servicelocation_claim.state
    WHEN ambulancedropoff_claim.npi IS NOT NULL
    THEN ambulancedropoff_claim.state
    END,                             -- prov_facility_state
    CASE
    WHEN servicelocation_claim.npi IS NOT NULL
    THEN servicelocation_claim.zip
    WHEN ambulancedropoff_claim.npi IS NOT NULL
    THEN ambulancedropoff_claim.zip
    END,                             -- prov_facility_zip
    CASE
    WHEN servicelocation_claim.npi IS NOT NULL
    THEN servicelocation_claim.taxonomy
    WHEN ambulancedropoff_claim.npi IS NOT NULL
    THEN ambulancedropoff_claim.taxonomy
    END,                             -- prov_facility_std_taxonomy
    payer2.sequencenumber,           -- cob_payer_seq_code_1
    payer2.payerid,                  -- cob_payer_hpid_1
    payer2.claimfileindicator,       -- cob_payer_claim_filing_ind_code_1
    payer2.payerclassification,      -- cob_ins_type_code_1
    payer3.sequencenumber,           -- cob_payer_seq_code_2
    payer3.payerid,                  -- cob_payer_hpid_2
    payer3.claimfileindicator,       -- cob_payer_claim_filing_ind_code_2
    payer3.payerclassification       -- cob_ins_type_code_2
FROM transactional_header header 
    LEFT JOIN matching_payload mp ON header.claimid = mp.claimid
    LEFT JOIN transactional_billing billing ON header.claimid = billing.claimid

    -- providers
    LEFT JOIN transactional_claimaffiliation rendering_claim ON header.claimid = rendering_claim.claimid 
    AND rendering_claim.type = 'Rendering'
    LEFT JOIN transactional_claimaffiliation referring_claim ON header.claimid = referring_claim.claimid 
    AND referring_claim.type = 'Referring'
    LEFT JOIN transactional_claimaffiliation servicelocation_claim ON header.claimid = servicelocation_claim.claimid 
    AND servicelocation_claim.type = 'ServiceLocation'
    LEFT JOIN transactional_claimaffiliation ambulancedropoff_claim ON header.claimid = ambulancedropoff_claim.claimid 
    AND ambulancedropoff_claim.type = 'AmbulanceDropOff'
    LEFT JOIN transactional_claimaffiliation supervising_claim ON header.claimid = supervising_claim.claimid 
    AND supervising_claim.type = 'Supervising'
    LEFT JOIN transactional_claimaffiliation operating_claim ON header.claimid = operating_claim.claimid 
    AND operating_claim.type = 'Operating'
    LEFT JOIN transactional_claimaffiliation purchased_claim ON header.claimid = purchased_claim.claimid 
    AND purchased_claim.type = 'Purchased'
    LEFT JOIN transactional_claimaffiliation other_claim ON header.claimid = other_claim.claimid 
    AND other_claim.type = 'Other'

    -- payer
    LEFT JOIN transactional_payer payer1 ON header.claimid = payer1.claimid
    AND payer1.sequencenumber = '1'
    LEFT JOIN transactional_payer payer2 ON header.claimid = payer2.claimid
    AND payer2.sequencenumber = '2'
    LEFT JOIN transactional_payer payer3 ON header.claimid = payer3.claimid
    AND payer3.sequencenumber = '3'

    -- diag
    LEFT JOIN transactional_diagnosis diagnosis ON diagnosis.claimid = header.claimid

    -- clean dates
    LEFT JOIN dates process_date ON header.ProcessDate = process_date.formatted
    LEFT JOIN dates start_date ON header.StartDate = start_date.formatted
    LEFT JOIN dates end_date ON header.EndDate = end_date.formatted

WHERE header.Type = 'Professional'
    AND diagnosis.diagnosiscode NOT IN (
    SELECT m2.diagnosis_code 
    FROM medicalclaims_common_model m2
    WHERE m2.claim_id = header.claimid
        )
;

-- insert rows for institutional claims that do not correspond to a service line
INSERT INTO medicalclaims_common_model (
        claim_id,
        hvid,
        source_version,
        patient_gender,
        patient_age,
        patient_year_of_birth,
        patient_zip3,
        patient_state,
        claim_type,
        date_received,
        date_service,
        date_service_end,
        inst_admit_type_std_id,
        inst_admit_source_std_id,
        inst_discharge_status_std_id,
        inst_type_of_bill_std_id,
        inst_drg_std_id,
        diagnosis_code,
        diagnosis_code_qual,
        medical_coverage_type,
        total_charge,
        prov_rendering_npi,
        prov_billing_npi,
        prov_referring_npi,
        prov_facility_npi,
        payer_vendor_id,
        payer_name,
        payer_type,
        prov_rendering_name_1,
        prov_rendering_name_2,
        prov_rendering_address_1,
        prov_rendering_address_2,
        prov_rendering_city,
        prov_rendering_state,
        prov_rendering_zip,
        prov_rendering_std_taxonomy,
        prov_billing_tax_id,
        prov_billing_ssn,
        prov_billing_state_license,
        prov_billing_upin,
        prov_billing_name_1,
        prov_billing_name_2,
        prov_billing_address_1,
        prov_billing_address_2,
        prov_billing_city,
        prov_billing_state,
        prov_billing_zip,
        prov_billing_std_taxonomy,
        prov_referring_name_1,
        prov_referring_name_2,
        prov_referring_address_1,
        prov_referring_address_2,
        prov_referring_city,
        prov_referring_state,
        prov_referring_zip,
        prov_referring_std_taxonomy,
        prov_facility_name_1,
        prov_facility_name_2,
        prov_facility_address_1,
        prov_facility_address_2,
        prov_facility_city,
        prov_facility_state,
        prov_facility_zip,
        prov_facility_std_taxonomy,
        cob_payer_seq_code_1,
        cob_payer_hpid_1,
        cob_payer_claim_filing_ind_code_1,
        cob_ins_type_code_1,
        cob_payer_seq_code_2,
        cob_payer_hpid_2,
        cob_payer_claim_filing_ind_code_2,
        cob_ins_type_code_2
        ) 
SELECT DISTINCT
    header.ClaimId,                  -- claim_id
    COALESCE(mp.parentid, mp.hvid),  -- hvid
    1,                               -- source_version
    mp.gender,                       -- patient_gender
    mp.age,                          -- patient_age
    mp.yearOfBirth,                  -- patient_year_of_birth
    mp.threeDigitZip,                -- patient_zip3
    mp.state,                        -- patient_state
    'I',                             -- claim_type
    process_date.formatted,          -- date_received
    CASE 
    WHEN start_date.formatted IS NOT NULL
    THEN start_date.formatted
    ELSE (
    SELECT MIN(clean.formatted) 
    FROM transactional_serviceline sl2
        LEFT JOIN dates clean ON sl2.ServiceStart = clean.formatted
    WHERE sl2.ClaimId = header.ClaimId
        )
    END,                             -- date_service
    CASE 
    WHEN start_date.formatted IS NOT NULL
    THEN header.EndDate
    ELSE (
    SELECT MIN(clean.formatted) 
    FROM transactional_serviceline sl2
        LEFT JOIN dates clean ON sl2.ServiceEnd = clean.formatted
    WHERE sl2.ClaimId = header.ClaimId
        )
    END,                             -- date_service_end
    header.AdmissionType,            -- inst_admit_type_std_id
    header.AdmissionSource,          -- inst_admit_source_std_id
    header.DischargeStatus,          -- inst_discharge_status_std_id
    CASE 
    WHEN header.InstitutionalType IS NOT NULL
    THEN header.InstitutionalType || header.ClaimFrequencyCode
    END,                             -- inst_type_of_bill_std_id
    header.DrgCode,                  -- inst_drg_std_id
    diagnosis.DiagnosisCode,         -- diagnosis_code
    CASE
    WHEN diagnosis.type LIKE 'A%'
    THEN '02'
    WHEN diagnosis.type LIKE 'B%'
    THEN '01'
    END,                             -- diagnosis_code_qual
    payer1.claimfileindicator,       -- medical_coverage_type
    header.totalcharge,              -- total_charge
    CASE
    WHEN rendering_claim.npi IS NOT NULL
    THEN rendering_claim.npi
    WHEN supervising_claim.npi IS NOT NULL
    THEN supervising_claim.npi
    WHEN operating_claim.npi IS NOT NULL
    THEN operating_claim.npi
    WHEN purchased_claim.npi IS NOT NULL
    THEN purchased_claim.npi
    WHEN other_claim.npi IS NOT NULL
    THEN other_claim.npi
    END,                             -- prov_rendering_npi
    billing.npi,                     -- prov_billing_npi
    referring_claim.npi,             -- prov_referring_npi
    CASE
    WHEN servicelocation_claim.npi IS NOT NULL
    THEN servicelocation_claim.npi
    WHEN ambulancedropoff_claim.npi IS NOT NULL
    THEN ambulancedropoff_claim.npi
    END,                             -- prov_facility_npi
    payer1.sourcepayerid,            -- payer_vendor_id
    payer1.name,                     -- payer_name
    payer1.payerclassification,      -- payer_type
    CASE
    WHEN rendering_claim.npi IS NOT NULL
    THEN rendering_claim.lastname
    WHEN supervising_claim.npi IS NOT NULL
    THEN supervising_claim.lastname
    WHEN operating_claim.npi IS NOT NULL
    THEN operating_claim.lastname
    WHEN purchased_claim.npi IS NOT NULL
    THEN purchased_claim.lastname
    WHEN other_claim.npi IS NOT NULL
    THEN other_claim.lastname
    END,                             -- prov_rendering_name_1
    CASE
    WHEN rendering_claim.npi IS NOT NULL
    THEN rendering_claim.firstname
    WHEN supervising_claim.npi IS NOT NULL
    THEN supervising_claim.firstname
    WHEN operating_claim.npi IS NOT NULL
    THEN operating_claim.firstname
    WHEN purchased_claim.npi IS NOT NULL
    THEN purchased_claim.firstname
    WHEN other_claim.npi IS NOT NULL
    THEN other_claim.firstname
    END,                             -- prov_rendering_name_2
    CASE
    WHEN rendering_claim.npi IS NOT NULL
    THEN rendering_claim.addr1
    WHEN supervising_claim.npi IS NOT NULL
    THEN supervising_claim.addr1
    WHEN operating_claim.npi IS NOT NULL
    THEN operating_claim.addr1
    WHEN purchased_claim.npi IS NOT NULL
    THEN purchased_claim.addr1
    WHEN other_claim.npi IS NOT NULL
    THEN other_claim.addr1
    END,                             -- prov_rendering_address_1
    CASE
    WHEN rendering_claim.npi IS NOT NULL
    THEN rendering_claim.addr2
    WHEN supervising_claim.npi IS NOT NULL
    THEN supervising_claim.addr2
    WHEN operating_claim.npi IS NOT NULL
    THEN operating_claim.addr2
    WHEN purchased_claim.npi IS NOT NULL
    THEN purchased_claim.addr2
    WHEN other_claim.npi IS NOT NULL
    THEN other_claim.addr2
    END,                             -- prov_rendering_address_2
    CASE
    WHEN rendering_claim.npi IS NOT NULL
    THEN rendering_claim.city
    WHEN supervising_claim.npi IS NOT NULL
    THEN supervising_claim.city
    WHEN operating_claim.npi IS NOT NULL
    THEN operating_claim.city
    WHEN purchased_claim.npi IS NOT NULL
    THEN purchased_claim.city
    WHEN other_claim.npi IS NOT NULL
    THEN other_claim.city
    END,                             -- prov_rendering_city
    CASE
    WHEN rendering_claim.npi IS NOT NULL
    THEN rendering_claim.state
    WHEN supervising_claim.npi IS NOT NULL
    THEN supervising_claim.state
    WHEN operating_claim.npi IS NOT NULL
    THEN operating_claim.state
    WHEN purchased_claim.npi IS NOT NULL
    THEN purchased_claim.state
    WHEN other_claim.npi IS NOT NULL
    THEN other_claim.state
    END,                             -- prov_rendering_state
    CASE
    WHEN rendering_claim.npi IS NOT NULL
    THEN rendering_claim.zip
    WHEN supervising_claim.npi IS NOT NULL
    THEN supervising_claim.zip
    WHEN operating_claim.npi IS NOT NULL
    THEN operating_claim.zip
    WHEN purchased_claim.npi IS NOT NULL
    THEN purchased_claim.zip
    WHEN other_claim.npi IS NOT NULL
    THEN other_claim.zip
    END,                             -- prov_rendering_zip
    CASE
    WHEN rendering_claim.npi IS NOT NULL
    THEN rendering_claim.taxonomy
    WHEN supervising_claim.npi IS NOT NULL
    THEN supervising_claim.taxonomy
    WHEN operating_claim.npi IS NOT NULL
    THEN operating_claim.taxonomy
    WHEN purchased_claim.npi IS NOT NULL
    THEN purchased_claim.taxonomy
    WHEN other_claim.npi IS NOT NULL
    THEN other_claim.taxonomy
    END,                             -- prov_rendering_std_taxonomy
    billing.taxid,                   -- prov_billing_tax_id
    billing.ssn,                     -- prov_billing_ssn
    billing.stlic,                   -- prov_billing_state_license
    billing.upin,                    -- prov_billing_upin
    billing.lastname,                -- prov_billing_name_1
    billing.firstname,               -- prov_billing_name_2
    billing.addr1,                   -- prov_billing_address_1
    billing.addr2,                   -- prov_billing_address_2
    billing.city,                    -- prov_billing_city
    billing.state,                   -- prov_billing_state
    billing.zip,                     -- prov_billing_zip
    billing.taxonomy,                -- prov_billing_std_taxonomy
    referring_claim.lastname,        -- prov_referring_name_1
    referring_claim.firstname,       -- prov_referring_name_2
    referring_claim.addr1,           -- prov_referring_address_1
    referring_claim.addr2,           -- prov_referring_address_2
    referring_claim.city,            -- prov_referring_city
    referring_claim.state,           -- prov_referring_state
    referring_claim.zip,             -- prov_referring_zip
    referring_claim.taxonomy,        -- prov_referring_std_taxonomy
    CASE
    WHEN servicelocation_claim.npi IS NOT NULL
    THEN servicelocation_claim.lastname
    WHEN ambulancedropoff_claim.npi IS NOT NULL
    THEN ambulancedropoff_claim.lastname
    END,                             -- prov_facility_name_1
    CASE
    WHEN servicelocation_claim.npi IS NOT NULL
    THEN servicelocation_claim.firstname
    WHEN ambulancedropoff_claim.npi IS NOT NULL
    THEN ambulancedropoff_claim.firstname
    END,                             -- prov_facility_name_2
    CASE
    WHEN servicelocation_claim.npi IS NOT NULL
    THEN servicelocation_claim.addr1
    WHEN ambulancedropoff_claim.npi IS NOT NULL
    THEN ambulancedropoff_claim.addr1
    END,                             -- prov_facility_address_1
    CASE
    WHEN servicelocation_claim.npi IS NOT NULL
    THEN servicelocation_claim.addr2
    WHEN ambulancedropoff_claim.npi IS NOT NULL
    THEN ambulancedropoff_claim.addr2
    END,                             -- prov_facility_address_2
    CASE
    WHEN servicelocation_claim.npi IS NOT NULL
    THEN servicelocation_claim.city
    WHEN ambulancedropoff_claim.npi IS NOT NULL
    THEN ambulancedropoff_claim.city
    END,                             -- prov_facility_city
    CASE
    WHEN servicelocation_claim.npi IS NOT NULL
    THEN servicelocation_claim.state
    WHEN ambulancedropoff_claim.npi IS NOT NULL
    THEN ambulancedropoff_claim.state
    END,                             -- prov_facility_state
    CASE
    WHEN servicelocation_claim.npi IS NOT NULL
    THEN servicelocation_claim.zip
    WHEN ambulancedropoff_claim.npi IS NOT NULL
    THEN ambulancedropoff_claim.zip
    END,                             -- prov_facility_zip
    CASE
    WHEN servicelocation_claim.npi IS NOT NULL
    THEN servicelocation_claim.taxonomy
    WHEN ambulancedropoff_claim.npi IS NOT NULL
    THEN ambulancedropoff_claim.taxonomy
    END,                             -- prov_facility_std_taxonomy
    payer2.sequencenumber,           -- cob_payer_seq_code_1
    payer2.payerid,                  -- cob_payer_hpid_1
    payer2.claimfileindicator,       -- cob_payer_claim_filing_ind_code_1
    payer2.payerclassification,      -- cob_ins_type_code_1
    payer3.sequencenumber,           -- cob_payer_seq_code_2
    payer3.payerid,                  -- cob_payer_hpid_2
    payer3.claimfileindicator,       -- cob_payer_claim_filing_ind_code_2
    payer3.payerclassification       -- cob_ins_type_code_2
FROM transactional_header header 
    LEFT JOIN matching_payload mp ON header.claimid = mp.claimid
    LEFT JOIN transactional_billing billing ON header.claimid = billing.claimid

    -- providers
    LEFT JOIN transactional_claimaffiliation rendering_claim ON header.claimid = rendering_claim.claimid 
    AND rendering_claim.type = 'Rendering'
    LEFT JOIN transactional_claimaffiliation referring_claim ON header.claimid = referring_claim.claimid 
    AND referring_claim.type = 'Referring'
    LEFT JOIN transactional_claimaffiliation servicelocation_claim ON header.claimid = servicelocation_claim.claimid 
    AND servicelocation_claim.type = 'ServiceLocation'
    LEFT JOIN transactional_claimaffiliation ambulancedropoff_claim ON header.claimid = ambulancedropoff_claim.claimid 
    AND ambulancedropoff_claim.type = 'AmbulanceDropOff'
    LEFT JOIN transactional_claimaffiliation supervising_claim ON header.claimid = supervising_claim.claimid 
    AND supervising_claim.type = 'Supervising'
    LEFT JOIN transactional_claimaffiliation operating_claim ON header.claimid = operating_claim.claimid 
    AND operating_claim.type = 'Operating'
    LEFT JOIN transactional_claimaffiliation purchased_claim ON header.claimid = purchased_claim.claimid 
    AND purchased_claim.type = 'Purchased'
    LEFT JOIN transactional_claimaffiliation other_claim ON header.claimid = other_claim.claimid 
    AND other_claim.type = 'Other'

    -- payer
    LEFT JOIN transactional_payer payer1 ON header.claimid = payer1.claimid
    AND payer1.sequencenumber = '1'
    LEFT JOIN transactional_payer payer2 ON header.claimid = payer2.claimid
    AND payer2.sequencenumber = '2'
    LEFT JOIN transactional_payer payer3 ON header.claimid = payer3.claimid
    AND payer3.sequencenumber = '3'

    -- diag
    LEFT JOIN transactional_diagnosis diagnosis ON diagnosis.claimid = header.claimid

    -- clean dates
    LEFT JOIN dates process_date ON header.ProcessDate = process_date.formatted
    LEFT JOIN dates start_date ON header.StartDate = start_date.formatted
    LEFT JOIN dates end_date ON header.EndDate = end_date.formatted
WHERE header.Type = 'Institutional'
;
