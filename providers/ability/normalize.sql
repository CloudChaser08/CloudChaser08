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
    header.ClaimId,                  -- claim_id
    COALESCE(mp.parentid, mp.hvid),  -- hvid
    1,                               -- source_version
    mp.gender,                       -- patient_gender
    mp.age,                          -- patient_age
    mp.yearOfBirth,                  -- patient_year_of_birth
    mp.threeDigitZip,                -- patient_zip3
    mp.state,                        -- patient_state
    CASE
    WHEN header.Type = 'Institutional'
    THEN 'I'
    WHEN header.Type = 'Professional'
    THEN 'P'
    END,                             -- claim_type
    header.ProcessDate,              -- date_received
    CASE 
    WHEN serviceline.ServiceStart IS NOT NULL 
    THEN serviceline.ServiceStart
    WHEN header.StartDate IS NOT NULL
    THEN header.StartDate
    ELSE (
    SELECT MIN(sl2.ServiceStart) 
    FROM transactional_serviceline sl2 
    WHERE sl2.ClaimId = serviceline.ClaimId
        )
    END,                             -- date_service
    CASE 
    WHEN serviceline.ServiceStart IS NOT NULL 
    THEN serviceline.ServiceEnd
    WHEN header.StartDate IS NOT NULL
    THEN header.EndDate
    ELSE (
    SELECT MIN(sl2.ServiceEnd) 
    FROM transactional_serviceline sl2 
    WHERE sl2.ClaimId = serviceline.ClaimId
        )
    END,                             -- date_service_end
    CASE 
    WHEN header.Type = 'Institutional'
    THEN header.AdmissionType
    END,                             -- inst_admit_type_std_id
    CASE 
    WHEN header.Type = 'Institutional'
    THEN header.AdmissionSource
    END,                             -- inst_admit_source_std_id
    CASE 
    WHEN header.Type = 'Institutional'
    THEN header.DischargeStatus
    END,                             -- inst_discharge_status_std_id
    CASE 
    WHEN header.Type <> 'Institutional'
    THEN NULL
    WHEN header.InstitutionalType IS NOT NULL
    THEN header.InstitutionalType || header.ClaimFrequencyCode
    END,                             -- inst_type_of_bill_std_id
    CASE 
    WHEN header.Type = 'Institutional'
    THEN header.DrgCode
    END,                             -- inst_drg_std_id
    CASE 
    WHEN header.Type <> 'Professional'
    THEN NULL
    WHEN ServiceLine.PlaceOfService IS NOT NULL 
    THEN ServiceLine.PlaceOfService
    ELSE header.InstitutionalType
    END,                             -- place_of_service_std_id
    serviceline.SequenceNumber,      -- service_line_number
    diagnosis.DiagnosisCode,         -- diagnosis_code
    CASE
    WHEN diagnosis.type LIKE 'A%'
    THEN '02'
    WHEN diagnosis.type LIKE 'B%'
    THEN '01'
    END,                             -- diagnosis_code_qual
    CASE 
    WHEN diagnosis.SequenceNumber = serviceline.diagnosiscodepointer1
    THEN '1'
    WHEN diagnosis.SequenceNumber = serviceline.diagnosiscodepointer2
    THEN '2'
    WHEN diagnosis.SequenceNumber = serviceline.diagnosiscodepointer3
    THEN '3'
    WHEN diagnosis.SequenceNumber = serviceline.diagnosiscodepointer4
    THEN '4'
    END,                             -- diagnosis_priority
    CASE
    WHEN header.type <> 'Institutional'
    THEN NULL
    WHEN diagnosis.diagnosiscode = header.admissiondiagnosis
    THEN 'Y'
    ELSE 'N'
    END,                             -- admit_diagnosis_ind
    CASE 
    WHEN header.Type = 'Professional'
    THEN serviceline.ProcedureCode 
    WHEN header.Type = 'Institutional'
    AND serviceline.procedurecode IS NOT NULL 
    THEN serviceline.procedurecode
    WHEN header.Type = 'Institutional'
    AND proc.procedurecode IS NOT NULL
    THEN proc.procedurecode
    END,                             -- procedure_code
    serviceline.qualifier,           -- procedure_code_qual
    CASE 
    WHEN header.type <> 'Institutional'
    THEN NULL
    WHEN proc.procedurecode = serviceline.procedurecode
    AND proc.sequencenumber = '1'
    THEN 'Y'
    ELSE 'N'
    END,                             -- principal_proc_ind
    serviceline.amount,              -- procedure_units
    serviceline.modifier1,           -- procedure_modifier_1
    serviceline.modifier2,           -- procedure_modifier_2
    serviceline.modifier3,           -- procedure_modifier_3
    serviceline.modifier4,           -- procedure_modifier_4
    serviceline.revenuecode,         -- revenue_code
    serviceline.drugcode,            -- ndc_code
    payer1.claimfileindicator,       -- medical_coverage_type
    serviceline.linecharge,          -- line_charge
    header.totalcharge,              -- total_charge
    CASE
    WHEN rendering.npi IS NOT NULL
    THEN rendering.npi
    WHEN supervising.npi IS NOT NULL
    THEN supervising.npi
    WHEN operating.npi IS NOT NULL
    THEN operating.npi
    WHEN purchased.npi IS NOT NULL
    THEN purchased.npi
    WHEN other.npi IS NOT NULL
    THEN other.npi
    ELSE NULL
    END,                             -- prov_rendering_npi
    billing.npi,                     -- prov_billing_npi
    referring.npi,                   -- prov_referring_npi
    CASE
    WHEN servicelocation.npi IS NOT NULL
    THEN servicelocation.npi
    WHEN ambulancedropoff.npi IS NOT NULL
    THEN ambulancedropoff.npi
    ELSE NULL
    END,                             -- prov_facility_npi
    payer1.sourcepayerid,            -- payer_vendor_id
    payer1.name,                     -- payer_name
    payer1.payerclassification,      -- payer_type
    CASE
    WHEN rendering.npi IS NOT NULL
    THEN rendering.lastname
    WHEN supervising.npi IS NOT NULL
    THEN supervising.lastname
    WHEN operating.npi IS NOT NULL
    THEN operating.lastname
    WHEN purchased.npi IS NOT NULL
    THEN purchased.lastname
    WHEN other.npi IS NOT NULL
    THEN other.lastname
    ELSE NULL
    END,                             -- prov_rendering_name_1
    CASE
    WHEN rendering.npi IS NOT NULL
    THEN rendering.firstname
    WHEN supervising.npi IS NOT NULL
    THEN supervising.firstname
    WHEN operating.npi IS NOT NULL
    THEN operating.firstname
    WHEN purchased.npi IS NOT NULL
    THEN purchased.firstname
    WHEN other.npi IS NOT NULL
    THEN other.firstname
    ELSE NULL
    END,                             -- prov_rendering_name_2
    CASE
    WHEN rendering.npi IS NOT NULL
    THEN rendering.addr1
    WHEN supervising.npi IS NOT NULL
    THEN supervising.addr1
    WHEN operating.npi IS NOT NULL
    THEN operating.addr1
    WHEN purchased.npi IS NOT NULL
    THEN purchased.addr1
    WHEN other.npi IS NOT NULL
    THEN other.addr1
    ELSE NULL
    END,                             -- prov_rendering_address_1
    CASE
    WHEN rendering.npi IS NOT NULL
    THEN rendering.addr2
    WHEN supervising.npi IS NOT NULL
    THEN supervising.addr2
    WHEN operating.npi IS NOT NULL
    THEN operating.addr2
    WHEN purchased.npi IS NOT NULL
    THEN purchased.addr2
    WHEN other.npi IS NOT NULL
    THEN other.addr2
    ELSE NULL
    END,                             -- prov_rendering_address_2
    CASE
    WHEN rendering.npi IS NOT NULL
    THEN rendering.city
    WHEN supervising.npi IS NOT NULL
    THEN supervising.city
    WHEN operating.npi IS NOT NULL
    THEN operating.city
    WHEN purchased.npi IS NOT NULL
    THEN purchased.city
    WHEN other.npi IS NOT NULL
    THEN other.city
    ELSE NULL
    END,                             -- prov_rendering_city
    CASE
    WHEN rendering.npi IS NOT NULL
    THEN rendering.state
    WHEN supervising.npi IS NOT NULL
    THEN supervising.state
    WHEN operating.npi IS NOT NULL
    THEN operating.state
    WHEN purchased.npi IS NOT NULL
    THEN purchased.state
    WHEN other.npi IS NOT NULL
    THEN other.state
    ELSE NULL
    END,                             -- prov_rendering_state
    CASE
    WHEN rendering.npi IS NOT NULL
    THEN rendering.zip
    WHEN supervising.npi IS NOT NULL
    THEN supervising.zip
    WHEN operating.npi IS NOT NULL
    THEN operating.zip
    WHEN purchased.npi IS NOT NULL
    THEN purchased.zip
    WHEN other.npi IS NOT NULL
    THEN other.zip
    ELSE NULL
    END,                             -- prov_rendering_zip
    CASE
    WHEN rendering.npi IS NOT NULL
    THEN rendering.taxonomy
    WHEN supervising.npi IS NOT NULL
    THEN supervising.taxonomy
    WHEN operating.npi IS NOT NULL
    THEN operating.taxonomy
    WHEN purchased.npi IS NOT NULL
    THEN purchased.taxonomy
    WHEN other.npi IS NOT NULL
    THEN other.taxonomy
    ELSE NULL
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
    referring.lastname,              -- prov_referring_name_1
    referring.firstname,             -- prov_referring_name_2
    referring.addr1,                 -- prov_referring_address_1
    referring.addr2,                 -- prov_referring_address_2
    referring.city,                  -- prov_referring_city
    referring.state,                 -- prov_referring_state
    referring.zip,                   -- prov_referring_zip
    referring.taxonomy,              -- prov_referring_std_taxonomy
    CASE
    WHEN servicelocation.npi IS NOT NULL
    THEN servicelocation.lastname
    WHEN ambulancedropoff.npi IS NOT NULL
    THEN ambulancedropoff.lastname
    ELSE NULL
    END,                             -- prov_facility_name_1
    CASE
    WHEN servicelocation.npi IS NOT NULL
    THEN servicelocation.firstname
    WHEN ambulancedropoff.npi IS NOT NULL
    THEN ambulancedropoff.firstname
    ELSE NULL
    END,                             -- prov_facility_name_2
    CASE
    WHEN servicelocation.npi <> 'NULL'
    AND servicelocation.npi IS NOT NULL
    THEN servicelocation.addr1
    WHEN ambulancedropoff.npi <> 'NULL'
    AND ambulancedropoff.npi IS NOT NULL
    THEN ambulancedropoff.addr1
    ELSE NULL
    END,                             -- prov_facility_address_1
    CASE
    WHEN servicelocation.npi <> 'NULL'
    AND servicelocation.npi IS NOT NULL
    THEN servicelocation.addr2
    WHEN ambulancedropoff.npi <> 'NULL'
    AND ambulancedropoff.npi IS NOT NULL
    THEN ambulancedropoff.addr2
    ELSE NULL
    END,                             -- prov_facility_address_2
    CASE
    WHEN servicelocation.npi <> 'NULL'
    AND servicelocation.npi IS NOT NULL
    THEN servicelocation.city
    WHEN ambulancedropoff.npi <> 'NULL'
    AND ambulancedropoff.npi IS NOT NULL
    THEN ambulancedropoff.city
    ELSE NULL
    END,                             -- prov_facility_city
    CASE
    WHEN servicelocation.npi <> 'NULL'
    AND servicelocation.npi IS NOT NULL
    THEN servicelocation.state
    WHEN ambulancedropoff.npi <> 'NULL'
    AND ambulancedropoff.npi IS NOT NULL
    THEN ambulancedropoff.state
    ELSE NULL
    END,                             -- prov_facility_state
    CASE
    WHEN servicelocation.npi <> 'NULL'
    AND servicelocation.npi IS NOT NULL
    THEN servicelocation.zip
    WHEN ambulancedropoff.npi <> 'NULL'
    AND ambulancedropoff.npi IS NOT NULL
    THEN ambulancedropoff.zip
    ELSE NULL
    END,                             -- prov_facility_zip
    CASE
    WHEN servicelocation.npi <> 'NULL'
    AND servicelocation.npi IS NOT NULL
    THEN servicelocation.taxonomy
    WHEN ambulancedropoff.npi <> 'NULL'
    AND ambulancedropoff.npi IS NOT NULL
    THEN ambulancedropoff.taxonomy
    ELSE NULL
    END,                             -- prov_facility_std_taxonomy
    CASE
    WHEN payer2.sequencenumber = 'NULL'
    THEN NULL
    ELSE payer2.sequencenumber
    END,                             -- cob_payer_seq_code_1
    CASE
    WHEN payer2.payerid = 'NULL'
    THEN NULL
    ELSE payer2.payerid
    END,                             -- cob_payer_hpid_1
    CASE
    WHEN payer2.claimfileindicator = 'NULL'
    THEN NULL
    ELSE payer2.claimfileindicator
    END,                             -- cob_payer_claim_filing_ind_code_1
    CASE
    WHEN payer2.payerclassification = 'NULL'
    THEN NULL
    ELSE payer2.payerclassification
    END,                             -- cob_ins_type_code_1
    CASE
    WHEN payer3.sequencenumber = 'NULL'
    THEN NULL
    ELSE payer3.sequencenumber
    END,                             -- cob_payer_seq_code_2
    CASE
    WHEN payer3.payerid = 'NULL'
    THEN NULL
    ELSE payer3.payerid
    END,                             -- cob_payer_hpid_2
    CASE
    WHEN payer3.claimfileindicator = 'NULL'
    THEN NULL
    ELSE payer3.claimfileindicator
    END,                             -- cob_payer_claim_filing_ind_code_2
    CASE
    WHEN payer3.payerclassification = 'NULL'
    THEN payer3.payerclassification
    END                              -- cob_ins_type_code_
FROM transactional_header header 
    LEFT JOIN matching_payload mp ON header.claimid = mp.claimid
    LEFT JOIN transactional_serviceline serviceline ON header.claimid = serviceline.claimid
    LEFT JOIN transactional_billing billing ON header.claimid = billing.claimid

    -- providers
    LEFT JOIN transactional_servicelineaffiliation rendering ON serviceline.servicelineid = rendering.servicelineid 
    AND serviceline.claimid = rendering.claimid
    AND rendering.type = 'Rendering'
    LEFT JOIN transactional_servicelineaffiliation referring ON serviceline.servicelineid = referring.servicelineid 
    AND serviceline.claimid = referring.claimid
    AND referring.type = 'Referring'
    LEFT JOIN transactional_servicelineaffiliation servicelocation ON serviceline.servicelineid = servicelocation.servicelineid 
    AND serviceline.claimid = servicelocation.claimid
    AND servicelocation.type = 'ServiceLocation'
    LEFT JOIN transactional_servicelineaffiliation ambulancedropoff ON serviceline.servicelineid = ambulancedropoff.servicelineid 
    AND serviceline.claimid = ambulancedropoff.claimid
    AND ambulancedropoff.type = 'AmbulanceDropOff'
    LEFT JOIN transactional_servicelineaffiliation supervising ON serviceline.servicelineid = supervising.servicelineid 
    AND serviceline.claimid = supervising.claimid
    AND supervising.type = 'Supervising'
    LEFT JOIN transactional_servicelineaffiliation operating ON serviceline.servicelineid = operating.servicelineid 
    AND serviceline.claimid = operating.claimid
    AND operating.type = 'Operating'
    LEFT JOIN transactional_servicelineaffiliation purchased ON serviceline.servicelineid = purchased.servicelineid 
    AND serviceline.claimid = purchased.claimid
    AND purchased.type = 'Purchased'
    LEFT JOIN transactional_servicelineaffiliation other ON serviceline.servicelineid = other.servicelineid 
    AND serviceline.claimid = other.claimid
    AND other.type = 'Other'

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

    -- fix dates
    LEFT JOIN dates svc_
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
    CASE
    WHEN header.ClaimId = 'NULL'
    THEN NULL
    ELSE header.ClaimId
    END,                             -- claim_id
    COALESCE(mp.parentid, mp.hvid),  -- hvid
    1,                               -- source_version
    mp.gender,                       -- patient_gender
    mp.age,                          -- patient_age
    mp.yearOfBirth,                  -- patient_year_of_birth
    mp.threeDigitZip,                -- patient_zip3
    mp.state,                        -- patient_state
    'P',                             -- claim_type
    CASE
    WHEN header.ProcessDate = 'NULL'
    THEN NULL
    ELSE header.ProcessDate
    END,                             -- date_received
    CASE 
    WHEN header.StartDate IS NOT NULL
    AND header.StartDate <> 'NULL'
    THEN header.StartDate
    ELSE (
    SELECT MIN(sl2.ServiceStart) 
    FROM transactional_serviceline sl2 
    WHERE sl2.ClaimId = header.ClaimId
        )
    END,                             -- date_service
    CASE 
    WHEN header.StartDate IS NOT NULL
    AND header.StartDate <> 'NULL'
    CASE 
    WHEN header.EndDate = 'NULL'
    THEN NULL
    ELSE header.EndDate
    END
    ELSE (
    SELECT MIN(sl2.ServiceEnd) 
    FROM transactional_serviceline sl2 
    WHERE sl2.ClaimId = header.ClaimId
        AND sl2.ServiceEnd <> 'NULL'
        )
    END,                             -- date_service_end
    CASE
    WHEN header.InstitutionalType = 'NULL'
    THEN NULL
    ELSE header.InstitutionalType
    END,                             -- place_of_service_std_id
    CASE
    WHEN diagnosis.DiagnosisCode = 'NULL'
    THEN NULL
    ELSE diagnosis.DiagnosisCode
    END,                             -- diagnosis_code
    CASE
    WHEN diagnosis.type = 'NULL'
    THEN NULL
    WHEN diagnosis.type LIKE 'A%'
    THEN '02'
    WHEN diagnosis.type LIKE 'B%'
    THEN '01'
    END,                             -- diagnosis_code_qual
    CASE
    WHEN payer1.claimfileindicator = 'NULL'
    THEN NULL
    ELSE payer1.claimfileindicator
    END,                             -- medical_coverage_type
    CASE
    WHEN header.totalcharge = 'NULL'
    THEN NULL
    ELSE header.totalcharge
    END,                             -- total_charge
    CASE
    WHEN rendering.npi <> 'NULL'
    AND rendering.npi IS NOT NULL
    THEN rendering.npi
    WHEN supervising.npi <> 'NULL'
    AND supervising.npi IS NOT NULL
    THEN supervising.npi
    WHEN operating.npi <> 'NULL'
    AND operating.npi IS NOT NULL
    THEN operating.npi
    WHEN purchased.npi <> 'NULL'
    AND purchased.npi IS NOT NULL
    THEN purchased.npi
    WHEN other.npi <> 'NULL'
    AND other.npi IS NOT NULL
    THEN other.npi
    ELSE NULL
    END,                             -- prov_rendering_npi
    CASE
    WHEN billing.npi = 'NULL'
    THEN NULL
    ELSE billing.npi
    END,                             -- prov_billing_npi
    CASE
    WHEN referring.npi = 'NULL'
    THEN NULL
    ELSE referring.npi
    END,                             -- prov_referring_npi
    CASE
    WHEN servicelocation.npi <> 'NULL'
    AND servicelocation.npi IS NOT NULL
    THEN servicelocation.npi
    WHEN ambulancedropoff.npi <> 'NULL'
    AND ambulancedropoff.npi IS NOT NULL
    THEN ambulancedropoff.npi
    ELSE NULL
    END,                             -- prov_facility_npi
    CASE
    WHEN payer1.sourcepayerid = 'NULL'
    THEN NULL
    ELSE payer1.sourcepayerid
    END,                             -- payer_vendor_id
    CASE
    WHEN payer1.name = 'NULL'
    THEN NULL
    ELSE payer1.name
    END,                             -- payer_name
    CASE
    WHEN payer1.payerclassification = 'NULL'
    THEN NULL
    ELSE payer1.payerclassification
    END,                             -- payer_type
    CASE
    WHEN rendering.npi <> 'NULL'
    AND rendering.npi IS NOT NULL
    THEN rendering.lastname
    WHEN supervising.npi <> 'NULL'
    AND supervising.npi IS NOT NULL
    THEN supervising.lastname
    WHEN operating.npi <> 'NULL'
    AND operating.npi IS NOT NULL
    THEN operating.lastname
    WHEN purchased.npi <> 'NULL'
    AND purchased.npi IS NOT NULL
    THEN purchased.lastname
    WHEN other.npi <> 'NULL'
    AND other.npi IS NOT NULL
    THEN other.lastname
    ELSE NULL
    END,                             -- prov_rendering_name_1
    CASE
    WHEN rendering.npi <> 'NULL'
    AND rendering.npi IS NOT NULL
    THEN rendering.firstname
    WHEN supervising.npi <> 'NULL'
    AND supervising.npi IS NOT NULL
    THEN supervising.firstname
    WHEN operating.npi <> 'NULL'
    AND operating.npi IS NOT NULL
    THEN operating.firstname
    WHEN purchased.npi <> 'NULL'
    AND purchased.npi IS NOT NULL
    THEN purchased.firstname
    WHEN other.npi <> 'NULL'
    AND other.npi IS NOT NULL
    THEN other.firstname
    ELSE NULL
    END,                             -- prov_rendering_name_2
    CASE
    WHEN rendering.npi <> 'NULL'
    AND rendering.npi IS NOT NULL
    THEN rendering.addr1
    WHEN supervising.npi <> 'NULL'
    AND supervising.npi IS NOT NULL
    THEN supervising.addr1
    WHEN operating.npi <> 'NULL'
    AND operating.npi IS NOT NULL
    THEN operating.addr1
    WHEN purchased.npi <> 'NULL'
    AND purchased.npi IS NOT NULL
    THEN purchased.addr1
    WHEN other.npi <> 'NULL'
    AND other.npi IS NOT NULL
    THEN other.addr1
    ELSE NULL
    END,                             -- prov_rendering_address_1
    CASE
    WHEN rendering.npi <> 'NULL'
    AND rendering.npi IS NOT NULL
    THEN rendering.addr2
    WHEN supervising.npi <> 'NULL'
    AND supervising.npi IS NOT NULL
    THEN supervising.addr2
    WHEN operating.npi <> 'NULL'
    AND operating.npi IS NOT NULL
    THEN operating.addr2
    WHEN purchased.npi <> 'NULL'
    AND purchased.npi IS NOT NULL
    THEN purchased.addr2
    WHEN other.npi <> 'NULL'
    AND other.npi IS NOT NULL
    THEN other.addr2
    ELSE NULL
    END,                             -- prov_rendering_address_2
    CASE
    WHEN rendering.npi <> 'NULL'
    AND rendering.npi IS NOT NULL
    THEN rendering.city
    WHEN supervising.npi <> 'NULL'
    AND supervising.npi IS NOT NULL
    THEN supervising.city
    WHEN operating.npi <> 'NULL'
    AND operating.npi IS NOT NULL
    THEN operating.city
    WHEN purchased.npi <> 'NULL'
    AND purchased.npi IS NOT NULL
    THEN purchased.city
    WHEN other.npi <> 'NULL'
    AND other.npi IS NOT NULL
    THEN other.city
    ELSE NULL
    END,                             -- prov_rendering_city
    CASE
    WHEN rendering.npi <> 'NULL'
    AND rendering.npi IS NOT NULL
    THEN rendering.state
    WHEN supervising.npi <> 'NULL'
    AND supervising.npi IS NOT NULL
    THEN supervising.state
    WHEN operating.npi <> 'NULL'
    AND operating.npi IS NOT NULL
    THEN operating.state
    WHEN purchased.npi <> 'NULL'
    AND purchased.npi IS NOT NULL
    THEN purchased.state
    WHEN other.npi <> 'NULL'
    AND other.npi IS NOT NULL
    THEN other.state
    ELSE NULL
    END,                             -- prov_rendering_state
    CASE
    WHEN rendering.npi <> 'NULL'
    AND rendering.npi IS NOT NULL
    THEN rendering.zip
    WHEN supervising.npi <> 'NULL'
    AND supervising.npi IS NOT NULL
    THEN supervising.zip
    WHEN operating.npi <> 'NULL'
    AND operating.npi IS NOT NULL
    THEN operating.zip
    WHEN purchased.npi <> 'NULL'
    AND purchased.npi IS NOT NULL
    THEN purchased.zip
    WHEN other.npi <> 'NULL'
    AND other.npi IS NOT NULL
    THEN other.zip
    ELSE NULL
    END,                             -- prov_rendering_zip
    CASE
    WHEN rendering.npi <> 'NULL'
    AND rendering.npi IS NOT NULL
    THEN rendering.taxonomy
    WHEN supervising.npi <> 'NULL'
    AND supervising.npi IS NOT NULL
    THEN supervising.taxonomy
    WHEN operating.npi <> 'NULL'
    AND operating.npi IS NOT NULL
    THEN operating.taxonomy
    WHEN purchased.npi <> 'NULL'
    AND purchased.npi IS NOT NULL
    THEN purchased.taxonomy
    WHEN other.npi <> 'NULL'
    AND other.npi IS NOT NULL
    THEN other.taxonomy
    ELSE NULL
    END,                             -- prov_rendering_std_taxonomy
    CASE
    WHEN billing.taxid = 'NULL'
    THEN NULL
    ELSE billing.taxid
    END,                             -- prov_billing_tax_id
    CASE
    WHEN billing.ssn = 'NULL'
    THEN NULL
    ELSE billing.ssn
    END,                             -- prov_billing_ssn
    CASE
    WHEN billing.stlic = 'NULL'
    THEN NULL
    ELSE billing.stlic
    END,                             -- prov_billing_state_license
    CASE
    WHEN billing.upin = 'NULL'
    THEN NULL
    ELSE billing.upin
    END,                             -- prov_billing_upin
    CASE
    WHEN billing.lastname = 'NULL'
    THEN NULL
    ELSE billing.lastname
    END,                             -- prov_billing_name_1
    CASE
    WHEN billing.firstname = 'NULL'
    THEN NULL
    ELSE billing.firstname
    END,                             -- prov_billing_name_2
    CASE
    WHEN billing.addr1 = 'NULL'
    THEN NULL
    ELSE billing.addr1
    END,                             -- prov_billing_address_1
    CASE
    WHEN billing.addr2 = 'NULL'
    THEN NULL
    ELSE billing.addr2
    END,                             -- prov_billing_address_2
    CASE
    WHEN billing.city = 'NULL'
    THEN NULL
    ELSE billing.city
    END,                             -- prov_billing_city
    CASE
    WHEN billing.state = 'NULL'
    THEN NULL
    ELSE billing.state
    END,                             -- prov_billing_state
    CASE
    WHEN billing.zip = 'NULL'
    THEN NULL
    ELSE billing.zip
    END,                             -- prov_billing_zip
    CASE
    WHEN billing.taxonomy = 'NULL'
    THEN NULL
    ELSE billing.taxonomy
    END,                             -- prov_billing_std_taxonomy
    CASE
    WHEN referring.lastname = 'NULL'
    THEN NULL
    ELSE referring.lastname
    END,                             -- prov_referring_name_1
    CASE
    WHEN referring.firstname = 'NULL'
    THEN NULL
    ELSE referring.firstname
    END,                             -- prov_referring_name_2
    CASE
    WHEN referring.addr1 = 'NULL'
    THEN NULL
    ELSE referring.addr1
    END,                             -- prov_referring_address_1
    CASE
    WHEN referring.addr2 = 'NULL'
    THEN NULL
    ELSE referring.addr2
    END,                             -- prov_referring_address_2
    CASE
    WHEN referring.city = 'NULL'
    THEN NULL
    ELSE referring.city
    END,                             -- prov_referring_city
    CASE
    WHEN referring.state = 'NULL'
    THEN NULL
    ELSE referring.state
    END,                             -- prov_referring_state
    CASE
    WHEN referring.zip = 'NULL'
    THEN NULL
    ELSE referring.zip
    END,                             -- prov_referring_zip
    CASE
    WHEN referring.taxonomy = 'NULL'
    THEN NULL
    ELSE referring.taxonomy
    END,                             -- prov_referring_std_taxonomy
    CASE
    WHEN servicelocation.npi <> 'NULL'
    AND servicelocation.npi IS NOT NULL
    THEN servicelocation.lastname
    WHEN ambulancedropoff.npi <> 'NULL'
    AND ambulancedropoff.npi IS NOT NULL
    THEN ambulancedropoff.lastname
    ELSE NULL
    END,                             -- prov_facility_name_1
    CASE
    WHEN servicelocation.npi <> 'NULL'
    AND servicelocation.npi IS NOT NULL
    THEN servicelocation.firstname
    WHEN ambulancedropoff.npi <> 'NULL'
    AND ambulancedropoff.npi IS NOT NULL
    THEN ambulancedropoff.firstname
    ELSE NULL
    END,                             -- prov_facility_name_2
    CASE
    WHEN servicelocation.npi <> 'NULL'
    AND servicelocation.npi IS NOT NULL
    THEN servicelocation.addr1
    WHEN ambulancedropoff.npi <> 'NULL'
    AND ambulancedropoff.npi IS NOT NULL
    THEN ambulancedropoff.addr1
    ELSE NULL
    END,                             -- prov_facility_address_1
    CASE
    WHEN servicelocation.npi <> 'NULL'
    AND servicelocation.npi IS NOT NULL
    THEN servicelocation.addr2
    WHEN ambulancedropoff.npi <> 'NULL'
    AND ambulancedropoff.npi IS NOT NULL
    THEN ambulancedropoff.addr2
    ELSE NULL
    END,                             -- prov_facility_address_2
    CASE
    WHEN servicelocation.npi <> 'NULL'
    AND servicelocation.npi IS NOT NULL
    THEN servicelocation.city
    WHEN ambulancedropoff.npi <> 'NULL'
    AND ambulancedropoff.npi IS NOT NULL
    THEN ambulancedropoff.city
    ELSE NULL
    END,                             -- prov_facility_city
    CASE
    WHEN servicelocation.npi <> 'NULL'
    AND servicelocation.npi IS NOT NULL
    THEN servicelocation.state
    WHEN ambulancedropoff.npi <> 'NULL'
    AND ambulancedropoff.npi IS NOT NULL
    THEN ambulancedropoff.state
    ELSE NULL
    END,                             -- prov_facility_state
    CASE
    WHEN servicelocation.npi <> 'NULL'
    AND servicelocation.npi IS NOT NULL
    THEN servicelocation.zip
    WHEN ambulancedropoff.npi <> 'NULL'
    AND ambulancedropoff.npi IS NOT NULL
    THEN ambulancedropoff.zip
    ELSE NULL
    END,                             -- prov_facility_zip
    CASE
    WHEN servicelocation.npi <> 'NULL'
    AND servicelocation.npi IS NOT NULL
    THEN servicelocation.taxonomy
    WHEN ambulancedropoff.npi <> 'NULL'
    AND ambulancedropoff.npi IS NOT NULL
    THEN ambulancedropoff.taxonomy
    ELSE NULL
    END,                             -- prov_facility_std_taxonomy
    CASE
    WHEN payer2.sequencenumber = 'NULL'
    THEN NULL
    ELSE payer2.sequencenumber
    END,                             -- cob_payer_seq_code_1
    CASE
    WHEN payer2.payerid = 'NULL'
    THEN NULL
    ELSE payer2.payerid
    END,                             -- cob_payer_hpid_1
    CASE
    WHEN payer2.claimfileindicator = 'NULL'
    THEN NULL
    ELSE payer2.claimfileindicator
    END,                             -- cob_payer_claim_filing_ind_code_1
    CASE
    WHEN payer2.payerclassification = 'NULL'
    THEN NULL
    ELSE payer2.payerclassification
    END,                             -- cob_ins_type_code_1
    CASE
    WHEN payer3.sequencenumber = 'NULL'
    THEN NULL
    ELSE payer3.sequencenumber
    END,                             -- cob_payer_seq_code_2
    CASE
    WHEN payer3.payerid = 'NULL'
    THEN NULL
    ELSE payer3.payerid
    END,                             -- cob_payer_hpid_2
    CASE
    WHEN payer3.claimfileindicator = 'NULL'
    THEN NULL
    ELSE payer3.claimfileindicator
    END,                             -- cob_payer_claim_filing_ind_code_2
    CASE
    WHEN payer3.payerclassification = 'NULL'
    THEN NULL
    ELSE payer3.payerclassification
    END                              -- cob_ins_type_code_2
FROM transactional_header header 
    LEFT JOIN matching_payload mp ON header.claimid = mp.claimid
    LEFT JOIN transactional_billing billing ON header.claimid = billing.claimid
    LEFT JOIN transactional_servicelineaffiliation rendering ON header.claimid = rendering.claimid
    AND rendering.type = 'Rendering'
    LEFT JOIN transactional_servicelineaffiliation referring ON header.claimid = referring.claimid
    AND referring.type = 'Referring'
    LEFT JOIN transactional_servicelineaffiliation servicelocation ON header.claimid = servicelocation.claimid
    AND servicelocation.type = 'ServiceLocation'
    LEFT JOIN transactional_servicelineaffiliation ambulancedropoff ON header.claimid = ambulancedropoff.claimid
    AND ambulancedropoff.type = 'AmbulanceDropOff'
    LEFT JOIN transactional_servicelineaffiliation supervising ON header.claimid = supervising.claimid
    AND supervising.type = 'Supervising'
    LEFT JOIN transactional_servicelineaffiliation operating ON header.claimid = operating.claimid
    AND operating.type = 'Operating'
    LEFT JOIN transactional_servicelineaffiliation purchased ON header.claimid = purchased.claimid
    AND purchased.type = 'Purchased'
    LEFT JOIN transactional_servicelineaffiliation other ON header.claimid = other.claimid
    AND other.type = 'Other'
    LEFT JOIN transactional_payer payer1 ON header.claimid = payer1.claimid
    AND payer1.sequencenumber = '1'
    LEFT JOIN transactional_payer payer2 ON header.claimid = payer2.claimid
    AND payer2.sequencenumber = '2'
    LEFT JOIN transactional_payer payer3 ON header.claimid = payer3.claimid
    AND payer3.sequencenumber = '3'
    LEFT JOIN transactional_diagnosis diagnosis ON diagnosis.claimid = header.claimid

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
    CASE
    WHEN header.ClaimId = 'NULL'
    THEN NULL
    ELSE header.ClaimId
    END,                             -- claim_id
    COALESCE(mp.parentid, mp.hvid),  -- hvid
    1,                               -- source_version
    mp.gender,                       -- patient_gender
    mp.age,                          -- patient_age
    mp.yearOfBirth,                  -- patient_year_of_birth
    mp.threeDigitZip,                -- patient_zip3
    mp.state,                        -- patient_state
    'I',                             -- claim_type
    CASE
    WHEN header.ProcessDate = 'NULL'
    THEN NULL
    ELSE header.ProcessDate
    END,                             -- date_received
    CASE 
    WHEN header.StartDate IS NOT NULL
    AND header.StartDate <> 'NULL'
    THEN header.StartDate
    ELSE (
    SELECT MIN(sl2.ServiceStart) 
    FROM transactional_serviceline sl2 
    WHERE sl2.ClaimId = header.ClaimId
        )
    END,                             -- date_service
    CASE 
    WHEN header.StartDate IS NOT NULL
    AND header.StartDate <> 'NULL'
    CASE 
    WHEN header.EndDate = 'NULL'
    THEN NULL
    ELSE header.EndDate
    END
    ELSE (
    SELECT MIN(sl2.ServiceEnd) 
    FROM transactional_serviceline sl2 
    WHERE sl2.ClaimId = header.ClaimId
        AND sl2.ServiceEnd <> 'NULL'
        )
    END,                             -- date_service_end
    CASE
    WHEN header.AdmissionType = 'NULL'
    THEN NULL
    ELSE header.AdmissionType
    END,                             -- inst_admit_type_std_id
    CASE
    WHEN header.AdmissionSource = 'NULL'
    THEN NULL
    ELSE header.AdmissionSource
    END,                             -- inst_admit_source_std_id
    CASE
    WHEN header.DischargeStatus = 'NULL'
    THEN NULL
    ELSE header.DischargeStatus
    END,                             -- inst_discharge_status_std_id
    CASE 
    WHEN header.InstitutionalType IS NOT NULL
    AND header.InstitutionalType <> 'NULL'
    THEN header.InstitutionalType || header.ClaimFrequencyCode
    END,                             -- inst_type_of_bill_std_id
    CASE
    WHEN header.DrgCode = 'NULL'
    THEN NULL
    ELSE header.DrgCode
    END,                             -- inst_drg_std_id
    CASE
    WHEN diagnosis.DiagnosisCode = 'NULL'
    THEN NULL
    ELSE diagnosis.DiagnosisCode
    END,                             -- diagnosis_code
    CASE
    WHEN diagnosis.type = 'NULL'
    THEN NULL
    WHEN diagnosis.type LIKE 'A%'
    THEN '02'
    WHEN diagnosis.type LIKE 'B%'
    THEN '01'
    END,                             -- diagnosis_code_qual
    CASE
    WHEN payer1.claimfileindicator = 'NULL'
    THEN NULL
    ELSE payer1.claimfileindicator
    END,                             -- medical_coverage_type
    CASE
    WHEN header.totalcharge = 'NULL'
    THEN NULL
    ELSE header.totalcharge
    END,                             -- total_charge
    CASE
    WHEN rendering.npi <> 'NULL'
    AND rendering.npi IS NOT NULL
    THEN rendering.npi
    WHEN supervising.npi <> 'NULL'
    AND supervising.npi IS NOT NULL
    THEN supervising.npi
    WHEN operating.npi <> 'NULL'
    AND operating.npi IS NOT NULL
    THEN operating.npi
    WHEN purchased.npi <> 'NULL'
    AND purchased.npi IS NOT NULL
    THEN purchased.npi
    WHEN other.npi <> 'NULL'
    AND other.npi IS NOT NULL
    THEN other.npi
    ELSE NULL
    END,                             -- prov_rendering_npi
    CASE
    WHEN billing.npi = 'NULL'
    THEN NULL
    ELSE billing.npi
    END,                             -- prov_billing_npi
    CASE
    WHEN referring.npi = 'NULL'
    THEN NULL
    ELSE referring.npi
    END,                             -- prov_referring_npi
    CASE
    WHEN servicelocation.npi <> 'NULL'
    AND servicelocation.npi IS NOT NULL
    THEN servicelocation.npi
    WHEN ambulancedropoff.npi <> 'NULL'
    AND ambulancedropoff.npi IS NOT NULL
    THEN ambulancedropoff.npi
    ELSE NULL
    END,                             -- prov_facility_npi
    CASE
    WHEN payer1.sourcepayerid = 'NULL'
    THEN NULL
    ELSE payer1.sourcepayerid
    END,                             -- payer_vendor_id
    CASE
    WHEN payer1.name = 'NULL'
    THEN NULL
    ELSE payer1.name
    END,                             -- payer_name
    CASE
    WHEN payer1.payerclassification = 'NULL'
    THEN NULL
    ELSE payer1.payerclassification
    END,                             -- payer_type
    CASE
    WHEN rendering.npi <> 'NULL'
    AND rendering.npi IS NOT NULL
    THEN rendering.lastname
    WHEN supervising.npi <> 'NULL'
    AND supervising.npi IS NOT NULL
    THEN supervising.lastname
    WHEN operating.npi <> 'NULL'
    AND operating.npi IS NOT NULL
    THEN operating.lastname
    WHEN purchased.npi <> 'NULL'
    AND purchased.npi IS NOT NULL
    THEN purchased.lastname
    WHEN other.npi <> 'NULL'
    AND other.npi IS NOT NULL
    THEN other.lastname
    ELSE NULL
    END,                             -- prov_rendering_name_1
    CASE
    WHEN rendering.npi <> 'NULL'
    AND rendering.npi IS NOT NULL
    THEN rendering.firstname
    WHEN supervising.npi <> 'NULL'
    AND supervising.npi IS NOT NULL
    THEN supervising.firstname
    WHEN operating.npi <> 'NULL'
    AND operating.npi IS NOT NULL
    THEN operating.firstname
    WHEN purchased.npi <> 'NULL'
    AND purchased.npi IS NOT NULL
    THEN purchased.firstname
    WHEN other.npi <> 'NULL'
    AND other.npi IS NOT NULL
    THEN other.firstname
    ELSE NULL
    END,                             -- prov_rendering_name_2
    CASE
    WHEN rendering.npi <> 'NULL'
    AND rendering.npi IS NOT NULL
    THEN rendering.addr1
    WHEN supervising.npi <> 'NULL'
    AND supervising.npi IS NOT NULL
    THEN supervising.addr1
    WHEN operating.npi <> 'NULL'
    AND operating.npi IS NOT NULL
    THEN operating.addr1
    WHEN purchased.npi <> 'NULL'
    AND purchased.npi IS NOT NULL
    THEN purchased.addr1
    WHEN other.npi <> 'NULL'
    AND other.npi IS NOT NULL
    THEN other.addr1
    ELSE NULL
    END,                             -- prov_rendering_address_1
    CASE
    WHEN rendering.npi <> 'NULL'
    AND rendering.npi IS NOT NULL
    THEN rendering.addr2
    WHEN supervising.npi <> 'NULL'
    AND supervising.npi IS NOT NULL
    THEN supervising.addr2
    WHEN operating.npi <> 'NULL'
    AND operating.npi IS NOT NULL
    THEN operating.addr2
    WHEN purchased.npi <> 'NULL'
    AND purchased.npi IS NOT NULL
    THEN purchased.addr2
    WHEN other.npi <> 'NULL'
    AND other.npi IS NOT NULL
    THEN other.addr2
    ELSE NULL
    END,                             -- prov_rendering_address_2
    CASE
    WHEN rendering.npi <> 'NULL'
    AND rendering.npi IS NOT NULL
    THEN rendering.city
    WHEN supervising.npi <> 'NULL'
    AND supervising.npi IS NOT NULL
    THEN supervising.city
    WHEN operating.npi <> 'NULL'
    AND operating.npi IS NOT NULL
    THEN operating.city
    WHEN purchased.npi <> 'NULL'
    AND purchased.npi IS NOT NULL
    THEN purchased.city
    WHEN other.npi <> 'NULL'
    AND other.npi IS NOT NULL
    THEN other.city
    ELSE NULL
    END,                             -- prov_rendering_city
    CASE
    WHEN rendering.npi <> 'NULL'
    AND rendering.npi IS NOT NULL
    THEN rendering.state
    WHEN supervising.npi <> 'NULL'
    AND supervising.npi IS NOT NULL
    THEN supervising.state
    WHEN operating.npi <> 'NULL'
    AND operating.npi IS NOT NULL
    THEN operating.state
    WHEN purchased.npi <> 'NULL'
    AND purchased.npi IS NOT NULL
    THEN purchased.state
    WHEN other.npi <> 'NULL'
    AND other.npi IS NOT NULL
    THEN other.state
    ELSE NULL
    END,                             -- prov_rendering_state
    CASE
    WHEN rendering.npi <> 'NULL'
    AND rendering.npi IS NOT NULL
    THEN rendering.zip
    WHEN supervising.npi <> 'NULL'
    AND supervising.npi IS NOT NULL
    THEN supervising.zip
    WHEN operating.npi <> 'NULL'
    AND operating.npi IS NOT NULL
    THEN operating.zip
    WHEN purchased.npi <> 'NULL'
    AND purchased.npi IS NOT NULL
    THEN purchased.zip
    WHEN other.npi <> 'NULL'
    AND other.npi IS NOT NULL
    THEN other.zip
    ELSE NULL
    END,                             -- prov_rendering_zip
    CASE
    WHEN rendering.npi <> 'NULL'
    AND rendering.npi IS NOT NULL
    THEN rendering.taxonomy
    WHEN supervising.npi <> 'NULL'
    AND supervising.npi IS NOT NULL
    THEN supervising.taxonomy
    WHEN operating.npi <> 'NULL'
    AND operating.npi IS NOT NULL
    THEN operating.taxonomy
    WHEN purchased.npi <> 'NULL'
    AND purchased.npi IS NOT NULL
    THEN purchased.taxonomy
    WHEN other.npi <> 'NULL'
    AND other.npi IS NOT NULL
    THEN other.taxonomy
    ELSE NULL
    END,                             -- prov_rendering_std_taxonomy
    CASE
    WHEN billing.taxid = 'NULL'
    THEN NULL
    ELSE billing.taxid
    END,                             -- prov_billing_tax_id
    CASE
    WHEN billing.ssn = 'NULL'
    THEN NULL
    ELSE billing.ssn
    END,                             -- prov_billing_ssn
    CASE
    WHEN billing.stlic = 'NULL'
    THEN NULL
    ELSE billing.stlic
    END,                             -- prov_billing_state_license
    CASE
    WHEN billing.upin = 'NULL'
    THEN NULL
    ELSE billing.upin
    END,                             -- prov_billing_upin
    CASE
    WHEN billing.lastname = 'NULL'
    THEN NULL
    ELSE billing.lastname
    END,                             -- prov_billing_name_1
    CASE
    WHEN billing.firstname = 'NULL'
    THEN NULL
    ELSE billing.firstname
    END,                             -- prov_billing_name_2
    CASE
    WHEN billing.addr1 = 'NULL'
    THEN NULL
    ELSE billing.addr1
    END,                             -- prov_billing_address_1
    CASE
    WHEN billing.addr2 = 'NULL'
    THEN NULL
    ELSE billing.addr2
    END,                             -- prov_billing_address_2
    CASE
    WHEN billing.city = 'NULL'
    THEN NULL
    ELSE billing.city
    END,                             -- prov_billing_city
    CASE
    WHEN billing.state = 'NULL'
    THEN NULL
    ELSE billing.state
    END,                             -- prov_billing_state
    CASE
    WHEN billing.zip = 'NULL'
    THEN NULL
    ELSE billing.zip
    END,                             -- prov_billing_zip
    CASE
    WHEN billing.taxonomy = 'NULL'
    THEN NULL
    ELSE billing.taxonomy
    END,                             -- prov_billing_std_taxonomy
    CASE
    WHEN referring.lastname = 'NULL'
    THEN NULL
    ELSE referring.lastname
    END,                             -- prov_referring_name_1
    CASE
    WHEN referring.firstname = 'NULL'
    THEN NULL
    ELSE referring.firstname
    END,                             -- prov_referring_name_2
    CASE
    WHEN referring.addr1 = 'NULL'
    THEN NULL
    ELSE referring.addr1
    END,                             -- prov_referring_address_1
    CASE
    WHEN referring.addr2 = 'NULL'
    THEN NULL
    ELSE referring.addr2
    END,                             -- prov_referring_address_2
    CASE
    WHEN referring.city = 'NULL'
    THEN NULL
    ELSE referring.city
    END,                             -- prov_referring_city
    CASE
    WHEN referring.state = 'NULL'
    THEN NULL
    ELSE referring.state
    END,                             -- prov_referring_state
    CASE
    WHEN referring.zip = 'NULL'
    THEN NULL
    ELSE referring.zip
    END,                             -- prov_referring_zip
    CASE
    WHEN referring.taxonomy = 'NULL'
    THEN NULL
    ELSE referring.taxonomy
    END,                             -- prov_referring_std_taxonomy
    CASE
    WHEN servicelocation.npi <> 'NULL'
    AND servicelocation.npi IS NOT NULL
    THEN servicelocation.lastname
    WHEN ambulancedropoff.npi <> 'NULL'
    AND ambulancedropoff.npi IS NOT NULL
    THEN ambulancedropoff.lastname
    ELSE NULL
    END,                             -- prov_facility_name_1
    CASE
    WHEN servicelocation.npi <> 'NULL'
    AND servicelocation.npi IS NOT NULL
    THEN servicelocation.firstname
    WHEN ambulancedropoff.npi <> 'NULL'
    AND ambulancedropoff.npi IS NOT NULL
    THEN ambulancedropoff.firstname
    ELSE NULL
    END,                             -- prov_facility_name_2
    CASE
    WHEN servicelocation.npi <> 'NULL'
    AND servicelocation.npi IS NOT NULL
    THEN servicelocation.addr1
    WHEN ambulancedropoff.npi <> 'NULL'
    AND ambulancedropoff.npi IS NOT NULL
    THEN ambulancedropoff.addr1
    ELSE NULL
    END,                             -- prov_facility_address_1
    CASE
    WHEN servicelocation.npi <> 'NULL'
    AND servicelocation.npi IS NOT NULL
    THEN servicelocation.addr2
    WHEN ambulancedropoff.npi <> 'NULL'
    AND ambulancedropoff.npi IS NOT NULL
    THEN ambulancedropoff.addr2
    ELSE NULL
    END,                             -- prov_facility_address_2
    CASE
    WHEN servicelocation.npi <> 'NULL'
    AND servicelocation.npi IS NOT NULL
    THEN servicelocation.city
    WHEN ambulancedropoff.npi <> 'NULL'
    AND ambulancedropoff.npi IS NOT NULL
    THEN ambulancedropoff.city
    ELSE NULL
    END,                             -- prov_facility_city
    CASE
    WHEN servicelocation.npi <> 'NULL'
    AND servicelocation.npi IS NOT NULL
    THEN servicelocation.state
    WHEN ambulancedropoff.npi <> 'NULL'
    AND ambulancedropoff.npi IS NOT NULL
    THEN ambulancedropoff.state
    ELSE NULL
    END,                             -- prov_facility_state
    CASE
    WHEN servicelocation.npi <> 'NULL'
    AND servicelocation.npi IS NOT NULL
    THEN servicelocation.zip
    WHEN ambulancedropoff.npi <> 'NULL'
    AND ambulancedropoff.npi IS NOT NULL
    THEN ambulancedropoff.zip
    ELSE NULL
    END,                             -- prov_facility_zip
    CASE
    WHEN servicelocation.npi <> 'NULL'
    AND servicelocation.npi IS NOT NULL
    THEN servicelocation.taxonomy
    WHEN ambulancedropoff.npi <> 'NULL'
    AND ambulancedropoff.npi IS NOT NULL
    THEN ambulancedropoff.taxonomy
    ELSE NULL
    END,                             -- prov_facility_std_taxonomy
    CASE
    WHEN payer2.sequencenumber = 'NULL'
    THEN NULL
    ELSE payer2.sequencenumber
    END,                             -- cob_payer_seq_code_1
    CASE
    WHEN payer2.payerid = 'NULL'
    THEN NULL
    ELSE payer2.payerid
    END,                             -- cob_payer_hpid_1
    CASE
    WHEN payer2.claimfileindicator = 'NULL'
    THEN NULL
    ELSE payer2.claimfileindicator
    END,                             -- cob_payer_claim_filing_ind_code_1
    CASE
    WHEN payer2.payerclassification = 'NULL'
    THEN NULL
    ELSE payer2.payerclassification
    END,                             -- cob_ins_type_code_1
    CASE
    WHEN payer3.sequencenumber = 'NULL'
    THEN NULL
    ELSE payer3.sequencenumber
    END,                             -- cob_payer_seq_code_2
    CASE
    WHEN payer3.payerid = 'NULL'
    THEN NULL
    ELSE payer3.payerid
    END,                             -- cob_payer_hpid_2
    CASE
    WHEN payer3.claimfileindicator = 'NULL'
    THEN NULL
    ELSE payer3.claimfileindicator
    END,                             -- cob_payer_claim_filing_ind_code_2
    CASE
    WHEN payer3.payerclassification = 'NULL'
    THEN NULL
    ELSE payer3.payerclassification
    END                              -- cob_ins_type_code_2
FROM transactional_header header 
    LEFT JOIN matching_payload mp ON header.claimid = mp.claimid
    LEFT JOIN transactional_billing billing ON header.claimid = billing.claimid
    LEFT JOIN transactional_servicelineaffiliation rendering ON header.claimid = rendering.claimid
    AND rendering.type = 'Rendering'
    LEFT JOIN transactional_servicelineaffiliation referring ON header.claimid = referring.claimid
    AND referring.type = 'Referring'
    LEFT JOIN transactional_servicelineaffiliation servicelocation ON header.claimid = servicelocation.claimid
    AND servicelocation.type = 'ServiceLocation'
    LEFT JOIN transactional_servicelineaffiliation ambulancedropoff ON header.claimid = ambulancedropoff.claimid
    AND ambulancedropoff.type = 'AmbulanceDropOff'
    LEFT JOIN transactional_servicelineaffiliation supervising ON header.claimid = supervising.claimid
    AND supervising.type = 'Supervising'
    LEFT JOIN transactional_servicelineaffiliation operating ON header.claimid = operating.claimid
    AND operating.type = 'Operating'
    LEFT JOIN transactional_servicelineaffiliation purchased ON header.claimid = purchased.claimid
    AND purchased.type = 'Purchased'
    LEFT JOIN transactional_servicelineaffiliation other ON header.claimid = other.claimid
    AND other.type = 'Other'
    LEFT JOIN transactional_payer payer1 ON header.claimid = payer1.claimid
    AND payer1.sequencenumber = '1'
    LEFT JOIN transactional_payer payer2 ON header.claimid = payer2.claimid
    AND payer2.sequencenumber = '2'
    LEFT JOIN transactional_payer payer3 ON header.claimid = payer3.claimid
    AND payer3.sequencenumber = '3'
    LEFT JOIN transactional_diagnosis diagnosis ON diagnosis.claimid = header.claimid
WHERE header.Type = 'Institutional'
;
