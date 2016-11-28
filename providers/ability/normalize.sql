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
        inst_jdischarge_status_std_id,
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
SELECT
    header.ClaimId,              -- claim_id
-- hvid
    1,                           -- source_version
-- patient_gender
-- patient_age
-- patient_year_of_birth
-- patient_zip3
-- patient_state
    header.Type,                 -- claim_type
    header.ProcessDate,          -- date_received
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
    END,                         -- date_service
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
    END,                         -- date_service_end
    CASE 
    WHEN header.Type = 'Institutional'
    THEN header.AdmissionType
    END,                         -- inst_admit_type_std_id
    CASE 
    WHEN header.Type = 'Institutional'
    THEN header.AdmissionSource
    END,                         -- inst_admit_source_std_id
    CASE 
    WHEN header.Type = 'Institutional'
    THEN header.DischargeStatus
    END,                         -- inst_discharge_status_std_id
    CASE 
    WHEN header.Type <> 'Institutional'
    THEN NULL
    WHEN serviceline.PlaceOfService IS NOT NULL
    THEN serviceline.PlaceOfService || serviceline.FacilityType
    WHEN header.InstitutionalType IS NOT NULL
    THEN header.InstitutionalType || header.ClaimFrequencyCode
    END,                         -- inst_type_of_bill_std_id
    CASE 
    WHEN header.Type = 'Institutional'
    THEN header.DrgCode
    END,                         -- inst_drg_std_id
    CASE 
    WHEN header.Type = 'Professional'
    THEN COALESCE(ServiceLine.PlaceOfService, header.InstitutionalType)
    END,                         -- place_of_service_std_id
    serviceline.SequenceNumber,  -- service_line_number
    diagnosis.DiagnosisCode,     -- diagnosis_code
    diagnosis.DiagnosisType,     -- diagnosis_code_qual
    CASE 
    WHEN diagnosis.SequenceNumber = serviceline.diagnosiscodepointer1
    THEN '1'
    WHEN diagnosis.SequenceNumber = serviceline.diagnosiscodepointer2
    THEN '2'
    WHEN diagnosis.SequenceNumber = serviceline.diagnosiscodepointer3
    THEN '3'
    WHEN diagnosis.SequenceNumber = serviceline.diagnosiscodepointer4
    THEN '4'
    END,                         -- diagnosis_priority
-- admit_diagnosis_ind
    CASE 
    WHEN header.Type = 'Professional'
    THEN serviceline.ProcedureCode 
    WHEN header.Type = 'Institutional'
    THEN COALESCE(serviceline.procedurecode, proc.procedurecode) 
    END,                         -- procedure_code
    serviceline.qualifier,       -- procedure_code_qual
    CASE 
    WHEN header.type <> 'Institutional'
    THEN NULL
    WHEN proc.procedurecode = serviceline.procedurecode
    AND proc.sequencenumber = '1'
    THEN 'Y'
    ELSE 'N'
    END,                         -- principal_proc_ind
    serviceline.amount,          -- procedure_units
    serviceline.modifier1,       -- procedure_modifier_1
    serviceline.modifier2,       -- procedure_modifier_2
    serviceline.modifier3,       -- procedure_modifier_3
    serviceline.modifier4,       -- procedure_modifier_4
    serviceline.revenuecode,     -- revenue_code
    serviceline.drugcode,        -- ndc_code
    payer1.claimfileindicator,   -- medical_coverage_type
    serviceline.linecharge,      -- line_charge
    header.totalcharge,          -- total_charge
    rendering.npi,               -- prov_rendering_npi
    billing.npi,                 -- prov_billing_npi
    referring.npi,               -- prov_referring_npi
    facility.npi,                -- prov_facility_npi
    payer1.sourcepayerid,        -- payer_vendor_id
    payer1.name,                 -- payer_name
    payer1.payerclassification,  -- payer_type
    rendering.lastname,          -- prov_rendering_name_1
    rendering.firstname,         -- prov_rendering_name_2
    rendering.addr1,             -- prov_rendering_address_1
    rendering.addr2,             -- prov_rendering_address_2
    rendering.city,              -- prov_rendering_city
    rendering.state,             -- prov_rendering_state
    rendering.zip,               -- prov_rendering_zip
    rendering.taxonomy,          -- prov_rendering_std_taxonomy
    billing.taxid,               -- prov_billing_tax_id
    billing.ssn,                 -- prov_billing_ssn
    billing.stlic,               -- prov_billing_state_license
    billing.upin,                -- prov_billing_upin
    billing.lastname,            -- prov_billing_name_1
    billing.firstname,           -- prov_billing_name_2
    billing.addr1,               -- prov_billing_address_1
    billing.addr2,               -- prov_billing_address_2
    billing.city,                -- prov_billing_city
    billing.state,               -- prov_billing_state
    billing.zip,                 -- prov_billing_zip
    billing.taxonomy,            -- prov_billing_std_taxonomy
    referring.lastname,          -- prov_referring_name_1
    referring.firstname,         -- prov_referring_name_2
    referring.addr1,             -- prov_referring_address_1
    referring.addr2,             -- prov_referring_address_2
    referring.city,              -- prov_referring_city
    referring.state,             -- prov_referring_state
    referring.zip,               -- prov_referring_zip
    referring.taxonomy,          -- prov_referring_std_taxonomy
    facility.lastname,           -- prov_facility_name_1
    facility.firstname,          -- prov_facility_name_2
    facility.addr1,              -- prov_facility_address_1
    facility.addr2,              -- prov_facility_address_2
    facility.city,               -- prov_facility_city
    facility.state,              -- prov_facility_state
    facility.zip,                -- prov_facility_zip
    facility.taxonomy,           -- prov_facility_std_taxonomy
    payer2.sequencenumber,       -- cob_payer_seq_code_1
    payer2.payerid,              -- cob_payer_hpid_1
    payer2.claimfileindicator,   -- cob_payer_claim_filing_ind_code_1
    payer2.payerclassification,  -- cob_ins_type_code_1
    payer3.sequencenumber,       -- cob_payer_seq_code_2
    payer3.payerid,              -- cob_payer_hpid_2
    payer3.claimfileindicator,   -- cob_payer_claim_filing_ind_code_2
    payer3.payerclassification   -- cob_ins_type_code_
FROM transactional_header header 
    LEFT JOIN transactional_serviceline serviceline ON header.claimid = serviceline.claimid
    LEFT JOIN transactional_billing billing ON header.claimid = billing.claimid
    LEFT JOIN transactional_servicelineaffiliation rendering ON serviceline.servicelineid = rendering.servicelineid 
    AND serviceline.claimid = rendering.claimid
    AND rendering.type = 'Rendering'
    LEFT JOIN transactional_servicelineaffiliation referring ON serviceline.servicelineid = referring.servicelineid 
    AND serviceline.claimid = referring.claimid
    AND referring.type = 'Referring'
    LEFT JOIN transactional_servicelineaffiliation referring ON serviceline.servicelineid = referring.servicelineid 
    AND serviceline.claimid = referring.claimid
    AND referring.type = 'Facility'
    LEFT JOIN transactional_payer payer1 ON header.claimid = payer1.claimid
    AND payer1.sequencenumber = '1'
    LEFT JOIN transactional_payer payer2 ON header.claimid = payer2.claimid
    AND payer2.sequencenumber = '2'
    LEFT JOIN transactional_payer payer3 ON header.claimid = payer3.claimid
    AND payer3.sequencenumber = '3'
    LEFT JOIN transactional_diagnosis diagnosis ON diagnosis.claimid = header.claimid
    AND header.Type = 'Professional'
    AND (
        serviceline.diagnosiscodepointer1 = diagnosis.sequencenumber 
        OR serviceline.diagnosiscodepointer2 = diagnosis.sequencenumber 
        OR serviceline.diagnosiscodepointer3 = diagnosis.sequencenumber 
        OR serviceline.diagnosiscodepointer4 = diagnosis.sequencenumber 
        )
    LEFT JOIN transactional_procedure proc ON proc.claimid = header.claimid
;
