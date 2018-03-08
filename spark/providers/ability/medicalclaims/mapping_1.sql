SELECT
    header.claimid                                  AS claim_id,
    mp.hvid                                         AS hvid,
    mp.gender                                       AS patient_gender,
    mp.age                                          AS patient_age,
    mp.yearofbirth                                  AS patient_year_of_birth,
    mp.threedigitzip                                AS patient_zip3,
    mp.state                                        AS patient_state,
    CASE
        WHEN header.type = 'Institutional'
        THEN 'I'
        WHEN header.type = 'Professional'
        THEN 'P'
    END                                             AS claim_type,
    extract_date(
        substring(header.processdate, 1, 10), '%Y-%m-%d'
        )                                           AS date_received,
    extract_date(
        substring(COALESCE(
                serviceline.servicestart,
                header.startdate, 
                (SELECT MIN(sl2.servicestart)
                FROM transactional_serviceline sl2
                WHERE sl2.claimid = serviceline.claimid)
        ), 1, 10), '%Y-%m-%d'
        )                                           AS date_service,
    extract_date(
        substring(
            CASE 
                WHEN serviceline.servicestart IS NOT NULL 
                    THEN serviceline.serviceend
                WHEN header.startdate IS NOT NULL
                    THEN header.enddate
                ELSE (
                    SELECT MAX(sl2.serviceend)
                    FROM transactional_serviceline sl2
                    WHERE sl2.claimid = serviceline.claimid
                )
            END, 1, 10), '%Y-%m-%d'
        )                                           AS date_service_end,
    CASE 
        WHEN header.type = 'Institutional'
        THEN header.admissiontype
    END                                             AS inst_admit_type_std_id,
    CASE 
        WHEN header.type = 'Institutional'
        THEN header.admissionsource
    END                                             AS inst_admit_source_std_id,
    CASE 
        WHEN header.type = 'Institutional'
        THEN header.dischargestatus
    END                                             AS inst_discharge_status_std_id,
    CASE 
        WHEN header.type = 'Institutional' AND header.institutionaltype IS NOT NULL
        THEN CONCAT(header.institutionaltype, header.claimfrequencycode)
    END                                             AS inst_type_of_bill_std_id,
    CASE 
        WHEN header.type = 'Institutional'
        THEN header.drgcode
    END                                             AS inst_drg_std_id,
    CASE 
        WHEN header.type = 'Professional'
        THEN COALESCE(serviceline.placeofservice, header.institutionaltype)
    END                                             AS place_of_service_std_id,
    serviceline.sequencenumber                      AS service_line_number,
    diagnosis.diagnosiscode                         AS diagnosis_code,
    CASE
        WHEN diagnosis.type LIKE 'A%'
        THEN '02'
        WHEN diagnosis.type LIKE 'B%'
        THEN '01'
    END                                             AS diagnosis_code_qual,
    CASE 
        WHEN diagnosis.sequencenumber = serviceline.diagnosiscodepointer1
        THEN '1'
        WHEN diagnosis.sequencenumber = serviceline.diagnosiscodepointer2
        THEN '2'
        WHEN diagnosis.sequencenumber = serviceline.diagnosiscodepointer3
        THEN '3'
        WHEN diagnosis.sequencenumber = serviceline.diagnosiscodepointer4
        THEN '4'
    END                                             AS diagnosis_priority,
    CASE
        WHEN header.type = 'Institutional' AND diagnosis.diagnosiscode = header.admissiondiagnosis
        THEN 'Y'
        WHEN header.type = 'Institutional'
        THEN 'N'
    END                                             AS admit_diagnosis_ind,
    CASE 
        WHEN header.type = 'Professional'
            THEN serviceline.procedurecode 
        WHEN header.type = 'Institutional'
            THEN COALESCE(serviceline.procedurecode, proc.procedurecode)
    END                                             AS procedure_code,
    serviceline.qualifier                           AS procedure_code_qual,
    CASE 
        WHEN header.type = 'Institutional' AND proc.procedurecode = serviceline.procedurecode
            AND proc.sequencenumber = '1'
        THEN 'Y'
        WHEN header.type = 'Institutional'
        THEN 'N'
    END                                             AS principal_proc_ind,
    serviceline.amount                              AS procedure_units,
    serviceline.modifier1                           AS procedure_modifier_1,
    serviceline.modifier2                           AS procedure_modifier_2,
    serviceline.modifier3                           AS procedure_modifier_3,
    serviceline.modifier4                           AS procedure_modifier_4,
    serviceline.revenuecode                         AS revenue_code,
    serviceline.drugcode                            AS ndc_code,
    payer1.claimfileindicator                       AS medical_coverage_type,
    serviceline.linecharge                          AS line_charge,
    header.totalcharge                              AS total_charge,
    COALESCE(
        rendering.npi, rendering_claim.npi, supervising.npi,
        supervising_claim.npi, operating.npi, operating_claim.npi,
        purchased.npi, purchased_claim.npi, other.npi,
        other_claim.npi
    )                                               AS prov_rendering_npi,
    billing.npi                                     AS prov_billing_npi,
    COALESCE(referring.npi, referring_claim.npi)    AS prov_referring_npi,
    COALESCE(servicelocation.npi, servicelocation_claim.npi,
        ambulancedropoff.npi, ambulancedropoff_claim.npi)
                                                    AS prov_facility_npi,
    payer1.sourcepayerid                            AS payer_vendor_id,
    payer1.name                                     AS payer_name,
    payer1.payerclassification                      AS payer_type,
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
    END                                             AS prov_rendering_name_1,
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
    END                                             AS prov_rendering_name_2,
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
    END                                             AS prov_rendering_address_1,
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
    END                                             AS prov_rendering_address_2,
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
    END                                             AS prov_rendering_city,
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
    END                                             AS prov_rendering_state,
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
    END                                             AS prov_rendering_zip,
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
    END                                             AS prov_rendering_std_taxonomy,
    billing.taxid                                   AS prov_billing_tax_id,
    billing.ssn                                     AS prov_billing_ssn,
    billing.stlic                                   AS prov_billing_state_license,
    billing.upin                                    AS prov_billing_upin,
    billing.lastname                                AS prov_billing_name_1,
    billing.firstname                               AS prov_billing_name_2,
    billing.addr1                                   AS prov_billing_address_1,
    billing.addr2                                   AS prov_billing_address_2,
    billing.city                                    AS prov_billing_city,
    billing.state                                   AS prov_billing_state,
    billing.zip                                     AS prov_billing_zip,
    billing.taxonomy                                AS prov_billing_std_taxonomy,
    CASE
        WHEN referring.npi IS NOT NULL
        THEN referring.lastname
        WHEN referring_claim.npi IS NOT NULL
        THEN referring_claim.lastname
    END                                             AS prov_referring_name_1,
    CASE
        WHEN referring.npi IS NOT NULL
        THEN referring.firstname
        WHEN referring_claim.npi IS NOT NULL
        THEN referring_claim.firstname
    END                                             AS prov_referring_name_2,
    CASE
        WHEN referring.npi IS NOT NULL
        THEN referring.addr1
        WHEN referring_claim.npi IS NOT NULL
        THEN referring_claim.addr1
    END                                             AS prov_referring_address_1,
    CASE
        WHEN referring.npi IS NOT NULL
        THEN referring.addr2
        WHEN referring_claim.npi IS NOT NULL
        THEN referring_claim.addr2
    END                                             AS prov_referring_address_2,
    CASE
        WHEN referring.npi IS NOT NULL
        THEN referring.city
        WHEN referring_claim.npi IS NOT NULL
        THEN referring_claim.city
    END                                             AS prov_referring_city,
    CASE
        WHEN referring.npi IS NOT NULL
        THEN referring.state
        WHEN referring_claim.npi IS NOT NULL
        THEN referring_claim.state
    END                                             AS prov_referring_state,
    CASE
        WHEN referring.npi IS NOT NULL
        THEN referring.zip
        WHEN referring_claim.npi IS NOT NULL
        THEN referring_claim.zip
    END                                             AS prov_referring_zip,
    CASE
        WHEN referring.npi IS NOT NULL
        THEN referring.taxonomy
        WHEN referring_claim.npi IS NOT NULL
        THEN referring_claim.taxonomy
    END                                             AS prov_referring_std_taxonomy,
    CASE
        WHEN servicelocation.npi IS NOT NULL
        THEN servicelocation.lastname
        WHEN servicelocation_claim.npi IS NOT NULL
        THEN servicelocation_claim.lastname
        WHEN ambulancedropoff.npi IS NOT NULL
        THEN ambulancedropoff.lastname
        WHEN ambulancedropoff_claim.npi IS NOT NULL
        THEN ambulancedropoff_claim.lastname
    END                                             AS prov_facility_name_1,
    CASE
        WHEN servicelocation.npi IS NOT NULL
        THEN servicelocation.firstname
        WHEN servicelocation_claim.npi IS NOT NULL
        THEN servicelocation_claim.firstname
        WHEN ambulancedropoff.npi IS NOT NULL
        THEN ambulancedropoff.firstname
        WHEN ambulancedropoff_claim.npi IS NOT NULL
        THEN ambulancedropoff_claim.firstname
    END                                             AS prov_facility_name_2,
    CASE
        WHEN servicelocation.npi IS NOT NULL
        THEN servicelocation.addr1
        WHEN servicelocation_claim.npi IS NOT NULL
        THEN servicelocation_claim.addr1
        WHEN ambulancedropoff.npi IS NOT NULL
        THEN ambulancedropoff.addr1
        WHEN ambulancedropoff_claim.npi IS NOT NULL
        THEN ambulancedropoff_claim.addr1
    END                                             AS prov_facility_address_1,
    CASE
        WHEN servicelocation.npi IS NOT NULL
        THEN servicelocation.addr2
        WHEN servicelocation_claim.npi IS NOT NULL
        THEN servicelocation_claim.addr2
        WHEN ambulancedropoff.npi IS NOT NULL
        THEN ambulancedropoff.addr2
        WHEN ambulancedropoff_claim.npi IS NOT NULL
        THEN ambulancedropoff_claim.addr2
    END                                             AS prov_facility_address_2,
    CASE
        WHEN servicelocation.npi IS NOT NULL
        THEN servicelocation.city
        WHEN servicelocation_claim.npi IS NOT NULL
        THEN servicelocation_claim.city
        WHEN ambulancedropoff.npi IS NOT NULL
        THEN ambulancedropoff.city
        WHEN ambulancedropoff_claim.npi IS NOT NULL
        THEN ambulancedropoff_claim.city
    END                                             AS prov_facility_city,
    CASE
        WHEN servicelocation.npi IS NOT NULL
        THEN servicelocation.state
        WHEN servicelocation_claim.npi IS NOT NULL
        THEN servicelocation_claim.state
        WHEN ambulancedropoff.npi IS NOT NULL
        THEN ambulancedropoff.state
        WHEN ambulancedropoff_claim.npi IS NOT NULL
        THEN ambulancedropoff_claim.state
    END                                             AS prov_facility_state,
    CASE
        WHEN servicelocation.npi IS NOT NULL
        THEN servicelocation.zip
        WHEN servicelocation_claim.npi IS NOT NULL
        THEN servicelocation_claim.zip
        WHEN ambulancedropoff.npi IS NOT NULL
        THEN ambulancedropoff.zip
        WHEN ambulancedropoff_claim.npi IS NOT NULL
        THEN ambulancedropoff_claim.zip
    END                                             AS prov_facility_zip,
    CASE
        WHEN servicelocation.npi IS NOT NULL
        THEN servicelocation.taxonomy
        WHEN servicelocation_claim.npi IS NOT NULL
        THEN servicelocation_claim.taxonomy
        WHEN ambulancedropoff.npi IS NOT NULL
        THEN ambulancedropoff.taxonomy
        WHEN ambulancedropoff_claim.npi IS NOT NULL
        THEN ambulancedropoff_claim.taxonomy
    END                                             AS prov_facility_std_taxonomy,
    payer2.sequencenumber                           AS cob_payer_seq_code_1,
    payer2.payerid                                  AS cob_payer_hpid_1,
    payer2.claimfileindicator                       AS cob_payer_claim_filing_ind_code_1,
    payer2.payerclassification                      AS cob_ins_type_code_1,
    payer3.sequencenumber                           AS cob_payer_seq_code_2,
    payer3.payerid                                  AS cob_payer_hpid_2,
    payer3.claimfileindicator                       AS cob_payer_claim_filing_ind_code_2,
    payer3.payerclassification                      AS cob_ins_type_code_2
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
    LEFT JOIN transactional_servicelineaffiliation other ON serviceline.servicelineid = other.servicelineid 
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
