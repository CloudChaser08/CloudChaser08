SELECT
    header.claimid                      AS claim_id,
    hvid                                AS hvid,
    mp.gender                           AS patient_gender,
    mp.age                              AS patient_age,
    mp.yearofbirth                      AS patient_year_of_birth,
    mp.threedigitzip                    AS patient_zip3,
    mp.state                            AS patient_state,
    'I'                                 AS claim_type,
    extract_date(
        substring(header.processdate, 1, 10), '%Y-%m-%d'
        )                               AS date_received,
    extract_date(
        substring(COALESCE(
                header.startdate, 
                (SELECT MIN(sl2.servicestart)
                FROM transactional_serviceline sl2
                WHERE sl2.claimid = header.claimid)
        ), 1, 10), '%Y-%m-%d', CAST({min_date} AS DATE)
        )                               AS date_service,
    extract_date(
        substring(
            CASE 
                WHEN header.startdate IS NOT NULL
                    THEN header.enddate
                ELSE (
                    SELECT MAX(sl2.serviceend)
                    FROM transactional_serviceline sl2
                    WHERE sl2.claimid = header.claimid
                )
            END, 1, 10), '%Y-%m-%d', CAST({min_date} AS DATE)
        )                               AS date_service_end,
    header.admissiontype                AS inst_admit_type_std_id,
    header.admissionsource              AS inst_admit_source_std_id,
    header.dischargestatus              AS inst_discharge_status_std_id,
    CONCAT(header.institutionaltype, header.claimfrequencycode)
                                        AS inst_type_of_bill_std_id,
    header.drgcode                      AS inst_drg_std_id,
    diagnosis.diagnosiscode             AS diagnosis_code,
    CASE
        WHEN diagnosis.type LIKE 'A%'
        THEN '02'
        WHEN diagnosis.type LIKE 'B%'
        THEN '01'
    END                                 AS diagnosis_code_qual,
    payer1.claimfileindicator           AS medical_coverage_type,
    header.totalcharge                  AS total_charge,
    COALESCE(rendering_claim.npi, supervising_claim.npi,
        operating_claim.npi, operating_claim.npi,
        purchased_claim.npi, other_claim.npi)
                                        AS prov_rendering_npi,
    billing.npi                         AS prov_billing_npi,
    referring_claim.npi                 AS prov_referring_npi,
    COALESCE(servicelocation_claim.npi, ambulancedropoff_claim.npi)
                                        AS prov_facility_npi,
    payer1.sourcepayerid                AS payer_vendor_id,
    payer1.name                         AS payer_name,
    payer1.payerclassification          AS payer_type,
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
    END                                 AS prov_rendering_name_1,
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
    END                                 AS prov_rendering_name_2,
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
    END                                 AS prov_rendering_address_1,
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
    END                                 AS prov_rendering_address_2,
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
    END                                 AS prov_rendering_city,
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
    END                                 AS prov_rendering_state,
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
    END                                 AS prov_rendering_zip,
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
    END                                 AS prov_rendering_std_taxonomy,
    billing.taxid                       AS prov_billing_tax_id,
    billing.ssn                         AS prov_billing_ssn,
    billing.stlic                       AS prov_billing_state_license,
    billing.upin                        AS prov_billing_upin,
    billing.lastname                    AS prov_billing_name_1,
    billing.firstname                   AS prov_billing_name_2,
    billing.addr1                       AS prov_billing_address_1,
    billing.addr2                       AS prov_billing_address_2,
    billing.city                        AS prov_billing_city,
    billing.state                       AS prov_billing_state,
    billing.zip                         AS prov_billing_zip,
    billing.taxonomy                    AS prov_billing_std_taxonomy,
    referring_claim.lastname            AS prov_referring_name_1,
    referring_claim.firstname           AS prov_referring_name_2,
    referring_claim.addr1               AS prov_referring_address_1,
    referring_claim.addr2               AS prov_referring_address_2,
    referring_claim.city                AS prov_referring_city,
    referring_claim.state               AS prov_referring_state,
    referring_claim.zip                 AS prov_referring_zip,
    referring_claim.taxonomy            AS prov_referring_std_taxonomy,
    CASE
        WHEN servicelocation_claim.npi IS NOT NULL
        THEN servicelocation_claim.lastname
        WHEN ambulancedropoff_claim.npi IS NOT NULL
        THEN ambulancedropoff_claim.lastname
    END                                 AS prov_facility_name_1,
    CASE
        WHEN servicelocation_claim.npi IS NOT NULL
        THEN servicelocation_claim.firstname
        WHEN ambulancedropoff_claim.npi IS NOT NULL
        THEN ambulancedropoff_claim.firstname
    END                                 AS prov_facility_name_2,
    CASE
        WHEN servicelocation_claim.npi IS NOT NULL
        THEN servicelocation_claim.addr1
        WHEN ambulancedropoff_claim.npi IS NOT NULL
        THEN ambulancedropoff_claim.addr1
    END                                 AS prov_facility_address_1,
    CASE
        WHEN servicelocation_claim.npi IS NOT NULL
        THEN servicelocation_claim.addr2
        WHEN ambulancedropoff_claim.npi IS NOT NULL
        THEN ambulancedropoff_claim.addr2
    END                                 AS prov_facility_address_2,
    CASE
        WHEN servicelocation_claim.npi IS NOT NULL
        THEN servicelocation_claim.city
        WHEN ambulancedropoff_claim.npi IS NOT NULL
        THEN ambulancedropoff_claim.city
    END                                 AS prov_facility_city,
    CASE
        WHEN servicelocation_claim.npi IS NOT NULL
        THEN servicelocation_claim.state
        WHEN ambulancedropoff_claim.npi IS NOT NULL
        THEN ambulancedropoff_claim.state
    END                                 AS prov_facility_state,
    CASE
        WHEN servicelocation_claim.npi IS NOT NULL
        THEN servicelocation_claim.zip
        WHEN ambulancedropoff_claim.npi IS NOT NULL
        THEN ambulancedropoff_claim.zip
    END                                 AS prov_facility_zip,
    CASE
        WHEN servicelocation_claim.npi IS NOT NULL
        THEN servicelocation_claim.taxonomy
        WHEN ambulancedropoff_claim.npi IS NOT NULL
        THEN ambulancedropoff_claim.taxonomy
    END                                 AS prov_facility_std_taxonomy,
    payer2.sequencenumber               AS cob_payer_seq_code_1,
    payer2.payerid                      AS cob_payer_hpid_1,
    payer2.claimfileindicator           AS cob_payer_claim_filing_ind_code_1,
    payer2.payerclassification          AS cob_ins_type_code_1,
    payer3.sequencenumber               AS cob_payer_seq_code_2,
    payer3.payerid                      AS cob_payer_hpid_2,
    payer3.claimfileindicator           AS cob_payer_claim_filing_ind_code_2,
    payer3.payerclassification          AS cob_ins_type_code_2
FROM transactional_header header 
    LEFT JOIN matching_payload mp ON header.claimid = mp.claimid
    LEFT JOIN transactional_billing billing ON header.claimid = billing.claimid

    -- providers
    LEFT JOIN transactional_claimaffiliation_rendering rendering_claim ON header.claimid = rendering_claim.claimid
    LEFT JOIN transactional_claimaffiliation_referring referring_claim ON header.claimid = referring_claim.claimid
    LEFT JOIN transactional_claimaffiliation_servicelocation servicelocation_claim ON header.claimid = servicelocation_claim.claimid
    LEFT JOIN transactional_claimaffiliation_ambulancedropoff ambulancedropoff_claim ON header.claimid = ambulancedropoff_claim.claimid
    LEFT JOIN transactional_claimaffiliation_supervising supervising_claim ON header.claimid = supervising_claim.claimid
    LEFT JOIN transactional_claimaffiliation_operating operating_claim ON header.claimid = operating_claim.claimid
    LEFT JOIN transactional_claimaffiliation_purchased purchased_claim ON header.claimid = purchased_claim.claimid
    LEFT JOIN transactional_claimaffiliation_other other_claim ON header.claimid = other_claim.claimid

    -- payer
    LEFT JOIN transactional_payer payer1 ON header.claimid = payer1.claimid
    AND payer1.sequencenumber = '1'
    LEFT JOIN transactional_payer payer2 ON header.claimid = payer2.claimid
    AND payer2.sequencenumber = '2'
    LEFT JOIN transactional_payer payer3 ON header.claimid = payer3.claimid
    AND payer3.sequencenumber = '3'

    -- diag
    LEFT JOIN transactional_diagnosis diagnosis ON diagnosis.claimid = header.claimid
WHERE header.type = 'Institutional'
