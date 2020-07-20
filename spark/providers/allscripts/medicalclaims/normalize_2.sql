SELECT DISTINCT
    header_entity_id                        AS claim_id,
    hvid                                    AS hvid,
    header_version_code                     AS source_version,
    CASE WHEN COALESCE(header_patient_sex_, gender) IN ('F', 'M')
        THEN COALESCE(header_patient_sex_, gender)
        ELSE 'U' END                        AS patient_gender,
    COALESCE(SUBSTR(header_patient_dob, 1, 4), yearOfBirth)
                                            AS patient_year_of_birth,
    SUBSTR(COALESCE(header_patient_zip_code, threeDigitZip), 1, 3)
                                            AS patient_zip3, 
    UPPER(COALESCE(header_patient_state, state, ''))
                                            AS patient_state,
    'P'                                     AS claim_type,
    extract_date(
        substring(header_create_date, length(header_create_date) - 5, 8), '%y%m%d'
    )                                       AS date_received,
    earliest_service_date                   AS date_service,
    latest_service_date                     AS date_service_end,
    service_place_of_service                AS place_of_service_std_id,
    all_diagnoses[x.n]                      AS diagnosis_code,
    header_source_of_payment                AS medical_coverage_type,
    header_total_claim_charge_amount        AS total_charge,
    COALESCE(service_rendering_provider_npi, header_rendering_provider_npi)
                                            AS prov_rendering_npi,
    header_billing_prov_npi                 AS prov_billing_npi,
    COALESCE(service_referring_provider_npi, header_referring_prov_npi_)
                                            AS prov_referring_npi,
    COALESCE(service_service_facility_npi, header_facility_lab_npi)
                                            AS prov_facility_npi,
    header_primary_payer_name               AS payer_name,
    header_primary_payer_tspid              AS payer_plan_id,
    header_insurance_type_code              AS payer_type,
    CASE WHEN service_rendering_provider_tax_id_qual IN ('24', '34')
            THEN service_rendering_provider_primary_id
        WHEN header_rendering_provider_primary_id_qualifier IN ('24', '34')
            THEN header_rendering_provider_primary_id
    END                                     AS prov_rendering_tax_id,
    CASE WHEN service_rendering_provider_tax_id_qual = '24'
            THEN service_rendering_provider_primary_id
        WHEN header_rendering_provider_primary_id_qualifier = '24'
            THEN header_rendering_provider_primary_id
    END                                     AS prov_rendering_ssn,
    CASE WHEN service_rendering_provider_last IS NOT NULL AND service_rendering_provider_first IS NOT NULL
            THEN TRIM(CONCAT(service_rendering_provider_last, ', ', service_rendering_provider_first, ' ', COALESCE(service_rendering_provider_middle, '')))
        WHEN service_rendering_provider_last IS NOT NULL
            THEN service_rendering_provider_last
        WHEN header_rendering_provider_last IS NOT NULL AND header_rendering_provider_first IS NOT NULL
            THEN TRIM(CONCAT(header_rendering_provider_last, ', ', header_rendering_provider_first, ' ', COALESCE(header_rendering_provider_middle, '')))
        ELSE header_rendering_provider_last
    END                                     AS prov_rendering_name_2,
    COALESCE(service_rendering_provider_specialty_code, header_rendering_provider_specialty_code)
                                            AS prov_rendering_std_taxonomy,
    CASE WHEN header_billing_prov_id_qual IN ('24', '34')
        THEN header_billing_prov_tax_id
    END                                     AS prov_billing_tax_id,
    CASE WHEN header_billing_prov_id_qual = '24'
        THEN header_billing_prov_tax_id
    END                                     AS prov_billing_ssn,
    header_billing_prov_organization_name_or_billing_prov_last_name
                                            AS prov_billing_name_1,
    CASE WHEN header_billing_prov_last_name IS NOT NULL AND header_billing_prov_first_name IS NOT NULL
        THEN TRIM(CONCAT(header_billing_prov_last_name, ', ', header_billing_prov_first_name, ' ', COALESCE(header_billing_prov_mi, '')))
        ELSE header_billing_prov_last_name
    END                                     AS prov_billing_name_2,
    header_billing_providers_address_1      AS prov_billing_address_1,
    header_billing_providers_address_2      AS prov_billing_address_2,
    header_billing_providers_city           AS prov_billing_city,
    header_billing_providers_state          AS prov_billing_state,
    header_billing_providers_zip            AS prov_billing_zip,
    header_billing_or_pay_to_provider_taxonomy_code
                                            AS prov_billing_std_taxonomy,
    CASE WHEN header_referring_provider_primary_id_qualifier IN ('24', '34')
        THEN header_referring_provider_primary_id
    END                                     AS prov_referring_tax_id,
    CASE WHEN header_referring_provider_primary_id_qualifier = '24'
        THEN header_referring_provider_primary_id
    END                                     AS prov_referring_ssn,
    CASE WHEN service_referring_provider_last_name IS NOT NULL AND service_referring_provider_first_name IS NOT NULL
            THEN TRIM(CONCAT(service_referring_provider_last_name, ', ', service_referring_provider_first_name, ' ', COALESCE(service_referring_provider_mi,'')))
        WHEN service_referring_provider_last_name IS NOT NULL
            THEN service_referring_provider_last_name
        WHEN header_referring_provider_last_name IS NOT NULL AND header_referring_provider_first_name IS NOT NULL
            THEN TRIM(CONCAT(header_referring_provider_last_name, ', ', header_referring_provider_first_name, ' ', COALESCE(header_referring_provider_middle_initial, '')))
        ELSE header_referring_provider_last_name
    END                                     AS prov_referring_name_2,
    header_referring_provider_taxonomy_code AS prov_referring_std_taxonomy,
    CASE WHEN header_facility_lab_primary_id_qualifier IN ('24', '34')
        THEN header_facility_laboratory_primary_identifier
    END                                     AS prov_facility_tax_id,
    CASE WHEN header_facility_lab_primary_id_qualifier = '24'
        THEN header_facility_laboratory_primary_identifier
    END                                     AS prov_facility_ssn,
    CASE WHEN service_service_facility_name IS NOT NULL
        THEN service_service_facility_name
        ELSE header_facility_laboratory_name
    END                                     AS prov_facility_name_1,
    COALESCE(service_service_facility_address_1, header_facility_laboratory_street_address_1)
                                            AS prov_facility_address_1,
    COALESCE(service_service_facility_address_2, header_facility_laboratory_street_address_2)
                                            AS prov_facility_address_2,
    COALESCE(service_service_facility_city, header_facility_laboratory_city)
                                            AS prov_facility_city,
    UPPER(COALESCE(service_service_facility_state, header_facility_laboratory_state))
                                            AS prov_facility_state,
    COALESCE(service_service_facility_zip_code, header_facility_laboratory_zip_code)
                                            AS prov_facility_zip,
    header_2nd_payer_primary_id             AS cob_payer_vendor_id_1,
    header_secondary_payer_sequence_number  AS cob_payer_seq_code_1,
    header_secondary_payer_source_of_payment
                                            AS cob_payer_claim_filing_ind_code_1,
    header_secondary_payer_insurance_type_code
                                            AS cob_ins_type_code_1,
    header_3rd_payer_primary_id             AS cob_payer_vendor_id_2,
    header_teritary_payer_sequence_number   AS cob_payer_seq_code_2,
    header_teritary_payer_source_of_payment AS cob_payer_claim_filing_ind_code_2,
    header_teritary_payer_insurance_type_code
                                            AS cob_ins_type_code_2,
    pcn                                     AS medical_claim_link_text
FROM tmp2
    CROSS JOIN diag_exploder x
WHERE all_diagnoses[x.n] IS NOT NULL
    AND NOT array_contains(linked_diagnoses, all_diagnoses[x.n])
    AND service_charge_line_number = '1'
