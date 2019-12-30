SELECT MONOTONICALLY_INCREASING_ID() AS record_id, * FROM (

    SELECT
        claim_id,
        hvid,
        created,
        model_version,
        data_set,
        data_feed,
        data_vendor,
        patient_gender,
        patient_age,
        patient_year_of_birth,
        patient_zip3,
        patient_state,
        claim_type,
        date_received,
        /* date_service */
        /* For professional claims, if date_service is NULL, set it to date_service_end. */
        CASE
            WHEN COALESCE(claim_type, 'X') = 'P'
                THEN COALESCE(date_service, date_service_end)
            ELSE date_service
        END                                                                                     AS date_service,
        /* date_service_end */
        /* For professional claims, if date_service_end is NULL, set it to date_service. */
        CASE
            WHEN COALESCE(claim_type, 'X') = 'P'
                THEN COALESCE(date_service_end, date_service)
            ELSE date_service_end
        END                                                                                     AS date_service_end,
        inst_date_admitted,
        inst_admit_type_std_id,
        inst_admit_source_std_id,
        inst_discharge_status_std_id,
        inst_type_of_bill_std_id,
        inst_drg_std_id,
        place_of_service_std_id,
        service_line_number,
        /* diagnosis_code */
        /* We left the privacy filtering to the final normalization */
        /* so we can accurately add the claim-level diagnoses.      */
        CLEAN_UP_DIAGNOSIS_CODE
            (
                diagnosis_code,
                diagnosis_code_qual,
                date_service
            )                                                                                   AS diagnosis_code,
        diagnosis_code_qual,
        diagnosis_priority,
        admit_diagnosis_ind,
        procedure_code,
        procedure_code_qual,
        principal_proc_ind,
        procedure_units_billed,
        procedure_modifier_1,
        procedure_modifier_2,
        procedure_modifier_3,
        procedure_modifier_4,
        revenue_code,
        ndc_code,
        medical_coverage_type,
        line_charge,
        line_allowed,
        total_charge,
        total_allowed,
        prov_rendering_npi,
        prov_billing_npi,
        prov_referring_npi,
        prov_facility_npi,
        payer_name,
        payer_plan_id,
        prov_rendering_state_license,
        prov_rendering_upin,
        prov_rendering_name_1,
        prov_rendering_std_taxonomy,
        prov_rendering_vendor_specialty,
        prov_billing_tax_id,
        prov_billing_ssn,
        prov_billing_state_license,
        prov_billing_upin,
        prov_billing_name_1,
        prov_billing_address_1,
        prov_billing_address_2,
        prov_billing_city,
        prov_billing_state,
        prov_billing_zip,
        prov_billing_std_taxonomy,
        prov_referring_name_1,
        prov_facility_state_license,
        prov_facility_name_1,
        prov_facility_address_1,
        prov_facility_address_2,
        prov_facility_city,
        prov_facility_state,
        prov_facility_zip,
        medical_claim_link_text,
        part_provider,
        part_best_date
     FROM waystar_norm01_norm_lines
    /* Select where the claim is not professional, or there is no date_service, */
    /* or there is no date_service_end, or the two dates are the same, or the */
    /* two dates are more than a year apart. */
    WHERE COALESCE(claim_type, 'X') <> 'P'
       OR date_service IS NULL
       OR date_service_end IS NULL
       OR DATEDIFF
            (
                COALESCE(date_service_end, CAST('1900-01-01' AS DATE)),
                COALESCE(date_service, CAST('1900-01-01' AS DATE))
            ) = 0
       OR DATEDIFF
            (
                COALESCE(date_service_end, CAST('1900-01-01' AS DATE)),
                COALESCE(date_service, CAST('1900-01-01' AS DATE))
            ) > 365
    --------------------------------------------------------------------------------------------
    ---- 3.1  line-level data that NEEDS to have the dates EXPLODED
    --------------------------------------------------------------------------------------------
    UNION ALL
    SELECT
        claim_id,
        hvid,
        created,
        model_version,
        data_set,
        data_feed,
        data_vendor,
        patient_gender,
        patient_age,
        patient_year_of_birth,
        patient_zip3,
        patient_state,
        claim_type,
        date_received,
        date_service,
        date_service_end,
        inst_date_admitted,
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
        procedure_units_billed,
        procedure_modifier_1,
        procedure_modifier_2,
        procedure_modifier_3,
        procedure_modifier_4,
        revenue_code,
        ndc_code,
        medical_coverage_type,
        line_charge,
        line_allowed,
        total_charge,
        total_allowed,
        prov_rendering_npi,
        prov_billing_npi,
        prov_referring_npi,
        prov_facility_npi,
        payer_name,
        payer_plan_id,
        prov_rendering_state_license,
        prov_rendering_upin,
        prov_rendering_name_1,
        prov_rendering_std_taxonomy,
        prov_rendering_vendor_specialty,
        prov_billing_tax_id,
        prov_billing_ssn,
        prov_billing_state_license,
        prov_billing_upin,
        prov_billing_name_1,
        prov_billing_address_1,
        prov_billing_address_2,
        prov_billing_city,
        prov_billing_state,
        prov_billing_zip,
        prov_billing_std_taxonomy,
        prov_referring_name_1,
        prov_facility_state_license,
        prov_facility_name_1,
        prov_facility_address_1,
        prov_facility_address_2,
        prov_facility_city,
        prov_facility_state,
        prov_facility_zip,
        medical_claim_link_text,
        part_provider,
        part_best_date
     FROM
    (
        SELECT
            claim_id,
            hvid,
            created,
            model_version,
            data_set,
            data_feed,
            data_vendor,
            patient_gender,
            patient_age,
            patient_year_of_birth,
            patient_zip3,
            patient_state,
            claim_type,
            date_received,
            --service_from,
            DATE_ADD (sln.date_service,dtexplode.i)   as date_service,
            --service_to,
            DATE_ADD (sln.date_service,dtexplode.i)   as date_service_end,

            inst_date_admitted,
            inst_admit_type_std_id,
            inst_admit_source_std_id,
            inst_discharge_status_std_id,
            inst_type_of_bill_std_id,
            inst_drg_std_id,
            place_of_service_std_id,
            service_line_number,
            /* diagnosis_code */
            /* We left the privacy filtering to the final normalization */
            /* so we can accurately add the claim-level diagnoses.      */
            CLEAN_UP_DIAGNOSIS_CODE
                (
                    diagnosis_code,
                    diagnosis_code_qual,
                    date_service
                )                                                                                   AS diagnosis_code,
            diagnosis_code_qual,
            diagnosis_priority,
            admit_diagnosis_ind,
            procedure_code,
            procedure_code_qual,
            principal_proc_ind,
            procedure_units_billed,
            procedure_modifier_1,
            procedure_modifier_2,
            procedure_modifier_3,
            procedure_modifier_4,
            revenue_code,
            ndc_code,
            medical_coverage_type,
            line_charge,
            line_allowed,
            total_charge,
            total_allowed,
            prov_rendering_npi,
            prov_billing_npi,
            prov_referring_npi,
            prov_facility_npi,
            payer_name,
            payer_plan_id,
            prov_rendering_state_license,
            prov_rendering_upin,
            prov_rendering_name_1,
            prov_rendering_std_taxonomy,
            prov_rendering_vendor_specialty,
            prov_billing_tax_id,
            prov_billing_ssn,
            prov_billing_state_license,
            prov_billing_upin,
            prov_billing_name_1,
            prov_billing_address_1,
            prov_billing_address_2,
            prov_billing_city,
            prov_billing_state,
            prov_billing_zip,
            prov_billing_std_taxonomy,
            prov_referring_name_1,
            prov_facility_state_license,
            prov_facility_name_1,
            prov_facility_address_1,
            prov_facility_address_2,
            prov_facility_city,
            prov_facility_state,
            prov_facility_zip,
            medical_claim_link_text,
            part_provider,
            /* part_best_date */
            CASE
                WHEN CAP_DATE
                        (
                            DATE_ADD (sln.date_service,dtexplode.i) ,
                            CAST('2015-01-01' AS DATE), -- To avoid memory issues.
                            -- ahdt.gen_ref_1_dt,
                            CAST('{VDR_FILE_DT}' AS DATE)
                        ) IS NULL
                     THEN '0_PREDATES_HVM_HISTORY'
                ELSE CONCAT
                        (
                            SUBSTR(CAST(DATE_ADD (sln.date_service,dtexplode.i)  AS STRING), 1, 7), '-01'
                        )
            END 																					AS part_best_date
        FROM waystar_norm01_norm_lines sln
        lateral view
        posexplode(split(space(datediff(sln.date_service_end,sln.date_service)),' ')) dtexplode as i,x
        WHERE
             COALESCE(sln.claim_type, 'X') = 'P'
        AND sln.date_service IS NOT NULL
        AND sln.date_service_end IS NOT NULL
        AND DATEDIFF
            (
                COALESCE(sln.date_service_end, CAST('1900-01-01' AS DATE)),
                COALESCE(sln.date_service, CAST('1900-01-01' AS DATE))
            ) <> 0
        AND DATEDIFF
            (
                COALESCE(sln.date_service_end, CAST('1900-01-01' AS DATE)),
                COALESCE(sln.date_service, CAST('1900-01-01' AS DATE))
            ) <= 365
        AND  DATE_ADD (sln.date_service,dtexplode.i) <= COALESCE(sln.date_service_end, CAST('1900-01-01' AS DATE))
    )
    --------------------------------------------------------------------------------------------
    ---- 3.1  claim-level data that DOES NOT need to have the dates exploded
    --------------------------------------------------------------------------------------------
    UNION ALL
    SELECT
        claim_id,
        hvid,
        created,
        model_version,
        data_set,
        data_feed,
        data_vendor,
        patient_gender,
        patient_age,
        patient_year_of_birth,
        patient_zip3,
        patient_state,
        claim_type,
        date_received,
        /* date_service */
        /* For professional claims, if date_service is NULL, set it to date_service_end. */
        CASE
            WHEN COALESCE(claim_type, 'X') = 'P'
                THEN COALESCE(date_service, date_service_end)
            ELSE date_service
        END                                                                                     AS date_service,
        /* date_service_end */
        /* For professional claims, if date_service_end is NULL, set it to date_service. */
        CASE
            WHEN COALESCE(claim_type, 'X') = 'P'
                THEN COALESCE(date_service_end, date_service)
            ELSE date_service_end
        END                                                                                     AS date_service_end,
        inst_date_admitted,
        inst_admit_type_std_id,
        inst_admit_source_std_id,
        inst_discharge_status_std_id,
        inst_type_of_bill_std_id,
        inst_drg_std_id,
        place_of_service_std_id,
        service_line_number,
        /* diagnosis_code */
        /* We left the privacy filtering to the final normalization */
        /* so we can accurately add the claim-level diagnoses.      */
        CLEAN_UP_DIAGNOSIS_CODE
            (
                diagnosis_code,
                diagnosis_code_qual,
                date_service
            )                                                                                   AS diagnosis_code,
        diagnosis_code_qual,
        diagnosis_priority,
        admit_diagnosis_ind,
        procedure_code,
        procedure_code_qual,
        principal_proc_ind,
        procedure_units_billed,
        procedure_modifier_1,
        procedure_modifier_2,
        procedure_modifier_3,
        procedure_modifier_4,
        revenue_code,
        ndc_code,
        medical_coverage_type,
        line_charge,
        line_allowed,
        total_charge,
        total_allowed,
        prov_rendering_npi,
        prov_billing_npi,
        prov_referring_npi,
        prov_facility_npi,
        payer_name,
        payer_plan_id,
        prov_rendering_state_license,
        prov_rendering_upin,
        prov_rendering_name_1,
        prov_rendering_std_taxonomy,
        prov_rendering_vendor_specialty,
        prov_billing_tax_id,
        prov_billing_ssn,
        prov_billing_state_license,
        prov_billing_upin,
        prov_billing_name_1,
        prov_billing_address_1,
        prov_billing_address_2,
        prov_billing_city,
        prov_billing_state,
        prov_billing_zip,
        prov_billing_std_taxonomy,
        prov_referring_name_1,
        prov_facility_state_license,
        prov_facility_name_1,
        prov_facility_address_1,
        prov_facility_address_2,
        prov_facility_city,
        prov_facility_state,
        prov_facility_zip,
        medical_claim_link_text,
        part_provider,
        part_best_date
     FROM waystar_norm02_norm_claims
    /* Select where the claim is not professional, or there is no date_service, */
    /* or there is no date_service_end, or the two dates are the same, or the */
    /* two dates are more than a year apart. */
    WHERE COALESCE(claim_type, 'X') <> 'P'
       OR date_service IS NULL
       OR date_service_end IS NULL
       OR DATEDIFF
            (
                COALESCE(date_service_end, CAST('1900-01-01' AS DATE)),
                COALESCE(date_service, CAST('1900-01-01' AS DATE))
            ) = 0
       OR DATEDIFF
            (
                COALESCE(date_service_end, CAST('1900-01-01' AS DATE)),
                COALESCE(date_service, CAST('1900-01-01' AS DATE))
            ) > 365

    --------------------------------------------------------------------------------------------
    ---- 3.1  claim-level data that needs to have the DATES EXPLODED
    --------------------------------------------------------------------------------------------
    UNION ALL
    SELECT
        claim_id,
        hvid,
        created,
        model_version,
        data_set,
        data_feed,
        data_vendor,
        patient_gender,
        patient_age,
        patient_year_of_birth,
        patient_zip3,
        patient_state,
        claim_type,
        date_received,
        date_service,
        date_service_end,
        inst_date_admitted,
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
        procedure_units_billed,
        procedure_modifier_1,
        procedure_modifier_2,
        procedure_modifier_3,
        procedure_modifier_4,
        revenue_code,
        ndc_code,
        medical_coverage_type,
        line_charge,
        line_allowed,
        total_charge,
        total_allowed,
        prov_rendering_npi,
        prov_billing_npi,
        prov_referring_npi,
        prov_facility_npi,
        payer_name,
        payer_plan_id,
        prov_rendering_state_license,
        prov_rendering_upin,
        prov_rendering_name_1,
        prov_rendering_std_taxonomy,
        prov_rendering_vendor_specialty,
        prov_billing_tax_id,
        prov_billing_ssn,
        prov_billing_state_license,
        prov_billing_upin,
        prov_billing_name_1,
        prov_billing_address_1,
        prov_billing_address_2,
        prov_billing_city,
        prov_billing_state,
        prov_billing_zip,
        prov_billing_std_taxonomy,
        prov_referring_name_1,
        prov_facility_state_license,
        prov_facility_name_1,
        prov_facility_address_1,
        prov_facility_address_2,
        prov_facility_city,
        prov_facility_state,
        prov_facility_zip,
        medical_claim_link_text,
        part_provider,
        part_best_date
     FROM
    (
        SELECT
            claim_id,
            hvid,
            created,
            model_version,
            data_set,
            data_feed,
            data_vendor,
            patient_gender,
            patient_age,
            patient_year_of_birth,
            patient_zip3,
            patient_state,
            claim_type,
            date_received,
            --service_from,
            DATE_ADD (clm.date_service,dtexplode.i)   as date_service,
            --service_to,
            DATE_ADD (clm.date_service,dtexplode.i)   as date_service_end,

            inst_date_admitted,
            inst_admit_type_std_id,
            inst_admit_source_std_id,
            inst_discharge_status_std_id,
            inst_type_of_bill_std_id,
            inst_drg_std_id,
            place_of_service_std_id,
            service_line_number,
            /* diagnosis_code */
            /* We left the privacy filtering to the final normalization */
            /* so we can accurately add the claim-level diagnoses.      */
            CLEAN_UP_DIAGNOSIS_CODE
                (
                    diagnosis_code,
                    diagnosis_code_qual,
                    date_service
                )                                                                                   AS diagnosis_code,
            diagnosis_code_qual,
            diagnosis_priority,
            admit_diagnosis_ind,
            procedure_code,
            procedure_code_qual,
            principal_proc_ind,
            procedure_units_billed,
            procedure_modifier_1,
            procedure_modifier_2,
            procedure_modifier_3,
            procedure_modifier_4,
            revenue_code,
            ndc_code,
            medical_coverage_type,
            line_charge,
            line_allowed,
            total_charge,
            total_allowed,
            prov_rendering_npi,
            prov_billing_npi,
            prov_referring_npi,
            prov_facility_npi,
            payer_name,
            payer_plan_id,
            prov_rendering_state_license,
            prov_rendering_upin,
            prov_rendering_name_1,
            prov_rendering_std_taxonomy,
            prov_rendering_vendor_specialty,
            prov_billing_tax_id,
            prov_billing_ssn,
            prov_billing_state_license,
            prov_billing_upin,
            prov_billing_name_1,
            prov_billing_address_1,
            prov_billing_address_2,
            prov_billing_city,
            prov_billing_state,
            prov_billing_zip,
            prov_billing_std_taxonomy,
            prov_referring_name_1,
            prov_facility_state_license,
            prov_facility_name_1,
            prov_facility_address_1,
            prov_facility_address_2,
            prov_facility_city,
            prov_facility_state,
            prov_facility_zip,
            medical_claim_link_text,
            part_provider,
            /* part_best_date */
            CASE
                WHEN CAP_DATE
                        (
                            DATE_ADD (clm.date_service,dtexplode.i) ,
                            CAST('2015-01-01' AS DATE), -- To avoid memory issues.
                            -- ahdt.gen_ref_1_dt,
                            CAST('{VDR_FILE_DT}' AS DATE)
                        ) IS NULL
                     THEN '0_PREDATES_HVM_HISTORY'
                ELSE CONCAT
                        (
                            SUBSTR(CAST(DATE_ADD (clm.date_service,dtexplode.i)  AS STRING), 1, 7), '-01'
                        )
            END 																					AS part_best_date
        FROM waystar_norm02_norm_claims clm
        lateral view
        posexplode(split(space(datediff(clm.date_service_end,clm.date_service)),' ')) dtexplode as i,x
        WHERE
            COALESCE(clm.claim_type, 'X') = 'P'
            AND clm.date_service IS NOT NULL
            AND clm.date_service_end IS NOT NULL
            AND DATEDIFF
                (
                    COALESCE(clm.date_service_end, CAST('1900-01-01' AS DATE)),
                    COALESCE(clm.date_service, CAST('1900-01-01' AS DATE))
                ) <> 0
            AND DATEDIFF
                (
                    COALESCE(clm.date_service_end, CAST('1900-01-01' AS DATE)),
                    COALESCE(clm.date_service, CAST('1900-01-01' AS DATE))
                ) <= 365
            AND DATE_ADD(clm.date_service,dtexplode.i) <= COALESCE(clm.date_service_end, CAST('1900-01-01' AS DATE))

    )
)