SELECT DISTINCT
    clm.entity_id                                                          AS claimid,
    clm.record_type                                                        AS recordtype,
     CAST(EXTRACT_DATE(CONCAT('20',clm.create_date), '%Y%m%d') AS DATE)  AS receiveddate,
    'P'                                                                    AS claimtype,
    CAST(NULL AS STRING)                                                   AS dvtoken1,
    CAST(NULL AS STRING)                                                   AS dvtoken2,
    CAST(NULL AS STRING)                                                   AS dvtoken2zip3,
    CASE
        WHEN pay.hvid IS NOT NULL THEN pay.hvid
    ELSE NULL
    END                                                                    AS hvid,
    CAST(NULL AS boolean)                                                  AS hvidreview,
    VALIDATE_STATE_CODE(UPPER(COALESCE(clm.patient_state, pay.state, ''))) AS patientstate,
    MASK_ZIP_CODE(SUBSTR(COALESCE(clm.patient_zip_code, pay.threedigitzip), 1, 3))  AS patientzip3,
    ----------------------------------------------------------------------------------------------------------------------
    -- patientrelationshipcode
    ----------------------------------------------------------------------------------------------------------------------
    CASE
        WHEN COALESCE(clm.patient_relationship_to_insured, clm.secondary_payer_patient_relationship_to_insured,  clm.teritary_payer_patient_relationship_to_insured) = '18' THEN '18' --'SELF'
        WHEN COALESCE(clm.patient_relationship_to_insured, clm.secondary_payer_patient_relationship_to_insured,  clm.teritary_payer_patient_relationship_to_insured) = '01' THEN '01' --'SPOUSE'
        WHEN COALESCE(clm.patient_relationship_to_insured, clm.secondary_payer_patient_relationship_to_insured,  clm.teritary_payer_patient_relationship_to_insured) = '19' THEN '19' -- 'CHILD'
    ELSE COALESCE(clm.patient_relationship_to_insured, clm.secondary_payer_patient_relationship_to_insured,  clm.teritary_payer_patient_relationship_to_insured)
    END                                                                    AS patientrelationshipcode,
    ----------------------------------------------------------------------------------------------------------------------
    -- patientgender
    ----------------------------------------------------------------------------------------------------------------------
	(
    	CASE
    	    WHEN clm.patient_sex_ IS NULL AND pay.gender IS NULL THEN NULL
    	    WHEN SUBSTR(UPPER(clm.patient_sex_), 1, 1) IN ('F', 'M', 'U') THEN SUBSTR(UPPER(clm.patient_sex_), 1, 1)
    	    WHEN SUBSTR(UPPER(pay.gender     ), 1, 1) IN ('F', 'M', 'U') THEN SUBSTR(UPPER(pay.gender     ), 1, 1)
    	    ELSE 'U'
    	END
    )                                                                      AS patientgender,
    ----------------------------------------------------------------------------------------------------------------------
    -- patientyob
    ----------------------------------------------------------------------------------------------------------------------
	CAP_YEAR_OF_BIRTH  -- Cap year of birth 1927 if age is 85 and over
        (
            NULL,
            CAST(EXTRACT_DATE(sln.service_to_date, '%Y%m%d' ) AS DATE),
            pay.yearofbirth
        )                                                                  AS patientyob,
    ----------------------------------------------------------------------------------------------------------------------
    -- BILLING PROVIDER NPI
    ----------------------------------------------------------------------------------------------------------------------
    CASE
        WHEN LPAD(sln.place_of_service, 2, '0') IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99') THEN NULL
    ELSE CLEAN_UP_NPI_CODE(clm.billing_prov_npi)
    END                                                                    AS billingprovidernpi,
    ----------------------------------------------------------------------------------------------------------------------
    -- billingprovidername1
    ----------------------------------------------------------------------------------------------------------------------
    CASE
        WHEN LPAD(sln.place_of_service, 2, '0') IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99') THEN NULL
    ELSE clm.billing_prov_first_name
    END                                                                    AS billingprovidername1,
    ----------------------------------------------------------------------------------------------------------------------
    -- billingprovidername2
    ----------------------------------------------------------------------------------------------------------------------
    CASE
        WHEN LPAD(sln.place_of_service, 2, '0') IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99') THEN NULL
    ELSE clm.billing_prov_last_name
    END                                                                    AS billingprovidername2,
    ----------------------------------------------------------------------------------------------------------------------
    -- billingprovideraddress1
    ----------------------------------------------------------------------------------------------------------------------
    CASE
        WHEN LPAD(sln.place_of_service, 2, '0') IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99') THEN NULL
    ELSE clm.billing_providers_address_1
    END                                                                    AS billingprovideraddress1,
    ----------------------------------------------------------------------------------------------------------------------
    -- billingprovideraddress2
    ----------------------------------------------------------------------------------------------------------------------
    CASE
        WHEN LPAD(sln.place_of_service, 2, '0') IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99') THEN NULL
    ELSE clm.billing_providers_address_2
    END                                                                    AS billingprovideraddress2,
    ----------------------------------------------------------------------------------------------------------------------
    -- city
    ----------------------------------------------------------------------------------------------------------------------
    CASE
        WHEN LPAD(sln.place_of_service, 2, '0') IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99') THEN NULL
    ELSE clm.billing_providers_city
    END                                                                    AS billingprovidercity,
    ----------------------------------------------------------------------------------------------------------------------
    -- state
    ----------------------------------------------------------------------------------------------------------------------
    CASE
        WHEN LPAD(sln.place_of_service, 2, '0') IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99') THEN NULL
    ELSE validate_state_code(clm.billing_providers_state)
    END                                                                    AS billingproviderstate,
    ----------------------------------------------------------------------------------------------------------------------
    -- zip
    ----------------------------------------------------------------------------------------------------------------------
    CASE
        WHEN LPAD(sln.place_of_service, 2, '0') IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99') THEN NULL
    ELSE clm.billing_providers_zip
    END                                                                    AS billingproviderzip,
    ----------------------------------------------------------------------------------------------------------------------
    -- REFERRING PROVIDER NPI
    ----------------------------------------------------------------------------------------------------------------------
    CASE
        WHEN  LPAD(sln.place_of_service, 2, '0')  IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99') THEN NULL
    ELSE CLEAN_UP_NPI_CODE(referring_prov_npi_)
    END                                                                    AS referringprovidernpi,
    ----------------------------------------------------------------------------------------------------------------------
    -- billingprovidertaxonomy
    ----------------------------------------------------------------------------------------------------------------------
    CASE
        WHEN  LPAD(sln.place_of_service, 2, '0')  IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99') THEN NULL
    ELSE clm.billing_or_pay_to_provider_taxonomy_code
    END                                                                    AS billingprovidertaxonomy,
    ----------------------------------------------------------------------------------------------------------------------
    -- referringprovidername
    ----------------------------------------------------------------------------------------------------------------------
    CASE
      WHEN LPAD(sln.place_of_service, 2, '0')  IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')          THEN NULL
      WHEN clm.referring_provider_first_name IS NOT NULL AND clm.referring_provider_last_name IS NOT NULL THEN CONCAT(clm.referring_provider_first_name,', ',clm.referring_provider_last_name)
      WHEN clm.referring_provider_first_name IS     NULL AND clm.referring_provider_last_name IS NOT NULL THEN clm.referring_provider_last_name
      WHEN clm.referring_provider_first_name IS NOT NULL AND clm.referring_provider_last_name IS     NULL THEN clm.referring_provider_first_name
    ELSE NULL
    END                                                                    AS referringprovidername,
    ----------------------------------------------------------------------------------------------------------------------
    -- renderingprovidernpi
    ----------------------------------------------------------------------------------------------------------------------
    CASE
        WHEN LPAD(sln.place_of_service, 2, '0')  IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')          THEN NULL
    ELSE CLEAN_UP_NPI_CODE(clm.rendering_provider_npi)
    END                                                                    AS renderingprovidernpi,
    ----------------------------------------------------------------------------------------------------------------------
    -- renderingprovidername1
    ----------------------------------------------------------------------------------------------------------------------
    CASE
        WHEN LPAD(sln.place_of_service, 2, '0')  IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')          THEN NULL
        WHEN clm.rendering_provider_first IS NOT NULL  THEN clm.rendering_provider_first
    ELSE NULL
    END                                                                    AS renderingprovidername1,
    ----------------------------------------------------------------------------------------------------------------------
    -- renderingprovidername2
    ----------------------------------------------------------------------------------------------------------------------
    CASE
        WHEN LPAD(sln.place_of_service, 2, '0')  IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')          THEN NULL
        WHEN clm.rendering_provider_last IS NOT NULL  THEN  clm.rendering_provider_last
    ELSE NULL
    END                                                                    AS renderingprovidername2,
    ----------------------------------------------------------------------------------------------------------------------
    -- renderingproviderspecialtycode
    ----------------------------------------------------------------------------------------------------------------------
    CASE
        WHEN LPAD(sln.place_of_service, 2, '0')  IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')          THEN NULL
    ELSE clm.rendering_provider_specialty_code
    END                                                                    AS renderingproviderspecialtycode,
    ----------------------------------------------------------------------------------------------------------------------
    -- facilityname
    ----------------------------------------------------------------------------------------------------------------------
    CASE
        WHEN LPAD(sln.place_of_service, 2, '0')  IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')          THEN NULL
    ELSE clm.facility_laboratory_name
    END                                                                    AS facilityname,
    ----------------------------------------------------------------------------------------------------------------------
    -- facility_lab_npi
    ----------------------------------------------------------------------------------------------------------------------
    CASE
        WHEN LPAD(sln.place_of_service, 2, '0')  IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')          THEN NULL
    ELSE CLEAN_UP_NPI_CODE(clm.facility_lab_npi)
    END                                                                    AS facilitynpi,
    ----------------------------------------------------------------------------------------------------------------------
    -- facilityaddress1
    ----------------------------------------------------------------------------------------------------------------------
    CASE
        WHEN LPAD(sln.place_of_service, 2, '0')  IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')          THEN NULL
    ELSE clm.facility_laboratory_street_address_1
    END                                                                    AS facilityaddress1,
    ----------------------------------------------------------------------------------------------------------------------
    -- facilityaddress2
    ----------------------------------------------------------------------------------------------------------------------
    CASE
        WHEN LPAD(sln.place_of_service, 2, '0')  IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')          THEN NULL
    ELSE clm.facility_laboratory_street_address_2
    END                                                                    AS facilityaddress2,
    ----------------------------------------------------------------------------------------------------------------------
    -- facilitycity
    ----------------------------------------------------------------------------------------------------------------------
    CASE
        WHEN LPAD(sln.place_of_service, 2, '0')  IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')          THEN NULL
    ELSE clm.facility_laboratory_city
    END                                                                    AS facilitycity,
    ----------------------------------------------------------------------------------------------------------------------
    -- facilitystate
    ----------------------------------------------------------------------------------------------------------------------
    CASE
        WHEN LPAD(sln.place_of_service, 2, '0')  IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')          THEN NULL
    ELSE validate_state_code(clm.facility_laboratory_state)
    END                                                                    AS facilitystate,
    ----------------------------------------------------------------------------------------------------------------------
    -- facilityzip
    ----------------------------------------------------------------------------------------------------------------------
    CASE
        WHEN LPAD(sln.place_of_service, 2, '0')  IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99')          THEN NULL
    ELSE clm.facility_laboratory_zip_code
    END                                                                    AS facilityzip,
    ----------------------------------------------------------------------------------------------------------------------
    -- statementstartdate
    ----------------------------------------------------------------------------------------------------------------------
    COALESCE
    (
        CAST(EXTRACT_DATE(sln_date.min_service_from   , '%Y%m%d' ) AS DATE),
        CAST(EXTRACT_DATE(CONCAT('20',clm.create_date), '%Y%m%d' ) AS DATE)
    )                                                                      AS statementstartdate,
    ----------------------------------------------------------------------------------------------------------------------
    -- statementenddate
    ----------------------------------------------------------------------------------------------------------------------
    COALESCE
    (
        CAST(EXTRACT_DATE(sln_date.max_service_to     , '%Y%m%d' ) AS DATE),
        CAST(EXTRACT_DATE(CONCAT('20',clm.create_date), '%Y%m%d' ) AS DATE)
    )                                                                     AS statementenddate,

    CAST(clm.total_claim_charge_amount AS FLOAT)                          AS claimcharges,
    CAST(NULL AS STRING)                                                  AS billtypecode,
    CASE
        WHEN COALESCE
                (
                      source_of_payment
                    , clm.secondary_payer_source_of_payment
                    , clm.teritary_payer_source_of_payment
                ) IS NULL
            THEN NULL
        ELSE SUBSTR
                (
                    CONCAT
                        (
                            ' | PAYER_1: ',source_of_payment,
                            CASE
                                WHEN secondary_payer_source_of_payment IS NULL THEN ''
                                ELSE CONCAT
                                        (
                                            ' | PAYER_2: ',secondary_payer_source_of_payment
                                        )
                            END,
                            CASE
                                WHEN teritary_payer_source_of_payment IS NULL THEN ''
                                ELSE CONCAT
                                        (
                                            ' | PAYER_3: ', teritary_payer_source_of_payment
                                        )
                            END
                        ), 4
                )
    END                                                                     AS claimfilingindicatorcode,
    CASE
        WHEN COALESCE
                (
                     clm.work_related_indicator
                    ,clm.auto_accident_indicator
                    ,clm.other_accident_indicator
                ) IS NULL
            THEN NULL
        ELSE SUBSTR
                (
                    CONCAT
                        (
                            ' | INCIDENT_1: ', clm.work_related_indicator,
                            CASE
                                WHEN clm.auto_accident_indicator IS NULL THEN ''
                                ELSE CONCAT
                                        (
                                            ' | INCIDENT_2: ', clm.auto_accident_indicator
                                        )
                            END,
                            CASE
                                WHEN clm.other_accident_indicator IS NULL THEN ''
                                ELSE CONCAT
                                        (
                                            ' | INCIDENT_3: ', clm.other_accident_indicator
                                        )
                            END
                        ), 4
                )
    END                                                                         AS accidentrelatedindicator,
    CAST(EXTRACT_DATE(clm.admission_date, '%Y%m%d' )  AS DATE)                  AS admissiondate,
    CAST(NULL AS STRING)                                                        AS admissiontypecode,
    CAST(NULL AS STRING)                                                        AS admissionsourcecode,
    CAST(NULL AS STRING)                                                        AS patientstatuscode,
    CAST(NULL AS STRING)                                                        AS drg,
    ----------------------------------------------------------------------------------------------------------------------
    -- icdprocedurecode1
    ----------------------------------------------------------------------------------------------------------------------
    CLEAN_UP_ALPHANUMERIC_CODE
        (
            UPPER
                (
                    COALESCE
                            (
                                std_chg_line_hcpcs_procedure_code,
                                dme_chg_line_hcpcs_procedure_code
                            )
                )
        )
                                                                                AS icdprocedurecode1,
    ----------------------------------------------------------------------------------------------------------------------
    -- diagnosiscode
    ----------------------------------------------------------------------------------------------------------------------
    CLEAN_UP_DIAGNOSIS_CODE(clm.diagnosis_code_1, '02',  sln.service_from_date) AS principaldiagnosiscode,
    CLEAN_UP_DIAGNOSIS_CODE(clm.diagnosis_code_2, '02',  sln.service_from_date) AS secondarydiagnosiscode1,
    CLEAN_UP_DIAGNOSIS_CODE(clm.diagnosis_code_3, '02',  sln.service_from_date) AS secondarydiagnosiscode2,
    CLEAN_UP_DIAGNOSIS_CODE(clm.diagnosis_code_4, '02',  sln.service_from_date) AS secondarydiagnosiscode3,
    CLEAN_UP_DIAGNOSIS_CODE(clm.diagnosis_code_5, '02',  sln.service_from_date) AS secondarydiagnosiscode4,
    CLEAN_UP_DIAGNOSIS_CODE(clm.diagnosis_code_6, '02',  sln.service_from_date) AS secondarydiagnosiscode5,
    CLEAN_UP_DIAGNOSIS_CODE(clm.diagnosis_code_7, '02',  sln.service_from_date) AS secondarydiagnosiscode6,
    CLEAN_UP_DIAGNOSIS_CODE(clm.diagnosis_code_8, '02',  sln.service_from_date) AS secondarydiagnosiscode7,
    CAST(NULL AS STRING)                                                        AS secondarydiagnosiscode8,
    CAST(NULL AS STRING)                                                        AS secondarydiagnosiscode9,
    CAST(NULL AS STRING)                                                        AS secondarydiagnosiscode10,
    CAST(NULL AS STRING)                                                        AS secondarydiagnosiscode11,
    CAST(NULL AS STRING)                                                        AS secondarydiagnosiscode12,
    CAST(NULL AS STRING)                                                        AS secondarydiagnosiscode13,
    CAST(NULL AS STRING)                                                        AS secondarydiagnosiscode14,
    CAST(NULL AS STRING)                                                        AS secondarydiagnosiscode15,
    CAST(NULL AS STRING)                                                        AS secondarydiagnosiscode16,
    CAST(NULL AS STRING)                                                        AS secondarydiagnosiscode17,
    CAST(NULL AS STRING)                                                        AS secondarydiagnosiscode18,
    CAST(NULL AS STRING)                                                        AS secondarydiagnosiscode19,
    CAST(NULL AS STRING)                                                        AS secondarydiagnosiscode20,
    CAST(NULL AS STRING)                                                        AS secondarydiagnosiscode21,
    CAST(NULL AS STRING)                                                        AS secondarydiagnosiscode22,
    CAST(NULL AS STRING)                                                        AS secondarydiagnosiscode23,
    CAST(NULL AS STRING)                                                        AS secondarydiagnosiscode24,
    CAST(NULL AS STRING)                                                        AS icdprocedurecode2,
    CAST(NULL AS STRING)                                                        AS icdprocedurecode3,
    CAST(NULL AS STRING)                                                        AS icdprocedurecode4,
    CAST(NULL AS STRING)                                                        AS icdprocedurecode5,
    CAST(NULL AS STRING)                                                        AS icdprocedurecode6,
    CAST(NULL AS STRING)                                                        AS icdprocedurecode7,
    CAST(NULL AS STRING)                                                        AS icdprocedurecode8,
    CAST(NULL AS STRING)                                                        AS icdprocedurecode9,
    CAST(NULL AS STRING)                                                        AS icdprocedurecode10,
    CAST(NULL AS STRING)                                                        AS icdprocedurecode11,
    CAST(NULL AS STRING)                                                        AS icdprocedurecode12,
    CAST(NULL AS STRING)                                                        AS icdprocedurecode13,
    CAST(NULL AS STRING)                                                        AS icdprocedurecode14,
    CAST(NULL AS STRING)                                                        AS icdprocedurecode15,
    CAST(NULL AS STRING)                                                        AS icdprocedurecode16,
    CAST(NULL AS STRING)                                                        AS icdprocedurecode17,
    CAST(NULL AS STRING)                                                        AS icdprocedurecode18,
    CAST(NULL AS STRING)                                                        AS icdprocedurecode19,
    CAST(NULL AS STRING)                                                        AS icdprocedurecode20,
    CAST(NULL AS STRING)                                                        AS icdprocedurecode21,
    CAST(NULL AS STRING)                                                        AS icdprocedurecode22,
    CAST(NULL AS STRING)                                                        AS icdprocedurecode23,
    CAST(NULL AS STRING)                                                        AS icdprocedurecode24,
    CAST(NULL AS STRING)                                                        AS icdprocedurecode25,
    COALESCE(clm.primary_payer_id, sln.payer_primary_id)                        AS	payerid,
    ----------------------------------------------------------------------------------------------------------------------
    -- payername
    ----------------------------------------------------------------------------------------------------------------------
    CASE
        WHEN COALESCE
                (
                  clm.primary_payer_name
                , clm.2nd_payer_name
                , clm.3rd_payer_name
                ) IS NULL
            THEN NULL
        ELSE SUBSTR
                (
                    CONCAT
                        (
                            ' | PAYER_NAME_1: ', clm.primary_payer_name,
                            CASE
                                WHEN clm.2nd_payer_name IS NULL THEN ''
                                ELSE CONCAT
                                        (
                                            ' |  PAYER_NAME_2: ', clm.2nd_payer_name
                                        )
                            END,
                            CASE
                                WHEN clm.3rd_payer_name IS NULL THEN ''
                                ELSE CONCAT
                                        (
                                            ' |  PAYER_NAME_3: ', clm.3rd_payer_name
                                        )
                            END
                        ), 4
                )
    END                                                                   AS payername,
    CAST(NULL AS STRING)                                                  AS codingtype,
    SPLIT(clm.input_file_name, '/')[SIZE(SPLIT(clm.input_file_name, '/')) - 1] AS sourcefilename,
    CAST(NULL AS STRING)                                                  AS datasource,
    'apollo'                                                              AS data_vendor,
    CAST(NULL AS STRING)                                                  AS dhcreceiveddate,
    CAST(NULL AS INT)                                                     AS rownumber,
    CAST(NULL AS INT)                                                     AS fileyear,
    CAST(NULL AS INT)                                                     AS filemonth,
    CAST(NULL AS INT)                                                     AS fileday,
    '26'                                                                  AS data_feed,
    'allscripts'                                                          AS part_provider,
    CASE
        WHEN
            COALESCE
            (
                CAST(EXTRACT_DATE(sln_date.min_service_from   , '%Y%m%d' ) AS DATE),
                CAST(EXTRACT_DATE(CONCAT('20',clm.create_date), '%Y%m%d' ) AS DATE)
            ) IS NULL THEN '0_PREDATES_HVM_HISTORY'
        ELSE
            COALESCE
            (
                CONCAT
                    (
                      SUBSTR(sln_date.min_service_from, 1, 4), '-',
                      SUBSTR(sln_date.min_service_from, 5, 2), '-01'
                    ),
                CONCAT
                    (
                      SUBSTR(CONCAT('20',clm.create_date), 1, 4), '-',
                      SUBSTR(CONCAT('20',clm.create_date), 5, 2), '-01'
                    )
            )
    END                                                                    AS part_best_date

FROM claim clm
LEFT OUTER JOIN serviceline      sln  on clm.entity_id = sln.entity_id AND sln.charge_line_number = 1
LEFT OUTER JOIN matching_payload pay on clm.entity_id = pay.claimid
LEFT OUTER JOIN
(
    SELECT sln_1.entity_id, MIN(sln_1.service_from_date) AS min_service_from, MAX( sln_1.service_to_date) AS max_service_to
    FROM serviceline sln_1
    GROUP BY sln_1.entity_id
) sln_date  ON clm.entity_id = sln_date.entity_id
WHERE clm.entity_id IS NOT NULL
  AND LOWER(clm.entity_id) <> 'entity_id'
ORDER BY 1
