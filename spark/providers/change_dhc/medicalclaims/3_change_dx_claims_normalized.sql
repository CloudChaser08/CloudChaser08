SELECT DISTINCT
    clm.claim_tcn_id                                                                        AS claimid,
    clm.record_type                                                                         AS recordtype,
    TO_DATE(clm.received_date,'yyyyMMdd')                                                   AS receiveddate,
    clm.claim_type                                                                          AS claimtype,
    CAST(NULL AS STRING)                                                                    AS dvtoken1,
    CAST(NULL AS STRING)                                                                    AS dvtoken2,
    CAST(NULL AS STRING)                                                                    AS dvtoken2zip3,
    CASE
        WHEN pay.hvid IS NOT NULL THEN pay.hvid
        ELSE NULL
    END                                                                                     AS hvid,
    CAST(NULL AS BOOLEAN)                                                                      AS hvidreview,
    VALIDATE_STATE_CODE(COALESCE(clm.patient_state, pay.state))                             AS patientstate,
    MASK_ZIP_CODE(COALESCE(clm.patient_zip3, pay.threedigitzip))                            AS patient_zip3,
    clm.patient_relationship_code                                                           AS patientrelationshipcode,
	(
    	CASE
    	    WHEN pln.patient_gender IS NULL AND clm.patient_gender_code IS NULL AND pay.gender IS NULL THEN NULL
    	    WHEN SUBSTR(UPPER(pln.patient_gender     ), 1, 1) IN ('F', 'M', 'U') THEN SUBSTR(UPPER(pln.patient_gender),      1, 1)
    	    WHEN SUBSTR(UPPER(clm.patient_gender_code), 1, 1) IN ('F', 'M', 'U') THEN SUBSTR(UPPER(clm.patient_gender_code), 1, 1)
    	    WHEN SUBSTR(UPPER(pay.gender             ), 1, 1) IN ('F', 'M', 'U') THEN SUBSTR(UPPER(pay.gender             ), 1, 1)
    	    ELSE 'U'
    	END
    )                                                                                       AS patientgender,

-- Previous    COALESCE(clm.patient_birth_year, pay.yearofbirth)                                       AS patientyob,

	CAP_YEAR_OF_BIRTH  -- Cap year of birth 1927 if age is 85 and over
        (
            NULL,
            CAST(EXTRACT_DATE(sln.service_to_date, '%Y%m%d' ) AS DATE),
            pay.yearofbirth
        )                                                                 AS patientyob,

    CASE
        WHEN COALESCE(claim_type, 'X') NOT IN ('I', 'P') THEN NULL
        WHEN COALESCE(claim_type, 'X') = 'P' AND LPAD(clm.bill_type, 2, '0' )  IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99') THEN NULL
        WHEN COALESCE(claim_type, 'X') = 'I' AND SUBSTR(COALESCE(clm.bill_type, '3'), 1, 1) = '3' THEN NULL
        ELSE CLEAN_UP_NPI_CODE(COALESCE(clm.billing_prov_npi, ''))
    END                                                                                     AS billingprovidernpi,
    CASE -- Populating target with billing_prov_org_name, based on org_name, claim_type = 'I' only.
        WHEN COALESCE(claim_type, 'X') NOT IN ('I', 'P') THEN NULL
        WHEN COALESCE(claim_type, 'X') = 'P' AND LPAD(clm.bill_type, 2, '0' )  IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99') THEN NULL
        WHEN COALESCE(claim_type, 'X') = 'I' AND SUBSTR(COALESCE(clm.bill_type, '3'), 1, 1) = '3' THEN NULL
        WHEN COALESCE(claim_type, 'X') = 'I' THEN  clm.billing_prov_org_name
        ELSE NULL
    END                                                                                     AS billingprovidername1,
    CASE -- Populating target with billing_prov_ind_name, based on org_name, claim_type = 'I' only.
        WHEN COALESCE(claim_type, 'X') NOT IN ('I', 'P') THEN NULL
        WHEN COALESCE(claim_type, 'X') = 'P' AND LPAD(clm.bill_type, 2, '0' )  IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99') THEN NULL
        WHEN COALESCE(claim_type, 'X') = 'I' AND SUBSTR(COALESCE(clm.bill_type, '3'), 1, 1) = '3' THEN NULL
        WHEN COALESCE(claim_type, 'X') = 'I' THEN clm.billing_prov_ind_name
        ELSE NULL
    END                                                                                     AS billingprovidername2,
    CASE  -- Populating target with billing_prov_street_address_1, based on org_name, claim_type = 'I' only.
        WHEN COALESCE(claim_type, 'X') NOT IN ('I', 'P') THEN NULL
        WHEN COALESCE(claim_type, 'X') = 'P' AND LPAD(clm.bill_type, 2, '0' )  IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99') THEN NULL
        WHEN COALESCE(claim_type, 'X') = 'I' AND SUBSTR(COALESCE(clm.bill_type, '3'), 1, 1) = '3' THEN NULL
        WHEN COALESCE(claim_type, 'X') = 'I' THEN clm.billing_prov_street_address_1
        ELSE NULL
    END                                                                                     AS billingprovideraddress1,
    CASE  -- Populating target with billing_prov_street_address_2, only, based on org_name, claim_type = 'I'.
        WHEN COALESCE(claim_type, 'X') NOT IN ('I', 'P') THEN NULL
        WHEN COALESCE(claim_type, 'X') = 'P' AND LPAD(clm.bill_type, 2, '0' )  IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99') THEN NULL
        WHEN COALESCE(claim_type, 'X') = 'I' AND SUBSTR(COALESCE(clm.bill_type, '3'), 1, 1) = '3' THEN NULL
        WHEN COALESCE(claim_type, 'X') = 'I' THEN clm.billing_prov_street_address_2
        ELSE NULL
    END                                                                                     AS billingprovideraddress2,
    CASE   -- Populating target with billing_prov_city, only, based on org_name, claim_type = 'I'.
        WHEN COALESCE(claim_type, 'X') NOT IN ('I', 'P') THEN NULL
        WHEN COALESCE(claim_type, 'X') = 'P' AND LPAD(clm.bill_type, 2, '0' )  IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99') THEN NULL
        WHEN COALESCE(claim_type, 'X') = 'I' AND SUBSTR(COALESCE(clm.bill_type, '3'), 1, 1) = '3' THEN NULL
        WHEN COALESCE(claim_type, 'X') = 'I' THEN clm.billing_prov_city
        ELSE NULL
    END                                                                                     AS billingprovidercity,
    CASE   -- Populating target with billing_prov_city, only, based on org_name, claim_type = 'I'.
        WHEN COALESCE(claim_type, 'X') NOT IN ('I', 'P') THEN NULL
        WHEN COALESCE(claim_type, 'X') = 'P' AND LPAD(clm.bill_type, 2, '0' )  IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99') THEN NULL
        WHEN COALESCE(claim_type, 'X') = 'I' AND SUBSTR(COALESCE(clm.bill_type, '3'), 1, 1) = '3' THEN NULL
        WHEN COALESCE(claim_type, 'X') = 'I' THEN clm.billing_prov_state
        ELSE NULL
    END                                                                                     AS billingproviderstate,
    CASE   -- Populating target with billing_prov_zip, only, based on org_name, claim_type = 'I'.
        WHEN COALESCE(claim_type, 'X') NOT IN ('I', 'P') THEN NULL
        WHEN COALESCE(claim_type, 'X') = 'P' AND LPAD(clm.bill_type, 2, '0' )  IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99') THEN NULL
        WHEN COALESCE(claim_type, 'X') = 'I' AND SUBSTR(COALESCE(clm.bill_type, '3'), 1, 1) = '3' THEN NULL
        WHEN COALESCE(claim_type, 'X') = 'I' THEN clm.billing_prov_zip
        ELSE NULL
    END                                                                                     AS billingproviderzip,
    CASE
        WHEN COALESCE(claim_type, 'X') NOT IN ('I', 'P') THEN NULL
        WHEN COALESCE(claim_type, 'X') = 'P' AND LPAD(clm.bill_type, 2, '0' )  IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99') THEN NULL
        WHEN COALESCE(claim_type, 'X') = 'I' AND SUBSTR(COALESCE(clm.bill_type, '3'), 1, 1) = '3' THEN NULL
        ELSE CLEAN_UP_NUMERIC_CODE(clm.referring_prov_npi)
    END                                                                                     AS referringprovidernpi,
    clm.billing_pr_taxonomy                                                                 AS billingprovidertaxonomy,
    CASE
        WHEN COALESCE(claim_type, 'X') NOT IN ('I', 'P') THEN NULL
        WHEN COALESCE(claim_type, 'X') = 'P' AND LPAD(clm.bill_type, 2, '0' )  IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99') THEN NULL
        WHEN COALESCE(claim_type, 'X') = 'I' AND SUBSTR(COALESCE(clm.bill_type, '3'), 1, 1) = '3' THEN NULL
        ELSE clm.referring_prov_name
    END                                                                                     AS referringprovidername,
    CASE
        WHEN COALESCE(claim_type, 'X') NOT IN ('I', 'P') THEN NULL
        WHEN COALESCE(claim_type, 'X') = 'P' AND LPAD(clm.bill_type, 2, '0' )  IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99') THEN NULL
        WHEN COALESCE(claim_type, 'X') = 'I' AND SUBSTR(COALESCE(clm.bill_type, '3'), 1, 1) = '3' THEN NULL
        ELSE CLEAN_UP_NUMERIC_CODE(clm.rendering_attending_prov_npi)
    END                                                                                     AS renderingprovidernpi,
    CASE
        WHEN COALESCE(claim_type, 'X') NOT IN ('I', 'P') THEN NULL
        WHEN COALESCE(claim_type, 'X') = 'P' AND LPAD(clm.bill_type, 2, '0' )  IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99') THEN NULL
        WHEN COALESCE(claim_type, 'X') = 'I' AND SUBSTR(COALESCE(clm.bill_type, '3'), 1, 1) = '3' THEN NULL
        ELSE clm.rendering_prov_org_name
    END                                                                                     AS renderingprovidername1,
    clm.rendering_attending_prov_ind_name                                                   AS renderingprovidername2,
    clm.rendering_attending_prov_taxonomy                                                   AS renderingproviderspecialtycode,
    CASE
        WHEN COALESCE(claim_type, 'X') NOT IN ('I', 'P') THEN NULL
        WHEN COALESCE(claim_type, 'X') = 'P' AND LPAD(clm.bill_type, 2, '0' )  IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99') THEN NULL
        WHEN COALESCE(claim_type, 'X') = 'I' AND SUBSTR(COALESCE(clm.bill_type, '3'), 1, 1) = '3' THEN NULL
        ELSE clm.facility_name
    END                                                                                     AS facilityname,
    CASE
        WHEN COALESCE(claim_type, 'X') NOT IN ('I', 'P') THEN NULL
        WHEN COALESCE(claim_type, 'X') = 'P' AND LPAD(clm.bill_type, 2, '0' )  IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99') THEN NULL
        WHEN COALESCE(claim_type, 'X') = 'I' AND SUBSTR(COALESCE(clm.bill_type, '3'), 1, 1) = '3' THEN NULL
        ELSE CLEAN_UP_NUMERIC_CODE(clm.facility_npi)
    END                                                                                     AS facilitynpi,
    CASE
        WHEN COALESCE(claim_type, 'X') NOT IN ('I', 'P') THEN NULL
        WHEN COALESCE(claim_type, 'X') = 'P' AND LPAD(clm.bill_type, 2, '0' )  IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99') THEN NULL
        WHEN COALESCE(claim_type, 'X') = 'I' AND SUBSTR(COALESCE(clm.bill_type, '3'), 1, 1) = '3' THEN NULL
        ELSE clm.facility_street_address_1
    END                                                                                     AS facilityaddress1,
    CASE
        WHEN COALESCE(claim_type, 'X') NOT IN ('I', 'P') THEN NULL
        WHEN COALESCE(claim_type, 'X') = 'P' AND LPAD(clm.bill_type, 2, '0' )  IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99') THEN NULL
        WHEN COALESCE(claim_type, 'X') = 'I' AND SUBSTR(COALESCE(clm.bill_type, '3'), 1, 1) = '3' THEN NULL
        ELSE clm.facility_street_address_2
    END                                                                                     AS facilityaddress2,
    CASE
        WHEN COALESCE(claim_type, 'X') NOT IN ('I', 'P') THEN NULL
        WHEN COALESCE(claim_type, 'X') = 'P' AND LPAD(clm.bill_type, 2, '0' )  IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99') THEN NULL
        WHEN COALESCE(claim_type, 'X') = 'I' AND SUBSTR(COALESCE(clm.bill_type, '3'), 1, 1) = '3' THEN NULL
        ELSE clm.facility_city
    END                                                                                     AS facilitycity,
    CASE
        WHEN COALESCE(claim_type, 'X') NOT IN ('I', 'P') THEN NULL
        WHEN COALESCE(claim_type, 'X') = 'P' AND LPAD(clm.bill_type, 2, '0' )  IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99') THEN NULL
        WHEN COALESCE(claim_type, 'X') = 'I' AND SUBSTR(COALESCE(clm.bill_type, '3'), 1, 1) = '3' THEN NULL
        ELSE VALIDATE_STATE_CODE(clm.facility_city)
    END                                                                                     AS facilitystate,
    CASE
        WHEN COALESCE(claim_type, 'X') NOT IN ('I', 'P') THEN NULL
        WHEN COALESCE(claim_type, 'X') = 'P' AND LPAD(clm.bill_type, 2, '0' )  IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99') THEN NULL
        WHEN COALESCE(claim_type, 'X') = 'I' AND SUBSTR(COALESCE(clm.bill_type, '3'), 1, 1) = '3' THEN NULL
        ELSE MASK_ZIP_CODE(clm.facility_zip)
    END                                                                                     AS facilityzip,
/*
    CASE
        WHEN COALESCE(claim_type, 'X') NOT IN ('I', 'P') THEN NULL
        WHEN COALESCE(claim_type, 'X') = 'P' AND LPAD(clm.bill_type, 2, '0' )  IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99') THEN NULL
        WHEN COALESCE(claim_type, 'X') = 'I' AND SUBSTR(COALESCE(clm.bill_type, '3'), 1, 1) = '3' THEN NULL
        ELSE TO_DATE(clm.statement_from_date, 'yyyyMMdd')
    END                                                                                     AS statementstartdate,
    CASE
        WHEN COALESCE(claim_type, 'X') NOT IN ('I', 'P') THEN NULL
        WHEN COALESCE(claim_type, 'X') = 'P' AND LPAD(clm.bill_type, 2, '0' )  IN ('05', '06', '07', '08', '09', '12', '13', '14', '33', '99') THEN NULL
        WHEN COALESCE(claim_type, 'X') = 'I' AND SUBSTR(COALESCE(clm.bill_type, '3'), 1, 1) = '3' THEN NULL
        ELSE TO_DATE(clm.statement_to_date, 'yyyyMMdd')
    END                                                                                     AS statementenddate,
    */
    ----------------------------------------------------------------------------------------------------------------------
    -- Replaced old statementstartdate code with this syncronized (to Veradigm Allscripts) version
    ----------------------------------------------------------------------------------------------------------------------
    COALESCE
    (
        CAST(EXTRACT_DATE(sln_date.min_service_from     , '%Y%m%d' ) AS DATE),
        CAST(EXTRACT_DATE(clm.received_date             , '%Y%m%d' ) AS DATE)
    )                                                                                       AS statementstartdate,
    ----------------------------------------------------------------------------------------------------------------------
    -- Replaced old statementtodate code with this syncronized (to Veradigm Allscripts) version
    ----------------------------------------------------------------------------------------------------------------------
    COALESCE
    (
        CAST(EXTRACT_DATE(sln_date.max_service_to       , '%Y%m%d' ) AS DATE),
        CAST(EXTRACT_DATE(clm.received_date             , '%Y%m%d' ) AS DATE)
    )                                                                                       AS statementenddate,


    CAST(clm.total_claim_charge_amt AS FLOAT)                                               AS claimcharges,
    CASE
        WHEN COALESCE(claim_type, 'X') <> 'I' THEN NULL
        WHEN SUBSTR(COALESCE(clm.bill_type, 'X'), 1, 1) = '3' THEN CONCAT('X', SUBSTR(clm.bill_type, 2))
        ELSE clm.bill_type
    END                                                                                     AS billtypecode,
    clm.claim_filing_indicator_cd                                                           AS claimfilingindicatorcode,
    clm.accident_related_ind                                                                AS accidentrelatedindicator,
    TO_DATE(clm.admission_date, 'yyyyMMdd')                                                 AS admissiondate,               -- Converted this source, but not specified in Transformation.
    clm.admission_type_code                                                                 AS admissiontypecode,
    clm.admission_source_code                                                               AS admissionsourcecode,
    clm.patient_status_code                                                                 AS patientstatuscode,
    CASE
        WHEN claim_type = 'I'  AND clm.drg_code IN ('283', '284', '285', '789') THEN  NULL
        WHEN claim_type = 'I'   THEN CLEAN_UP_NUMERIC_CODE(clm.drg_code)
        ELSE NULL
    END                                                                                     AS drg,

    clm.principal_icd_procedure_code                                                        AS icdprocedurecode1,
    CLEAN_UP_DIAGNOSIS_CODE(clm.admitting_diagnosis_code,
        CASE
            WHEN clm.coding_type = '9' THEN '01'
            WHEN clm.coding_type = 'X' THEN '02'
            ELSE NULL
        END,
        COALESCE(clm.statement_from_date,sln.service_from_date))                            AS admittingdiagnosiscode,
    CLEAN_UP_DIAGNOSIS_CODE(clm.principal_diagnosis_code,
        CASE
            WHEN clm.coding_type = '9' THEN '01'
            WHEN clm.coding_type = 'X' THEN '02'
            ELSE NULL
        END,
        COALESCE(clm.statement_from_date,sln.service_from_date))                            AS principaldiagnosiscode,
    CLEAN_UP_DIAGNOSIS_CODE(clm.other_diagnosis_code_1,
        CASE
            WHEN clm.coding_type = '9' THEN '01'
            WHEN clm.coding_type = 'X' THEN '02'
            ELSE NULL
        END,
        COALESCE(clm.statement_from_date,sln.service_from_date))                            AS secondarydiagnosiscode1,
    CLEAN_UP_DIAGNOSIS_CODE(clm.other_diagnosis_code_2,
        CASE
            WHEN clm.coding_type = '9' THEN '01'
            WHEN clm.coding_type = 'X' THEN '02'
            ELSE NULL
        END,
        COALESCE(clm.statement_from_date,sln.service_from_date))                            AS secondarydiagnosiscode2,
    CLEAN_UP_DIAGNOSIS_CODE(clm.other_diagnosis_code_3,
        CASE
            WHEN clm.coding_type = '9' THEN '01'
            WHEN clm.coding_type = 'X' THEN '02'
            ELSE NULL
        END,
        COALESCE(clm.statement_from_date,sln.service_from_date))                            AS secondarydiagnosiscode3,
    CLEAN_UP_DIAGNOSIS_CODE(clm.other_diagnosis_code_4,
        CASE
            WHEN clm.coding_type = '9' THEN '01'
            WHEN clm.coding_type = 'X' THEN '02'
            ELSE NULL
        END,
        COALESCE(clm.statement_from_date,sln.service_from_date))                            AS secondarydiagnosiscode4,
    CLEAN_UP_DIAGNOSIS_CODE(clm.other_diagnosis_code_5,
        CASE
            WHEN clm.coding_type = '9' THEN '01'
            WHEN clm.coding_type = 'X' THEN '02'
            ELSE NULL
        END,
        COALESCE(clm.statement_from_date,sln.service_from_date))                            AS secondarydiagnosiscode5,
    CLEAN_UP_DIAGNOSIS_CODE(clm.other_diagnosis_code_6,
        CASE
            WHEN clm.coding_type = '9' THEN '01'
            WHEN clm.coding_type = 'X' THEN '02'
            ELSE NULL
        END,
        COALESCE(clm.statement_from_date,sln.service_from_date))                            AS secondarydiagnosiscode6,
    CLEAN_UP_DIAGNOSIS_CODE(clm.other_diagnosis_code_7,
        CASE
            WHEN clm.coding_type = '9' THEN '01'
            WHEN clm.coding_type = 'X' THEN '02'
            ELSE NULL
        END,
        COALESCE(clm.statement_from_date,sln.service_from_date))                            AS secondarydiagnosiscode7,
    CLEAN_UP_DIAGNOSIS_CODE(sup.other_diagnosis_code_8,
        CASE
            WHEN clm.coding_type = '9' THEN '01'
            WHEN clm.coding_type = 'X' THEN '02'
            ELSE NULL
        END,
        COALESCE(clm.statement_from_date,sln.service_from_date))                            AS secondarydiagnosiscode8,
    CLEAN_UP_DIAGNOSIS_CODE(sup.other_diagnosis_code_9,
        CASE
            WHEN clm.coding_type = '9' THEN '01'
            WHEN clm.coding_type = 'X' THEN '02'
            ELSE NULL
        END,
        COALESCE(clm.statement_from_date,sln.service_from_date))                            AS secondarydiagnosiscode9,
    CLEAN_UP_DIAGNOSIS_CODE(sup.other_diagnosis_code_10,
        CASE
            WHEN clm.coding_type = '9' THEN '01'
            WHEN clm.coding_type = 'X' THEN '02'
            ELSE NULL
            END,
            COALESCE(clm.statement_from_date,sln.service_from_date))                        AS secondarydiagnosiscode10,
    CLEAN_UP_DIAGNOSIS_CODE(sup.other_diagnosis_code_11,
        CASE
            WHEN clm.coding_type = '9' THEN '01'
            WHEN clm.coding_type = 'X' THEN '02'
            ELSE NULL
        END,
        COALESCE(clm.statement_from_date,sln.service_from_date))                            AS secondarydiagnosiscode11,
    CLEAN_UP_DIAGNOSIS_CODE(sup.other_diagnosis_code_12,
        CASE
            WHEN clm.coding_type = '9' THEN '01'
            WHEN clm.coding_type = 'X' THEN '02'
            ELSE NULL
        END,
        COALESCE(clm.statement_from_date,sln.service_from_date))                            AS secondarydiagnosiscode12,
    CLEAN_UP_DIAGNOSIS_CODE(sup.other_diagnosis_code_13,
        CASE
            WHEN clm.coding_type = '9' THEN '01'
            WHEN clm.coding_type = 'X' THEN '02'
            ELSE NULL
        END,
        COALESCE(clm.statement_from_date,sln.service_from_date))                            AS secondarydiagnosiscode13,
    CLEAN_UP_DIAGNOSIS_CODE(sup.other_diagnosis_code_14,
        CASE
            WHEN clm.coding_type = '9' THEN '01'
            WHEN clm.coding_type = 'X' THEN '02'
            ELSE NULL
        END,
        COALESCE(clm.statement_from_date,sln.service_from_date))                            AS secondarydiagnosiscode14,
    CLEAN_UP_DIAGNOSIS_CODE(sup.other_diagnosis_code_15,
        CASE
            WHEN clm.coding_type = '9' THEN '01'
            WHEN clm.coding_type = 'X' THEN '02'
            ELSE NULL
        END,
        COALESCE(clm.statement_from_date,sln.service_from_date))                            AS secondarydiagnosiscode15,
    CLEAN_UP_DIAGNOSIS_CODE(sup.other_diagnosis_code_16,
        CASE
            WHEN clm.coding_type = '9' THEN '01'
            WHEN clm.coding_type = 'X' THEN '02'
            ELSE NULL
        END,
        COALESCE(clm.statement_from_date,sln.service_from_date))                            AS secondarydiagnosiscode16,
    CLEAN_UP_DIAGNOSIS_CODE(sup.other_diagnosis_code_17,
        CASE
            WHEN clm.coding_type = '9' THEN '01'
            WHEN clm.coding_type = 'X' THEN '02'
            ELSE NULL
        END,
        COALESCE(clm.statement_from_date,sln.service_from_date))                            AS secondarydiagnosiscode17,
    CLEAN_UP_DIAGNOSIS_CODE(sup.other_diagnosis_code_18,
        CASE
            WHEN clm.coding_type = '9' THEN '01'
            WHEN clm.coding_type = 'X' THEN '02'
            ELSE NULL
        END,
        COALESCE(clm.statement_from_date,sln.service_from_date))                            AS secondarydiagnosiscode18,
    CLEAN_UP_DIAGNOSIS_CODE(sup.other_diagnosis_code_19,
        CASE
            WHEN clm.coding_type = '9' THEN '01'
            WHEN clm.coding_type = 'X' THEN '02'
            ELSE NULL
        END,
        COALESCE(clm.statement_from_date,sln.service_from_date))                            AS secondarydiagnosiscode19,
    CLEAN_UP_DIAGNOSIS_CODE(sup.other_diagnosis_code_20,
        CASE
            WHEN clm.coding_type = '9' THEN '01'
            WHEN clm.coding_type = 'X' THEN '02'
            ELSE NULL
        END,
        COALESCE(clm.statement_from_date,sln.service_from_date))                            AS secondarydiagnosiscode20,
    CLEAN_UP_DIAGNOSIS_CODE(sup.other_diagnosis_code_21,
        CASE
            WHEN clm.coding_type = '9' THEN '01'
            WHEN clm.coding_type = 'X' THEN '02'
            ELSE NULL
        END,
        COALESCE(clm.statement_from_date,sln.service_from_date))                            AS secondarydiagnosiscode21,
    CLEAN_UP_DIAGNOSIS_CODE(sup.other_diagnosis_code_22,
        CASE
            WHEN clm.coding_type = '9' THEN '01'
            WHEN clm.coding_type = 'X' THEN '02'
            ELSE NULL
        END,
        COALESCE(clm.statement_from_date,sln.service_from_date))                            AS secondarydiagnosiscode22,
    CLEAN_UP_DIAGNOSIS_CODE(sup.other_diagnosis_code_23,
        CASE
            WHEN clm.coding_type = '9' THEN '01'
            WHEN clm.coding_type = 'X' THEN '02'
            ELSE NULL
        END,
        COALESCE(clm.statement_from_date,sln.service_from_date))                            AS secondarydiagnosiscode23,
    CLEAN_UP_DIAGNOSIS_CODE(sup.other_diagnosis_code_24,
        CASE
            WHEN clm.coding_type = '9' THEN '01'
            WHEN clm.coding_type = 'X' THEN '02'
            ELSE NULL
        END,
        COALESCE(clm.statement_from_date,sln.service_from_date))                            AS secondarydiagnosiscode24,

    clm.other_icd_proc_code_1                                                               AS icdprocedurecode2,
    clm.other_icd_proc_code_2                                                               AS icdprocedurecode3,
    clm.other_icd_proc_code_3                                                               AS icdprocedurecode4,
    clm.other_icd_proc_code_4                                                               AS icdprocedurecode5,
    clm.other_icd_proc_code_5                                                               AS icdprocedurecode6,
    clm.other_icd_proc_code_6                                                               AS icdprocedurecode7,
    clm.other_icd_proc_code_7                                                               AS icdprocedurecode8,
    clm.other_icd_proc_code_8                                                               AS icdprocedurecode9,
    clm.other_icd_proc_code_9                                                               AS icdprocedurecode10,
    sup.other_icd_proc_code_10                                                              AS icdprocedurecode11,
    sup.other_icd_proc_code_11                                                              AS icdprocedurecode12,
    sup.other_icd_proc_code_12                                                              AS icdprocedurecode13,
    sup.other_icd_proc_code_13                                                              AS icdprocedurecode14,
    sup.other_icd_proc_code_14                                                              AS icdprocedurecode15,
    sup.other_icd_proc_code_15                                                              AS icdprocedurecode16,
    sup.other_icd_proc_code_16                                                              AS icdprocedurecode17,
    sup.other_icd_proc_code_17                                                              AS icdprocedurecode18,
    sup.other_icd_proc_code_18                                                              AS icdprocedurecode19,
    sup.other_icd_proc_code_19                                                              AS icdprocedurecode20,
    sup.other_icd_proc_code_20                                                              AS icdprocedurecode21,
    sup.other_icd_proc_code_21                                                              AS icdprocedurecode22,
    sup.other_icd_proc_code_22                                                              AS icdprocedurecode23,
    sup.other_icd_proc_code_23                                                              AS icdprocedurecode24,
    sup.other_icd_proc_code_24                                                              AS icdprocedurecode25,

    clm.payer_id                                                                            AS payerid,
    clm.payer_name                                                                          AS payername,
    CASE
        WHEN clm.coding_type = '9' THEN '01'
        WHEN clm.coding_type = 'X' THEN '02'
    END                                                                                     AS codingtype,
    SPLIT(clm.input_file_name, '/')[SIZE(SPLIT(clm.input_file_name, '/')) - 1]              AS sourcefilename,
    'citra'                                                                                 AS data_vendor,                 -- Will verified source/target names 4/27/2021.
    CAST(NULL AS STRING)                                                                    AS dhcreceiveddate,             -- Set to NULL per Will 4/22/2021.
    CAST(NULL AS STRING)                                                                    AS rownumber,                   -- Set to NULL per Will 4/22/2021.
    CAST(NULL AS STRING)                                                                    AS fileyear,
    CAST(NULL AS STRING)                                                                    AS filemonth,
    CAST(NULL AS STRING)                                                                    AS fileday,
    '219'                                                                                   AS data_feed,
    'change_dx'                                                                             AS part_provider,
    CASE
        WHEN
            COALESCE
            (
                CAST(EXTRACT_DATE(sln_date.min_service_from     , '%Y%m%d' ) AS DATE),
                CAST(EXTRACT_DATE(clm.received_date             , '%Y%m%d' ) AS DATE)
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
                      SUBSTR(clm.received_date, 1, 4), '-',
                      SUBSTR(clm.received_date, 5, 2), '-01'
                    )
            )
    END                                                                                     AS part_best_date

FROM change_dx_claims clm
LEFT OUTER JOIN change_dx_service_lines sln         ON UPPER(clm.claim_tcn_id) = UPPER(sln.claim_number)
                                                    AND sln.service_line_number = 1
LEFT OUTER JOIN change_dx_supplement sup            ON UPPER(clm.claim_tcn_id) = UPPER(sup.claim_number)
LEFT OUTER JOIN matching_payload pay               ON pay.hvid IS NOT NULL AND UPPER(clm.claim_tcn_id) = UPPER(pay.claimid)
LEFT OUTER JOIN change_dx_plainout pln              ON UPPER(clm.claim_tcn_id) = UPPER(pln.claim_number)
LEFT OUTER JOIN
(
SELECT sln_1.claim_number, MIN(sln_1.service_from_date) AS min_service_from, MAX( sln_1.service_to_date) AS max_service_to
FROM change_dx_service_lines sln_1
GROUP BY sln_1.claim_number
) sln_date ON clm.claim_tcn_id = sln_date.claim_number
ORDER BY 1
