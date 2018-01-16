INSERT INTO emr_common_model
SELECT DISTINCT
        NULL,                           -- record_id
        mp.hvid,                        -- hvid
        {today},                        -- created
        '1',                            -- model_version
        {filename},                     -- data_set
        {feedname},                     -- data_feed
        {vendor},                       -- data_vendor
        NULL,                           -- source_version
        big_union.claim_type,           -- claim_type
        CONCAT(
             base.patientidnumber,
             '-', base.analyticdos,
             '-', base.clinicorganizationidnumber
            ),                          -- claim_id
        NULL,                           -- claim_qual
        NULL,                           -- claim_date
        NULL,                           -- claim_error_ind
        cap_age(mp.age),                -- patient_age
        cap_year_of_birth(
            mp.age,
            big_union.date_service,
            mp.yearOfBirth
            ),                          -- patient_year_of_birth
        mp.threeDigitZip,               -- patient_zip
        UPPER(mp.state),                -- patient_state
        NULL,                           -- patient_deceased_flag
        mp.gender,                      -- patient_gender
        NULL,                           -- patient_race
        NULL,                           -- patient_ethnicity
        NULL,                           -- provider_client_id_qual
        NULL,                           -- provider_client_id
        NULL,                           -- provider_rendering_id_qual
        NULL,                           -- provider_rendering_id
        NULL,                           -- provider_referring_id_qual
        NULL,                           -- provider_referring_id
        NULL,                           -- provider_billing_id_qual
        NULL,                           -- provider_biling_id
        NULL,                           -- provider_facility_id_qual
        NULL,                           -- provider_facility_id
        NULL,                           -- provider_ordering_id_qual
        NULL,                           -- provider_ordering_id
        NULL,                           -- provider_lab_id_qual
        NULL,                           -- provider_lab_id
        NULL,                           -- provider_pharmacy_id_qual
        NULL,                           -- provider_pharmacy_id
        NULL,                           -- provider_prescriber_id_qual
        NULL,                           -- provider_prescriber_id
        NULL,                           -- payer_id_qual
        NULL,                           -- payer_id
        NULL,                           -- payer_type
        NULL,                           -- payer_parent
        NULL,                           -- payer_name
        NULL,                           -- plan_name
        NULL,                           -- encounter_id
        NULL,                           -- encounter_id_qual
        NULL,                           -- description
        NULL,                           -- description_qual
        COALESCE(
            extract_date(
                substring(big_union.date_service, 0, 10),
                '%Y-%m-%d',
                cast({min_date} as date),
                cast({max_date} as date)
                ),
            extract_date(
                substring(base.analyticdos, 0, 10),
                '%Y-%m-%d',
                cast({min_date} as date),
                cast({max_date} as date)
                )
            ),                          -- date_start
        NULL,                           -- date_end
        NULL,                           -- date_qual
        clean_up_diagnosis_code(
            big_union.diagnosis_code,
            big_union.diagnosis_code_qual,
            big_union.date_service
            ),                          -- diagnosis_code
        big_union.diagnosis_code_qual,  -- diagnosis_code_qual
        NULL,                           -- diagnosis_code_priority
        clean_up_procedure_code(
            big_union.procedure_code
            ),                          -- procedure_code
        NULL,                           -- procedure_code_qual
        NULL,                           -- procedure_code_modifier
        NULL,                           -- procedure_code_priority
        clean_up_ndc_code(
            big_union.ndc_code
            ),                          -- ndc_code
        NULL,                           -- ndc_code_qual
        NULL,                           -- loinc_code
        NULL,                           -- other_code
        NULL,                           -- other_code_qual
        NULL,                           -- other_code_modifier
        NULL,                           -- other_code_mod_qual
        NULL,                           -- other_code_priority
        NULL,                           -- type
        NULL,                           -- type_qual
        NULL,                           -- category
        NULL,                           -- category_qual
        NULL,                           -- panel
        NULL,                           -- panel_qual
        NULL,                           -- specimen
        NULL,                           -- specimen_qual
        NULL,                           -- method
        NULL,                           -- method_qual
        NULL,                           -- result
        NULL,                           -- result_qual
        NULL,                           -- reason
        NULL,                           -- reason_qual
        NULL,                           -- ref_range
        NULL,                           -- ref_range_qual
        NULL,                           -- abnormal
        NULL,                           -- abnormal_qual
        NULL,                           -- uom
        NULL,                           -- uom_qual
        NULL,                           -- severity
        NULL,                           -- severity_qual
        NULL,                           -- status
        NULL,                           -- status_qual
        NULL,                           -- units
        NULL,                           -- units_qual
        NULL,                           -- qty_dispensed
        NULL,                           -- qty_dispensed_qual
        NULL,                           -- days_supply
        NULL,                           -- elapsed_days
        NULL,                           -- num_refills
        NULL,                           -- route
        NULL,                           -- sig
        NULL,                           -- frequency_units
        NULL,                           -- frequency_times
        NULL,                           -- frequency_uom
        NULL,                           -- dose
        NULL,                           -- dose_qual
        NULL,                           -- strength
        NULL,                           -- form
        NULL,                           -- sample
        NULL,                           -- unverified
        NULL                            -- electronicrx
FROM dialysistreatment base
    FULL OUTER JOIN (
    -- diagnosis
    -- cleaning: only contain alpha-numeric characters, no longer than 7 characters
    SELECT
        patientdataanalyticrowidnumber as visonex_patient_id,
        CONCAT(
            patientidnumber,
             '-', analyticdos,
             '-', clinicorganizationidnumber
            ) as claim_id,
        'HOSPITALIZATION' AS claim_type,
        icd10 AS diagnosis_code,
        '02' AS diagnosis_code_qual,
        NULL AS procedure_code,
        NULL AS ndc_code,
        admissiondate AS date_service
    FROM hospitalization
    UNION
    SELECT
        patientdataanalyticrowidnumber as visonex_patient_id,
        CONCAT(
            patientidnumber,
             '-', analyticdos,
             '-', clinicorganizationidnumber
            ) as claim_id,
        'HOSPITALIZATION' AS claim_type,
        ICD9 AS diagnosis_code,
        '01' AS diagnosis_code_qual,
        NULL AS procedure_code,
        NULL AS ndc_code,
        admissiondate AS date_service
    FROM hospitalization
    UNION
    SELECT
        patientdataanalyticrowidnumber as visonex_patient_id,
        CONCAT(
            patientidnumber,
             '-', analyticdos,
             '-', clinicorganizationidnumber
            ) as claim_id,
        'IMMUNIZATION' AS claim_type,
        icd10 AS diagnosis_code,
        '02' AS diagnosis_code_qual,
        NULL AS procedure_code,
        NULL AS ndc_code,
        immunizationdate AS date_service
    FROM immunization
    UNION
    SELECT
        patientdataanalyticrowidnumber as visonex_patient_id,
        CONCAT(
            patientidnumber,
             '-', analyticdos,
             '-', clinicorganizationidnumber
            ) as claim_id,
        'PATIENTACCESS_EXAMPROC' AS claim_type,
        icd10 AS diagnosis_code,
        '02' AS diagnosis_code_qual,
        NULL AS procedure_code,
        NULL AS ndc_code,
        examproceduredate AS date_service
    FROM patientaccess_examproc
    UNION
    SELECT
        patientdataanalyticrowidnumber as visonex_patient_id,
        CONCAT(
            patientidnumber,
             '-', analyticdos,
             '-', clinicorganizationidnumber
            ) as claim_id,
        'PATIENTACCESS_EXAMPROC' AS claim_type,
        DIAGNOSTICCODE AS diagnosis_code,
        NULL AS diagnosis_code_qual,
        NULL AS procedure_code,
        NULL AS ndc_code,
        examproceduredate AS date_service
    FROM patientaccess_examproc
    UNION
    SELECT
        patientdataanalyticrowidnumber as visonex_patient_id,
        CONCAT(
            patientidnumber,
             '-', analyticdos,
             '-', clinicorganizationidnumber
            ) as claim_id,
        'LABPANELSDRAWN' AS claim_type,
        JUSTIFICATION AS diagnosis_code,
        NULL AS diagnosis_code_qual,
        NULL AS procedure_code,
        NULL AS ndc_code,
        thedate AS date_service
    FROM labpanelsdrawn
    UNION
    SELECT
        patientdataanalyticrowidnumber as visonex_patient_id,
        CONCAT(
            patientidnumber,
             '-', analyticdos,
             '-', clinicorganizationidnumber
            ) as claim_id,
        'LABRESULT' as claim_type,
        diagnosiscode as diagnosis_code,
        NULL as diagnosis_code_qual,
        NULL AS procedure_code,
        NULL AS ndc_code,
        receiveddate as date_service
    FROM labresult
    UNION
    SELECT
        patientdataanalyticrowidnumber as visonex_patient_id,
        CONCAT(
            patientidnumber,
             '-', analyticdos,
             '-', clinicorganizationidnumber
            ) as claim_id,
        'LABRESULT' as claim_type,
        icd10 as diagnosis_code,
        '02' as diagnosis_code_qual,
        NULL AS procedure_code,
        NULL AS ndc_code,
        receiveddate as date_service
    FROM labresult
    UNION
    SELECT
        patientdataanalyticrowidnumber as visonex_patient_id,
        CONCAT(
            patientidnumber,
             '-', analyticdos,
             '-', clinicorganizationidnumber
            ) as claim_id,
        'PATIENTDIAGCODES' as claim_type,
        diagnosiscode as diagnosis_code,
        NULL as diagnosis_code_qual,
        NULL AS procedure_code,
        NULL AS ndc_code,
        diagnosisdate as date_service
    FROM patientdiagcodes
    UNION
    SELECT
        patientdataanalyticrowidnumber as visonex_patient_id,
        CONCAT(
            patientidnumber,
             '-', analyticdos,
             '-', clinicorganizationidnumber
            ) as claim_id,
        'PATIENTDIAGCODES' as claim_type,
        icd10 as diagnosis_code,
        '02' as diagnosis_code_qual,
        NULL AS procedure_code,
        NULL AS ndc_code,
        diagnosisdate as date_service
    FROM patientdiagcodes
    UNION
    SELECT
        patientdataanalyticrowidnumber as visonex_patient_id,
        CONCAT(
            patientidnumber,
             '-', analyticdos,
             '-', clinicorganizationidnumber
            ) as claim_id,
        'PATIENTMEDADMINISTERED' as claim_type,
        icd10 as diagnosis_code,
        '02' as diagnosis_code_qual,
        NULL AS procedure_code,
        NULL AS ndc_code,
        startdate as date_service
    FROM patientmedadministered
    UNION
    SELECT
        patientdataanalyticrowidnumber as visonex_patient_id,
        CONCAT(
            patientidnumber,
             '-', analyticdos,
             '-', clinicorganizationidnumber
            ) as claim_id,
        'PATIENTMEDPRESCRIPTION' as claim_type,
        runjustification as diagnosis_code,
        NULL as diagnosis_code_qual,
        NULL AS procedure_code,
        NULL AS ndc_code,
        startdate as date_service
    FROM patientmedprescription
    UNION
    SELECT
        patientdataanalyticrowidnumber as visonex_patient_id,
        CONCAT(
            patientidnumber,
             '-', analyticdos,
             '-', clinicorganizationidnumber
            ) as claim_id,
        'PROBLEMLIST' as claim_type,
        icd9 as diagnosis_code,
        '01' as diagnosis_code_qual,
        NULL AS procedure_code,
        NULL AS ndc_code,
        startdate as date_service
    FROM problemlist
    UNION
    SELECT
        patientdataanalyticrowidnumber as visonex_patient_id,
        CONCAT(
            patientidnumber,
             '-', analyticdos,
             '-', clinicorganizationidnumber
            ) as claim_id,
        'PROBLEMLIST' as claim_type,
        icd10 as diagnosis_code,
        '02' as diagnosis_code_qual,
        NULL AS procedure_code,
        NULL AS ndc_code,
        startdate as date_service
    FROM problemlist
    UNION
    SELECT
        b.patientdataanalyticrowidnumber as visonex_patient_id,
        CONCAT(
            b.patientidnumber,
             '-', b.analyticdos,
             '-', b.clinicorganizationidnumber
            ) as claim_id,
        'LABIDLIST' as claim_type,
        a.icd9diagnosiscode as diagnosis_code,
        '01' as diagnosis_code_qual,
        NULL AS procedure_code,
        NULL AS ndc_code,
        b.receiveddate as date_service
    FROM labidlist a
        INNER JOIN labresult b ON a.universalserviceid=b.universalserviceid
        AND a.observationidentifier=b.observationidentifier AND a.testname=b.testname

    UNION

    -- procedure
    -- cleaning: only contain alpha-numeric characters, no longer than 7 characters
    SELECT
        patientdataanalyticrowidnumber as visonex_patient_id,
        CONCAT(
            patientidnumber,
             '-', analyticdos,
             '-', clinicorganizationidnumber
            ) as claim_id,
        'PATIENTACCESS_EXAMPROC' as claim_type,
        NULL AS diagnosis_code,
        NULL AS diagnosis_code_qual,
        procedurecode as procedure_code,
        NULL AS ndc_code,
        examproceduredate as date_service
    FROM PatientAccess_ExamProc
    UNION
    SELECT
        patientdataanalyticrowidnumber as visonex_patient_id,
        CONCAT(
            patientidnumber,
             '-', analyticdos,
             '-', clinicorganizationidnumber
            ) as claim_id,
        'LABRESULT' as claim_type,
        NULL AS diagnosis_code,
        NULL AS diagnosis_code_qual,
        procedurecode as procedure_code,
        NULL AS ndc_code,
        receiveddate as date_service
    FROM labresult
    UNION
    SELECT
        patientdataanalyticrowidnumber as visonex_patient_id,
        CONCAT(
            patientidnumber,
             '-', analyticdos,
             '-', clinicorganizationidnumber
            ) as claim_id,
        'LABPANELSDRAWN' as claim_type,
        NULL AS diagnosis_code,
        NULL AS diagnosis_code_qual,
        procedurecode as procedure_code,
        NULL AS ndc_code,
        thedate as date_service
    FROM labpanelsdrawn
    UNION
    SELECT
        patientdataanalyticrowidnumber as visonex_patient_id,
        CONCAT(
            patientidnumber,
             '-', analyticdos,
             '-', clinicorganizationidnumber
            ) as claim_id,
        'PATIENTMEDADMINISTERED' as claim_type,
        NULL AS diagnosis_code,
        NULL AS diagnosis_code_qual,
        procedurecode as procedure_code,
        NULL AS ndc_code,
        startdate as date_service
    FROM patientmedadministered
    UNION
    SELECT
        patientdataanalyticrowidnumber as visonex_patient_id,
        CONCAT(
            patientidnumber,
             '-', analyticdos,
             '-', clinicorganizationidnumber
            ) as claim_id,
        'PATIENTMEDADMINISTERED' as claim_type,
        NULL AS diagnosis_code,
        NULL AS diagnosis_code_qual,
        medicationcode as procedure_code,
        NULL AS ndc_code,
        startdate as date_service
    FROM patientmedadministered
    UNION
    SELECT
        b.patientdataanalyticrowidnumber as visonex_patient_id,
        CONCAT(
            b.patientidnumber,
             '-', b.analyticdos,
             '-', b.clinicorganizationidnumber
            ) as claim_id,
        'LABIDLIST' as claim_type,
        NULL AS diagnosis_code,
        NULL AS diagnosis_code_qual,
        a.cptcode as procedure_code,
        NULL AS ndc_code,
        b.receiveddate as date_service
    FROM labidlist a
        INNER JOIN labresult b ON a.universalserviceid=b.universalserviceid
        AND a.observationidentifier=b.observationidentifier AND a.testname=b.testname

    UNION

    -- ndc
    -- cleaning: only contain numeric characters, must be 11-digits long
    SELECT
        patientdataanalyticrowidnumber as visonex_patient_id,
        CONCAT(
            patientidnumber,
             '-', analyticdos,
             '-', clinicorganizationidnumber
            ) as claim_id,
        'PATIENTMEDADMINISTERED' as claim_type,
        NULL AS diagnosis_code,
        NULL AS diagnosis_code_qual,
        NULL AS procedure_code,
        procedurecode as ndc_code,
        startdate as date_service
    FROM patientmedadministered
    UNION

    -- misc tables with no codes
    SELECT patientdataanalyticrowidnumber as visonex_patient_id,
        CONCAT(
            patientidnumber,
             '-', '',
             '-', clinicorganizationidnumber
            ) as claim_id,
        'ADDRESS' as claim_type,
        NULL AS diagnosis_code,
        NULL AS diagnosis_code_qual,
        NULL AS procedure_code,
        NULL as ndc_code,
        NULL as date_service
    FROM address
    UNION
    SELECT patientdataanalyticrowidnumber as visonex_patient_id,
        CONCAT(
            patientidnumber,
             '-', analyticdos,
             '-', clinicorganizationidnumber
            ) as claim_id,
        'DIALYSISTRAINING' as claim_type,
        NULL AS diagnosis_code,
        NULL AS diagnosis_code_qual,
        NULL AS procedure_code,
        NULL as ndc_code,
        analyticdos as date_service
    FROM dialysistraining
    UNION
    SELECT patientdataanalyticrowidnumber as visonex_patient_id,
        CONCAT(
            patientidnumber,
             '-', analyticdos,
             '-', clinicorganizationidnumber
            ) as claim_id,
        'FACILITYADMITDISCHARGE' as claim_type,
        NULL AS diagnosis_code,
        NULL AS diagnosis_code_qual,
        NULL AS procedure_code,
        NULL as ndc_code,
        analyticdos as date_service
    FROM facilityadmitdischarge
    UNION
    SELECT patientdataanalyticrowidnumber as visonex_patient_id,
        CONCAT(
            patientidnumber,
             '-', '',
             '-', clinicorganizationidnumber
            ) as claim_id,
        'INSURANCE' as claim_type,
        NULL AS diagnosis_code,
        NULL AS diagnosis_code_qual,
        NULL AS procedure_code,
        NULL as ndc_code,
        NULL as date_service
    FROM insurance
    UNION
    SELECT patientdataanalyticrowidnumber as visonex_patient_id,
        CONCAT(
            patientidnumber,
             '-', '',
             '-', ''
            ) as claim_id,
        'MODALITYCHANGEHISTORYCROWNWEB' as claim_type,
        NULL AS diagnosis_code,
        NULL AS diagnosis_code_qual,
        NULL AS procedure_code,
        NULL as ndc_code,
        NULL as date_service
    FROM modalitychangehistorycrownweb
    UNION
    SELECT patientdataanalyticrowidnumber as visonex_patient_id,
        CONCAT(
            patientidnumber,
             '-', '',
             '-', clinicorganizationidnumber
            ) as claim_id,
        'PATIENTACCESS' as claim_type,
        NULL AS diagnosis_code,
        NULL AS diagnosis_code_qual,
        NULL AS procedure_code,
        NULL as ndc_code,
        NULL as date_service
    FROM patientaccess
    UNION
    SELECT patientdataanalyticrowidnumber as visonex_patient_id,
        CONCAT(
            patientidnumber,
             '-', analyticdos,
             '-', clinicorganizationidnumber
            ) as claim_id,
        'NURSINGHOMEHISTORY' as claim_type,
        NULL AS diagnosis_code,
        NULL AS diagnosis_code_qual,
        NULL AS procedure_code,
        NULL as ndc_code,
        analyticdos as date_service
    FROM nursinghomehistory
    UNION
    SELECT patientdataanalyticrowidnumber as visonex_patient_id,
        CONCAT(
            patientidnumber,
             '-', '',
             '-', clinicorganizationidnumber
            ) as claim_id,
        'PATIENTACCESS_OTHERACCESSEVENT' as claim_type,
        NULL AS diagnosis_code,
        NULL AS diagnosis_code_qual,
        NULL AS procedure_code,
        NULL as ndc_code,
        NULL as date_service
    FROM patientaccess_otheraccessevent
    UNION
    SELECT patientdataanalyticrowidnumber as visonex_patient_id,
        CONCAT(
            patientidnumber,
             '-', '',
             '-', clinicorganizationidnumber
            ) as claim_id,
        'PATIENTACCESS_PLACEDRECORDED' as claim_type,
        NULL AS diagnosis_code,
        NULL AS diagnosis_code_qual,
        NULL AS procedure_code,
        NULL as ndc_code,
        NULL as date_service
    FROM patientaccess_placedrecorded
    UNION
    SELECT patientdataanalyticrowidnumber as visonex_patient_id,
        CONCAT(
            patientidnumber,
             '-', '',
             '-', clinicorganizationidnumber
            ) as claim_id,
        'PATIENTACCESS_REMOVED' as claim_type,
        NULL AS diagnosis_code,
        NULL AS diagnosis_code_qual,
        NULL AS procedure_code,
        NULL as ndc_code,
        NULL as date_service
    FROM patientaccess_removed
    UNION
    SELECT patientdataanalyticrowidnumber as visonex_patient_id,
        CONCAT(
            patientidnumber,
             '-', '',
             '-', clinicorganizationidnumber
            ) as claim_id,
        'PATIENTALLERGY' as claim_type,
        NULL AS diagnosis_code,
        NULL AS diagnosis_code_qual,
        NULL AS procedure_code,
        NULL as ndc_code,
        NULL as date_service
    FROM patientallergy
    UNION
    SELECT patientdataanalyticrowidnumber as visonex_patient_id,
        CONCAT(
            patientidnumber,
             '-', '',
             '-', clinicorganizationidnumber
            ) as claim_id,
        'PATIENTALLERGY' as claim_type,
        NULL AS diagnosis_code,
        NULL AS diagnosis_code_qual,
        NULL AS procedure_code,
        NULL as ndc_code,
        NULL as date_service
    FROM patientallergy
    UNION
    SELECT patientdataanalyticrowidnumber as visonex_patient_id,
        CONCAT(
            patientidnumber,
             '-', '',
             '-', clinicorganizationidnumber
            ) as claim_id,
        'PATIENTCOMORBIDITYANDTRANSPLANTSTATE' as claim_type,
        NULL AS diagnosis_code,
        NULL AS diagnosis_code_qual,
        NULL AS procedure_code,
        NULL as ndc_code,
        NULL as date_service
    FROM patientcomorbidityandtransplantstate
    UNION
    SELECT analyticrowidnumber as visonex_patient_id,
        CONCAT(
            patientidnumber,
             '-', '',
             '-', clinicorganizationidnumber
            ) as claim_id,
        'PATIENTDATA' as claim_type,
        NULL AS diagnosis_code,
        NULL AS diagnosis_code_qual,
        NULL AS procedure_code,
        NULL as ndc_code,
        NULL as date_service
    FROM patientdata
    UNION
    SELECT patientdataanalyticrowidnumber as visonex_patient_id,
        CONCAT(
            patientidnumber,
             '-', analyticdos,
             '-', clinicorganizationidnumber
            ) as claim_id,
        'PATIENTDIALYSISPRESCRIPTION' as claim_type,
        NULL AS diagnosis_code,
        NULL AS diagnosis_code_qual,
        NULL AS procedure_code,
        NULL as ndc_code,
        analyticdos as date_service
    FROM patientdialysisprescription
    UNION
    SELECT patientdataanalyticrowidnumber as visonex_patient_id,
        CONCAT(
            '',
            '-', '',
            '-', clinicorganizationidnumber
            ) as claim_id,
        'PATIENTDIALYSISRXHEMO' as claim_type,
        NULL AS diagnosis_code,
        NULL AS diagnosis_code_qual,
        NULL AS procedure_code,
        NULL as ndc_code,
        NULL as date_service
    FROM patientdialysisrxhemo
    UNION
    SELECT patientdataanalyticrowidnumber as visonex_patient_id,
        CONCAT(
            '',
            '-', '',
            '-', clinicorganizationidnumber
            ) as claim_id,
        'PATIENTDIALYSISRXPD' as claim_type,
        NULL AS diagnosis_code,
        NULL AS diagnosis_code_qual,
        NULL AS procedure_code,
        NULL as ndc_code,
        NULL as date_service
    FROM patientdialysisrxpd
    UNION
    SELECT patientdataanalyticrowidnumber as visonex_patient_id,
        CONCAT(
            '',
            '-', '',
            '-', clinicorganizationidnumber
            ) as claim_id,
        'PATIENTDIALYSISRXPDEXCHANGES' as claim_type,
        NULL AS diagnosis_code,
        NULL AS diagnosis_code_qual,
        NULL AS procedure_code,
        NULL as ndc_code,
        NULL as date_service
    FROM patientdialysisrxpdexchanges
    UNION
    SELECT patientdataanalyticrowidnumber as visonex_patient_id,
        CONCAT(
            patientidnumber,
             '-', analyticdos,
             '-', clinicorganizationidnumber
            ) as claim_id,
        'PATIENTEVENT' as claim_type,
        NULL AS diagnosis_code,
        NULL AS diagnosis_code_qual,
        NULL AS procedure_code,
        NULL as ndc_code,
        analyticdos as date_service
    FROM patientevent
    UNION
    SELECT patientdataanalyticrowidnumber as visonex_patient_id,
        CONCAT(
            patientidnumber,
             '-', analyticdos,
             '-', clinicorganizationidnumber
            ) as claim_id,
        'PATIENTFLUIDWEIGHTMANAGEMENT' as claim_type,
        NULL AS diagnosis_code,
        NULL AS diagnosis_code_qual,
        NULL AS procedure_code,
        NULL as ndc_code,
        analyticdos as date_service
    FROM patientfluidweightmanagement
    UNION
    SELECT patientdataanalyticrowidnumber as visonex_patient_id,
        CONCAT(
            patientidnumber,
             '-', analyticdos,
             '-', clinicorganizationidnumber
            ) as claim_id,
        'PATIENTHEIGHTHISTORY' as claim_type,
        NULL AS diagnosis_code,
        NULL AS diagnosis_code_qual,
        NULL AS procedure_code,
        NULL as ndc_code,
        analyticdos as date_service
    FROM patientheighthistory
    UNION
    SELECT patientdataanalyticrowidnumber as visonex_patient_id,
        CONCAT(
            patientidnumber,
             '-', analyticdos,
             '-', clinicorganizationidnumber
            ) as claim_id,
        'PATIENTINFECTION' as claim_type,
        NULL AS diagnosis_code,
        NULL AS diagnosis_code_qual,
        NULL AS procedure_code,
        NULL as ndc_code,
        analyticdos as date_service
    FROM patientinfection
    UNION
    SELECT patientdataanalyticrowidnumber as visonex_patient_id,
        CONCAT(
            patientidnumber,
             '-', '',
             '-', clinicorganizationidnumber
            ) as claim_id,
        'PATIENTINFECTION_LABORGANISM' as claim_type,
        NULL AS diagnosis_code,
        NULL AS diagnosis_code_qual,
        NULL AS procedure_code,
        NULL as ndc_code,
        NULL as date_service
    FROM patientinfection_laborganism
    UNION
    SELECT patientdataanalyticrowidnumber as visonex_patient_id,
        CONCAT(
            patientidnumber,
             '-', '',
             '-', clinicorganizationidnumber
            ) as claim_id,
        'PATIENTINFECTION_LABORGANISMDRUG' as claim_type,
        NULL AS diagnosis_code,
        NULL AS diagnosis_code_qual,
        NULL AS procedure_code,
        NULL as ndc_code,
        NULL as date_service
    FROM patientinfection_laborganismdrug
    UNION
    SELECT patientdataanalyticrowidnumber as visonex_patient_id,
        CONCAT(
            patientidnumber,
             '-', '',
             '-', clinicorganizationidnumber
            ) as claim_id,
        'PATIENTINFECTION_MEDICATION' as claim_type,
        NULL AS diagnosis_code,
        NULL AS diagnosis_code_qual,
        NULL AS procedure_code,
        NULL as ndc_code,
        NULL as date_service
    FROM patientinfection_medication
    UNION
    SELECT patientdataanalyticrowidnumber as visonex_patient_id,
        CONCAT(
            patientidnumber,
             '-', analyticdos,
             '-', clinicorganizationidnumber
            ) as claim_id,
        'PATIENTINFECTION_LABRESULTCULTURE' as claim_type,
        NULL AS diagnosis_code,
        NULL AS diagnosis_code_qual,
        NULL AS procedure_code,
        NULL as ndc_code,
        analyticdos as date_service
    FROM patientinfection_labresultculture
    UNION
    SELECT patientdataanalyticrowidnumber as visonex_patient_id,
        CONCAT(
            patientidnumber,
             '-', '',
             '-', clinicorganizationidnumber
            ) as claim_id,
        'PATIENTINSTABILITYHISTORY' as claim_type,
        NULL AS diagnosis_code,
        NULL AS diagnosis_code_qual,
        NULL AS procedure_code,
        NULL as ndc_code,
        NULL as date_service
    FROM patientinstabilityhistory
    UNION
    SELECT patientdataanalyticrowidnumber as visonex_patient_id,
        CONCAT(
            patientidnumber,
             '-', '',
             '-', clinicorganizationidnumber
            ) as claim_id,
        'PATIENTMASTERSCHEDULEHEADER' as claim_type,
        NULL AS diagnosis_code,
        NULL AS diagnosis_code_qual,
        NULL AS procedure_code,
        NULL as ndc_code,
        NULL as date_service
    FROM patientmasterscheduleheader
    UNION
    SELECT patientdataanalyticrowidnumber as visonex_patient_id,
        CONCAT(
            patientidnumber,
             '-', analyticdos,
             '-', clinicorganizationidnumber
            ) as claim_id,
        'PATIENTMEDNOTGIVEN' as claim_type,
        NULL AS diagnosis_code,
        NULL AS diagnosis_code_qual,
        NULL AS procedure_code,
        NULL as ndc_code,
        analyticdos as date_service
    FROM patientmednotgiven
    UNION
    SELECT patientdataanalyticrowidnumber as visonex_patient_id,
        CONCAT(
            patientidnumber,
             '-', analyticdos,
             '-', clinicorganizationidnumber
            ) as claim_id,
        'PATIENTSTATUSHISTORY' as claim_type,
        NULL AS diagnosis_code,
        NULL AS diagnosis_code_qual,
        NULL AS procedure_code,
        NULL as ndc_code,
        analyticdos as date_service
    FROM patientstatushistory
    UNION
    SELECT patientdataanalyticrowidnumber as visonex_patient_id,
        CONCAT(
            patientidnumber,
             '-', analyticdos,
             '-', clinicorganizationidnumber
            ) as claim_id,
        'PATIENTCMS2728' as claim_type,
        NULL AS diagnosis_code,
        NULL AS diagnosis_code_qual,
        NULL AS procedure_code,
        NULL as ndc_code,
        analyticdos as date_service
    FROM patientcms2728
        ) big_union ON CONCAT(
        base.patientidnumber,
        '-', base.analyticdos,
        '-', base.clinicorganizationidnumber
        ) = big_union.claim_id
    LEFT JOIN matching_payload mp ON big_union.visonex_patient_id = mp.claimid
