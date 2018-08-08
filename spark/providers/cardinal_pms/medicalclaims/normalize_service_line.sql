SELECT
    t.ediclaim_id                              AS claim_id,
    t.hvid                                     AS hvid,
    '08'                                       AS model_version,
    t.tenant_id                                AS vendor_org_id,
    COALESCE(p.gender, t.patientgender)        AS patient_gender,
    COALESCE(p.yearOfBirth,
             YEAR(t.patientdob)
    )                                          AS patient_year_of_birth,
    'P'                                        AS claim_type,
    extract_date(
        t.dateservicestart,
        '%Y%m%d'
    )                                          AS date_service,
    t.facilitycode                             AS place_of_service_std_id,
    t.linesequencenumber                       AS service_line_number,
    t.id_3                                     AS service_line_id,
    ARRAY(t.principaldiagnosis, t.diagnosistwo,
        t.diagnosisthree, t.diagnosisfour, t.diagnosisfive,
        t.diagnosissix, t.diagnosisseven, t.diagnosiseight,
        t.diagnosisnine, t.diagnosisten, t.diagnosiseleven,
        t.diagnosistwelve
        )
        [
            CAST(
            ARRAY(
                t.linkeddiagnosisone, t.linkeddiagnosistwo,
                t.linkeddiagnosisthree, t.linkeddiagnosisfour,
                NULL)[sl_explode.n]
                AS INTEGER
            ) - 1
        ]                                      AS diagnosis_code,
    CASE
        WHEN ARRAY(t.linkeddiagnosisone, t.linkeddiagnosistwo,
            t.linkeddiagnosisthree, t.linkeddiagnosisfour,
            NULL)[sl_explode.n] IS NOT NULL
            THEN sl_explode.n + 1
        ELSE NULL 
    END                                        AS diagnosis_priority,
    t.procedurecode                            AS procedure_code,
    t.procedurecodequalifier                   AS procedure_code_qual,
    t.submittedunits                           AS procedure_units_billed,
    t.proceduremodifierone                     AS procedure_modifier_1,
    t.proceduremodifiertwo                     AS procedure_modifier_2,
    t.proceduremodifierthree                   AS procedure_modifier_3,
    t.proceduremodifierfour                    AS procedure_modifier_4,
    CASE
        WHEN t.product_service_id_qualifier = 'N4' THEN product_service_id
        ELSE NULL
    END                                        AS ndc_code,
    t.submittedcharge                          AS line_charge,
    t.submittedchargetotal                     AS total_charge,
    CASE
        WHEN t.renderingprovideridqualifier = 'XX' AND
             11 = LENGTH(TRIM(COALESCE(t.renderingprovidernpid, '')))
             THEN COALESCE(t.renderingproviderid, t.renderingprovidernpid)
        WHEN t.renderingprovideridqualifier = 'XX' 
             THEN t.renderingproviderid
        ELSE NULL
    END                                        AS prov_rendering_npi,
    CASE
        WHEN t.billprovideridqualifier = 'XX' AND
             11 = LENGTH(TRIM(COALESCE(t.billprovidernpid, '')))
             THEN COALESCE(t.billproviderid, t.billprovidernpid)
        WHEN t.billprovideridqualifier = 'XX'
             THEN t.billproviderid
        ELSE NULL
    END                                        AS prov_billing_npi,
    CASE
        WHEN t.referringprovideridqualifier = 'XX'
             THEN t.referringproviderid
        ELSE NULL
    END                                        AS prov_referring_npi,
    CASE
        WHEN t.servicefacilityidqualifier = 'XX'
             THEN t.servicefacilityid
        ELSE NULL
    END                                        AS prov_facility_npi,
    t.payerid                                  AS payer_vendor_id,
    t.payername                                AS payer_name,
    CASE
        WHEN t.renderingprovideridqualifier <> 'XX'
             AND 0 <> LENGTH(TRIM(COALESCE(t.renderingproviderid, '')))
             THEN t.renderingproviderid
        ELSE NULL
    END                                        AS prov_rendering_vendor_id,
    t.renderingprovidername                    AS prov_rendering_name_1,
    t.renderingprovidertaxonomycode            AS prov_rendering_std_taxonomy,
    CASE
        WHEN t.billprovideridqualifier <> 'XX'
             AND 0 <> LENGTH(TRIM(COALESCE(t.billproviderid, '')))
             THEN t.billproviderid
        ELSE NULL
    END                                        AS prov_billing_vendor_id,
    t.billprovidername                         AS prov_billing_name_1,
    t.billprovidertaxonomycode                 AS prov_billing_std_taxonomy,
    CASE
        WHEN t.referringprovideridqualifier <> 'XX'
             AND 0 <> LENGTH(TRIM(COALESCE(t.referringproviderid, '')))
             THEN referringproviderid
        ELSE NULL
    END                                        AS prov_referring_vendor_id,
    t.referringprovidername                    AS prov_referring_name_1,
    CASE
        WHEN t.servicefacilityidqualifier <> 'XX'
             AND 0 <> LENGTH(TRIM(COALESCE(t.servicefacilityid, '')))
             THEN t.servicefacilityid
        ELSE NULL
    END                                        AS prov_facility_vendor_id,
    t.servicefacilityname                      AS prov_facility_name_1,
    t.servicefacilityaddress                   AS prov_facility_address_1,
    t.servicefacilitycity                      AS prov_facility_city,
    t.servicefacilitystate                     AS prov_facility_state,
    t.servicefacilityzip                       AS prov_facility_zip
FROM transactional_cardinal_pms t
    LEFT OUTER JOIN matching_payload p
    ON t.hvJoinKey = p.hvJoinKey
    CROSS JOIN service_line_exploder sl_explode
WHERE
    -- Filter out cases from explosion where diagnosis_code is NULL
    (  
        ARRAY(t.linkeddiagnosisone, t.linkeddiagnosistwo,
            t.linkeddiagnosisthree, t.linkeddiagnosisfour)[sl_explode.n]
        IS NOT NULL 
    ) 
    OR 
    -- If all are NULL, include one row w/ NULL diagnosis_code
    ( 
        COALESCE(t.linkeddiagnosisone, t.linkeddiagnosistwo,
        t.linkeddiagnosisthree, t.linkeddiagnosisfour) IS NULL
        AND
        sl_explode.n = 0
    )
