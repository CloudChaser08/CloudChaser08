SELECT
    pay.hvid                                                AS hvid,
    COALESCE(SUBSTR(ptn.gender, 1, 1), pay.gender)          AS patient_gender,
    pay.age                                                 AS patient_age,
    pay.yearOfBirth                                         AS patient_year_of_birth,
    SUBSTR(COALESCE(ptn.zip, pay.threeDigitZip), 1, 3)      AS patient_zip3,
    COALESCE(ptn.stateprovidence, pay.state, '')            AS patient_state,
    UPPER(gns.genename)                                     AS test_ordered_name,
    UPPER(gns.pkphenotype)                                  AS result_name,
    'PHARMACOKINETIC_PHENOTYPE'                             AS result_desc,
    diag.icdcode                                            AS diagnosis_code,
    CASE
        WHEN diag.icdcodetype = '9' THEN '01'
        WHEN diag.icdcodetype = '10' THEN '02'
        ELSE NULL
    END                                                     AS diagnosis_code_qual,
    cln.providerid                                          AS ordering_npi,
    CONCAT(
        cln.lastname,
        CASE
            WHEN LENGTH(cln.lastname) <> 0 AND LENGTH(cln.firstname) <> 0 THEN ','
            ELSE ''
        END,
        cln.firstname
    )                                                       AS ordering_name,
    cln.specialization                                      AS ordering_specialty,
    cln.city                                                AS ordering_city,
    cln.state                                               AS ordering_state,
    cln.zipcode                                             AS ordering_zip,
    med.genericname                                         AS medication_generic_name,
    med.dosage                                              AS medication_dose
FROM patient ptn
LEFT OUTER JOIN matching_payload pay ON ptn.hvJoinKey = pay.hvJoinKey
LEFT OUTER JOIN clinicians_explode cln ON ptn.patientkey = cln.patientkey
LEFT OUTER JOIN diagnosis_dedup diag on ptn.patientkey = diag.patientkey
LEFT OUTER JOIN genes_dedup gns ON ptn.patientkey = gns.patientkey
LEFT OUTER JOIN medications_dedup med ON ptn.patientkey = med.patientkey
WHERE
UPPER(COALESCE(ptn.country, 'EMPTY')) = 'USA'
AND
UPPER(COALESCE(cln.country, 'USA')) = 'USA'
