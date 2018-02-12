SELECT
    patient_deid.hvid                         AS hvid,
    6                                         AS model_version,
    CASE
        WHEN UPPER(TRIM(COALESCE(patient_deid.gender, patient.gender)))
            IN ('F', 'M')
            THEN UPPER(TRIM(COALESCE(patient_deid.gender, patient.gender)))
        ELSE 'U'
    END                                       AS patient_gender,
    patient_deid.yearOfBirth                  AS patient_year_of_birth,
    extract_date(
        med.clinicalorderdate,
        '%Y%m%d',
        CAST({min_date} AS DATE),
        CAST({max_date} AS DATE)
    )                                         AS date_service,
    UPPER(med.jcode)                          AS procedure_code,
    med.ndc                                   AS ndc_code,
    med.quantity                              AS dispensed_quantity,
    UPPER(med.dosage_uom)                     AS unit_of_measure,
    med.unitquantity                          AS unit_dose_indicator
FROM cardinal_vitalpath_med med
        LEFT OUTER JOIN cardinal_vitalpath_med_deid med_deid ON med.hvJoinKey = med_deid.hvJoinKey
        LEFT OUTER JOIN cardinal_vitalpath_patient_deid patient_deid ON med_deid.patientId = patient_deid.patientId
        LEFT OUTER JOIN cardinal_vitalpath_patient patient ON patient.hvJoinKey = patient_deid.hvJoinKey
