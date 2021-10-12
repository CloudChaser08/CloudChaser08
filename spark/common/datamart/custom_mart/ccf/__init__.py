""" Custom schemas"""
from spark.common.datamart.custom_mart.ccf \
    import clinical_observation_crf, clinical_observation_emr, demography, \
    diagnosis_crf, diagnosis_emr, encounter_crf, encounter_emr, \
    lab_test_crf, lab_test_emr, \
    patient_history, patient_problem, \
    prescription_crf, prescription_emr, \
    procedure_crf, procedure_emr, \
    vaccine_crf, vaccine_emr

schemas = {
    'clinical_observation_crf': clinical_observation_crf.schema,
    'clinical_observation_emr': clinical_observation_emr.schema,
    'demography': demography.schema,
    'diagnosis_crf': diagnosis_crf.schema,
    'diagnosis_emr': diagnosis_emr.schema,
    'encounter_crf': encounter_crf.schema,
    'encounter_emr': encounter_emr.schema,
    'lab_test_crf': lab_test_crf.schema,
    'lab_test_emr': lab_test_emr.schema,
    'patient_history': patient_history.schema,
    'patient_problem': patient_problem.schema,
    'prescription_crf': prescription_crf.schema,
    'prescription_emr': prescription_emr.schema,
    'procedure_crf': procedure_crf.schema,
    'procedure_emr': procedure_emr.schema,
    'vaccine_crf': vaccine_crf.schema,
    'vaccine_emr': vaccine_emr.schema
}
