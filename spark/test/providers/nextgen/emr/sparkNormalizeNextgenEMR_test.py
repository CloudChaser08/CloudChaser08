import pytest

import datetime

import spark.providers.nextgen.emr.sparkNormalizeNextgenEMR as nextgen

results = {}
deduped_encounter = []
deduped_demographics = []
tables = ['clinical_observation', 'diagnosis', 'encounter', 'lab_order',
            'lab_result', 'medication', 'procedure', 'provider_order',
            'vital_sign']

def cleanup(spark):
    for t in tables:
        spark['sqlContext'].dropTempTable('{}_common_model'.format(t))


@pytest.mark.usefixtures("spark")
def test_init(spark):
    cleanup(spark)
    nextgen.run(spark['spark'], spark['runner'], '2017-02-28', True)
    global results, deduped_encounter, deduped_demographics
    for t in tables:
        results[t] = spark['sqlContext'].sql('select * from {}_common_model'.format(t)) \
                                 .collect()
    deduped_encounter = spark['sqlContext'].sql('select * from encounter_dedup') \
                            .collect()
    deduped_demographics = spark['sqlContext'].sql('select * from demographics_dedup') \
                            .collect()


def test_hvid():
    "Ensure that all the tables contain an expected HVID"

    global results
    for t in tables:
        assert len(filter(lambda r: r.hvid == "NG_00033_00000109", results[t])) >= 1

def test_enc_id():
    "Ensure that all the tables contain an expected encounter ID"

    global results
    for t in tables:
        assert len(filter(lambda r: r.hv_enc_id == "35_00033_285182", results[t])) >= 1

def test_encounter_demographics_deduped():
    "Ensure that all the encounter and demographics entries were deduped"

    global deduped_encounter, deduped_demographics
    assert len(deduped_encounter) == 1
    assert len(deduped_demographics) == 1

def test_lab_order_diagnoses_split():
    """Ensure that diagnoses in lab order records are split, put on individual rows,
    and transformed/filtered in accordance with diagnosis rules"""

    global results
    assert len(filter(lambda r: r.lab_ord_diag_cd == 'J4520', results['lab_order'])) == 1
    assert len(filter(lambda r: r.lab_ord_diag_cd == 'E782', results['lab_order'])) == 1
    assert len(filter(lambda r: r.lab_ord_diag_cd == 'V200XXA', results['lab_order'])) == 0

def test_diagnosis_diagnosis_code_transformation():
    """Ensure that diagnosis codes in the diagnosis table are transformed/filtered
    in accordance with diagnosis rules"""

    global results
    assert len(filter(lambda r: r.diag_cd == 'E782', results['diagnosis'])) == 1
    assert len(filter(lambda r: r.diag_cd == 'V200XXA', results['diagnosis'])) == 0

def test_lipid_msrmt_mapping():
    """Ensure that the lipid-related test results are properly mapped"""

    global results
    assert len(filter(lambda r: r.lab_test_panel_nm == 'LIPID_PANEL', results['lab_result'])) == 4
    assert len(filter(lambda r: r.lab_result_qual == 'LDL_CHOLESTEROL'
            and r.lab_result_msrmt == '147.00', results['lab_result'])) \
        == 1
    assert len(filter(lambda r: r.lab_result_qual == 'TOTAL_CHOLESTEROL'
            and r.lab_result_msrmt == '235.00', results['lab_result'])) \
        == 1

def test_medication_diagnosis_split():
    """Ensure that diagnoses in medication records are split, put on individual rows,
    and transformed/filtered in accordance with diagnosis rules"""

    global results
    assert len(filter(lambda r: r.medctn_diag_cd == 'J4520', results['medication'])) == 1
    assert len(filter(lambda r: r.medctn_diag_cd == 'E782', results['medication'])) == 1
    assert len(filter(lambda r: r.medctn_diag_cd == 'V200XXA', results['medication'])) == 0

def test_medication_ndc():
    """Ensure that ndc codes are properly mapped and transformed"""

    global results
    assert len(filter(lambda r: r.medctn_ndc == '00378395105', results['medication'])) == 3
    assert len(filter(lambda r: r.medctn_ndc == '0037839510512', results['medication'])) == 0

def test_procedure_cpt():
    """Ensure that cpt codes are properly mapped and transformed"""

    global results
    assert len(filter(lambda r: r.proc_cd == '36415', results['procedure'])) == 1

def test_provider_order_actcode():
    """Ensure that only whitelisted values are allowed in provider order code column"""

    global results
    # CPT codes
    assert len(filter(lambda r: r.prov_ord_cd == '36415', results['provider_order'])) == 1

    # ICD codes (won't show up because punctuation will cause whitelisting to fail)
    assert len(filter(lambda r: r.prov_ord_cd == 'E782', results['provider_order'])) == 0
    assert len(filter(lambda r: r.prov_ord_cd == 'V200XXA', results['provider_order'])) == 0

    # Random data
    assert len(filter(lambda r: r.prov_ord_cd == 'Einstein', results['provider_order'])) == 0

def test_vital_sign_msrmt():
    """Ensure that vital signs are proprely mapped and filtered in accordance
    with privacy rules"""

    global results
    assert len(filter(lambda r: r.vit_sign_typ_cd == 'WEIGHT'
            and r.vit_sign_msrmt == '170.0', results['vital_sign'])) == 1

    assert len(filter(lambda r: r.vit_sign_typ_cd == 'WEIGHT'
            and r.vit_sign_msrmt == '340.0', results['vital_sign'])) == 0

def test_cleanup(spark):
    cleanup(spark)
