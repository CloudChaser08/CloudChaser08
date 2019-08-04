import pytest

import datetime

import spark.providers.practice_insight.medicalclaims.sparkNormalizePracticeInsight \
    as practice_insight

results = []
res2 = []


def get_rows_for_test(claim_id):
    return [r for r in results if r.claim_id == claim_id]


def clean_up(spark):
    try:
        spark['sqlContext'].sql('DROP VIEW IF EXISTS transactional_raw')
    except:
        pass
    try:
        spark['sqlContext'].sql('DROP TABLE IF EXISTS transactional_raw')
    except:
        pass
    try:
        spark['sqlContext'].sql('DROP VIEW IF EXISTS exploded_proc_codes')
    except:
        pass
    try:
        spark['sqlContext'].sql('DROP TABLE IF EXISTS exploded_proc_codes')
    except:
        pass
    try:
        spark['sqlContext'].sql('DROP VIEW IF EXISTS exploded_diag_codes')
    except:
        pass
    try:
        spark['sqlContext'].sql('DROP TABLE IF EXISTS exploded_diag_codes')
    except:
        pass

@pytest.mark.usefixtures("spark")
def test_init(spark):
    clean_up(spark)
    practice_insight.run_part(
        spark['spark'], spark['runner'], '1', '2016-12-31', 40, True
    )

    global results, res2
    results = spark['sqlContext'].sql(
        'select * from medicalclaims_common_model'
    ).collect()
    res2 = spark['sqlContext'].sql("select * from tmp where claim_id = 'no_diag_cds'").collect()


def test_diag_code_explosion_p_all_present():
    "Ensure all diagnosis codes are present"
    distinct_diags = list(set(
        [r.diagnosis_code for r in get_rows_for_test('diagnosis_explosion_P')]
    ))
    distinct_diags.sort()

    assert distinct_diags == ['E784', 'I10', 'J209', 'NONSLDIAG', 'Z0001']


def test_diag_code_explosion_p_service_lines():
    "Ensure service line diagnosis codes appear on correct rows"
    service_lines = [
        [r.service_line_number, r.diagnosis_code, r.diagnosis_priority]
        for r in
        [
            r for r in get_rows_for_test('diagnosis_explosion_P')
            if r.service_line_number in ['1', '2']
        ]
    ]
    service_lines.sort()

    assert service_lines == [['1', 'E784', '2'], ['1', 'I10', '1'],
                             ['2', 'J209', '1'], ['2', 'Z0001', '2']]


def test_diag_code_explosion_p_non_service_lines():
    "Ensure non service line diagnosis codes are there too"
    non_service_lines = [
        r.diagnosis_code
        for r in
        [
            r for r in get_rows_for_test('diagnosis_explosion_P')
            if r.service_line_number is None
        ]
    ]

    assert non_service_lines == ['NONSLDIAG']


def test_diag_code_explosion_p_count():
    "Ensure that diagnosis codes were exploded to the correct count"
    assert len(get_rows_for_test('diagnosis_explosion_P')) == 5


def test_diag_code_explosion_i_all_present():
    "Ensure all diagnosis codes are present"
    distinct_diags = set(
        [r.diagnosis_code for r in get_rows_for_test('diagnosis_explosion_I')]
    )

    assert distinct_diags == set([None, 'E784', 'I10', 'J209', 'NONSLDIAG', 'Z0001'])


def test_diag_code_explosion_i_service_lines():
    "Ensure there are no service line diagnosis codes"
    service_lines = [
        [r.service_line_number, r.diagnosis_code, r.diagnosis_priority]
        for r in
        [
            r for r in get_rows_for_test('diagnosis_explosion_I')
            if r.service_line_number in ['1', '2']
        ]
    ]
    service_lines.sort()

    assert service_lines == [['1', None, None], ['2', None, None]]


def test_diag_code_explosion_i_non_service_lines():
    "Ensure non service line diagnosis codes are there too"
    non_service_lines = [
        r.diagnosis_code
        for r in
        [
            r for r in get_rows_for_test('diagnosis_explosion_I')
            if r.service_line_number is None
        ]
    ]
    non_service_lines.sort()

    assert non_service_lines == ['E784', 'I10', 'J209', 'NONSLDIAG', 'Z0001']


def test_diag_code_explosion_i_count():
    "Ensure that diagnosis codes were exploded to the correct count"
    assert len(get_rows_for_test('diagnosis_explosion_I')) == 7


def test_service_date_p_use_when_populated():
    "Ensure that the service date is used when populated"
    svc_date = [
        r.date_service for r in
        [
            r for r in get_rows_for_test('date_service_svc_date')
            if r.service_line_number == 'svc_populated'
        ]
    ]
    assert svc_date == [datetime.date(2016, 1, 1)]


def test_service_date_p_use_min():
    """
    Ensure that the min service date in the claim is used when stmnt
    and svc are null
    """
    svc_date = [
        r.date_service for r in
        [
            r for r in get_rows_for_test('date_service_svc_date')
            if r.service_line_number == 'svc_usemin'
        ]
    ]
    assert svc_date == [datetime.date(2016, 1, 1)]


def test_service_date_p_use_stmnt():
    "Ensure that the stmnt date is used when the service date is null"
    svc_date = [
        r.date_service for r in
        [
            r for r in get_rows_for_test('date_service_svc_date')
            if r.service_line_number == 'svc_usestmt'
        ]
    ]
    assert svc_date == [datetime.date(2016, 2, 1)]

def test_row_count_no_diagnosis_priority():
    "Ensure that with diagnosis codes but no diagnosis pointers are populated"
    assert len(get_rows_for_test('no_diag_cds')) == 4

def test_clean_up(spark):
    clean_up(spark)
