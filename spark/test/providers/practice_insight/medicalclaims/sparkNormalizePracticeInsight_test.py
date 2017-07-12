import pytest

import datetime

import spark.providers.practice_insight.medicalclaims.sparkNormalizePracticeInsight \
    as practice_insight

results = []


def get_rows_for_test(claim_id):
    return filter(
        lambda r: r.claim_id == claim_id,
        results
    )


@pytest.mark.usefixtures("spark")
def test_init(spark):
    practice_insight.run_part(
        spark['spark'], spark['runner'], '1', '2016-12-31', 40, True
    )

    global results
    results = spark['sqlContext'].sql(
        'select * from medicalclaims_common_model'
    ).collect()


def test_diag_code_explosion_p_all_present():
    "Ensure all diagnosis codes are present"
    distinct_diags = list(set(
        map(
            lambda r: r.diagnosis_code,
            get_rows_for_test('diagnosis_explosion_P')
        )
    ))
    distinct_diags.sort()

    assert distinct_diags == ['E784', 'I10', 'J209', 'NONSLDIAG', 'Z0001']


def test_diag_code_explosion_p_service_lines():
    "Ensure service line diagnosis codes appear on correct rows"
    service_lines = map(
        lambda r: [
            r.service_line_number, r.diagnosis_code, r.diagnosis_priority
        ],
        filter(
            lambda r: r.service_line_number in ['1', '2'],
            get_rows_for_test('diagnosis_explosion_P')
        )
    )
    service_lines.sort()

    assert service_lines == [['1', 'E784', '2'], ['1', 'I10', '1'],
                             ['2', 'J209', '1'], ['2', 'Z0001', '2']]


def test_diag_code_explosion_p_non_service_lines():
    "Ensure non service line diagnosis codes are there too"
    non_service_lines = map(
        lambda r: r.diagnosis_code,
        filter(
            lambda r: r.service_line_number is None,
            get_rows_for_test('diagnosis_explosion_P')
        )
    )

    assert non_service_lines == ['NONSLDIAG']


def test_diag_code_explosion_p_count():
    "Ensure that diagnosis codes were exploded to the correct count"
    assert len(get_rows_for_test('diagnosis_explosion_P')) == 5


def test_diag_code_explosion_i_all_present():
    "Ensure all diagnosis codes are present"
    distinct_diags = list(set(
        map(
            lambda r: r.diagnosis_code,
            get_rows_for_test('diagnosis_explosion_I')
        )
    ))
    distinct_diags.sort()

    assert distinct_diags == [None, 'E784', 'I10', 'J209', 'NONSLDIAG', 'Z0001']


def test_diag_code_explosion_i_service_lines():
    "Ensure there are no service line diagnosis codes"
    service_lines = map(
        lambda r: [
            r.service_line_number, r.diagnosis_code, r.diagnosis_priority
        ],
        filter(
            lambda r: r.service_line_number in ['1', '2'],
            get_rows_for_test('diagnosis_explosion_I')
        )
    )
    service_lines.sort()

    assert service_lines == [['1', None, None], ['2', None, None]]


def test_diag_code_explosion_i_non_service_lines():
    "Ensure non service line diagnosis codes are there too"
    non_service_lines = map(
        lambda r: r.diagnosis_code,
        filter(
            lambda r: r.service_line_number is None,
            get_rows_for_test('diagnosis_explosion_I')
        )
    )
    non_service_lines.sort()

    assert non_service_lines == ['E784', 'I10', 'J209', 'NONSLDIAG', 'Z0001']


def test_diag_code_explosion_i_count():
    "Ensure that diagnosis codes were exploded to the correct count"
    assert len(get_rows_for_test('diagnosis_explosion_I')) == 7


def test_service_date_p_use_when_populated():
    "Ensure that the service date is used when populated"
    svc_date = map(
        lambda r: r.date_service,
        filter(
            lambda r: r.service_line_number == 'svc_populated',
            get_rows_for_test('date_service_svc_date')
        )
    )
    assert svc_date == [datetime.date(2016, 1, 1)]


def test_service_date_p_use_min():
    """
    Ensure that the min service date in the claim is used when stmnt
    and svc are null
    """
    svc_date = map(
        lambda r: r.date_service,
        filter(
            lambda r: r.service_line_number == 'svc_usemin',
            get_rows_for_test('date_service_svc_date')
        )
    )
    assert svc_date == [datetime.date(2016, 1, 1)]


def test_service_date_p_use_stmnt():
    "Ensure that the stmnt date is used when the service date is null"
    svc_date = map(
        lambda r: r.date_service,
        filter(
            lambda r: r.service_line_number == 'svc_usestmt',
            get_rows_for_test('date_service_svc_date')
        )
    )
    assert svc_date == [datetime.date(2016, 2, 1)]
