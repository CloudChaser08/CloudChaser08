import pytest

import spark.providers.practice_insight.sparkNormalizePracticeInsight \
    as practice_insight
import spark.helpers.file_utils as file_utils

results = []


@pytest.mark.usefixtures("spark")
def test_init(spark):
    practice_insight.run_part(
        spark['spark'], spark['runner'], '1', '2016-12-31',
        file_utils.get_rel_path(__file__, 'resources/output/'), 40, True
    )

    global results
    results = spark['sqlContext'].sql(
        'select * from medicalclaims_common_model'
    ).collect()


# diagnosis code explosion tests
diag_explode_rows = filter(
    lambda r: r.claim_id == 'diagnosis_explosion_P',
    results
)


def test_diag_code_explosion_all_present():
    "Ensure all diagnosis codes are present"
    distinct_diags = list(set(
        map(
            lambda r: r.diagnosis_code,
            diag_explode_rows
        )
    ))
    distinct_diags.sort()

    assert distinct_diags == ['E784', 'I10', 'J209', 'NONSLDIAG', 'Z0001']


def test_diag_code_explosion_service_lines():
    "Ensure service line diagnosis codes appear on correct rows"
    service_lines = map(
        lambda r: [
            r.service_line_number, r.diagnosis_code, r.diagnosis_priority
        ],
        filter(
            lambda r: r.service_line_number in ['1', '2'],
            diag_explode_rows
        )
    )
    service_lines.sort()

    assert service_lines == [['1', 'E784', '2'], ['1', 'I10', '1'],
                              ['2', 'J209', '1'], ['2', 'Z0001', '2']]


def test_diag_code_explosion_non_service_lines():
    "Ensure non service line diagnosis codes are there too"
    non_service_lines = map(
        lambda r: r.diagnosis_code,
        filter(
            lambda r: r.service_line_number is None,
            diag_explode_rows
        )
    )

    assert non_service_lines == ['NONSLDIAG']


def test_diag_code_explosion_count():
    "Ensure that diagnosis codes were exploded to the correct count"

    assert len(diag_explode_rows) == 5
