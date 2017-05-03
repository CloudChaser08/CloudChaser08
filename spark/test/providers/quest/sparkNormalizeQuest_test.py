import pytest

import datetime

import spark.providers.quest.sparkNormalizeQuest as quest

results = []


@pytest.mark.usefixtures("spark")
def test_init(spark):
    quest.run(spark['spark'], spark['runner'], '2016-12-31', True)
    global results
    results = spark['sqlContext'].sql('select * from lab_common_model') \
                                 .collect()


def test_year_of_birth_cap(spark):
    "Ensure that year of birth capping was applied"
    assert filter(lambda r: r.claim_id == '2073344007_17897', results)[0] \
        .patient_year_of_birth == '1927'


def test_date_parsing(spark):
    "Ensure that dates are correctly parsed"
    assert filter(lambda r: r.claim_id == '2073344007_17897', results)[0] \
        .date_service == datetime.date(2016, 12, 1)


def test_diag_explosion(spark):
    "Ensure that diagnosis codes were exploded on '^'"
    diags = map(
        lambda r: str(r.diagnosis_code),
        filter(lambda r: r.claim_id == '2073344012_17897', results)
    )
    diags.sort()

    assert diags == ['DIAG1', 'DIAG4', 'DIAG6']


def test_nodiag_inclusion(spark):
    "Ensure that claims with no diagnosis codes were included"
    claim = filter(lambda r: r.claim_id == '2073344008_17897', results)

    assert len(claim) == 1


def test_diagnosis_qual_translation(spark):
    "Ensure that diagnosis code qualifiers were correctly translated to HV"
    assert filter(lambda r: r.claim_id == '2073344009_17897', results)[0] \
        .diagnosis_code_qual == '02'

    assert filter(lambda r: r.claim_id == '2073344012_17897', results)[0] \
        .diagnosis_code_qual == '01'
