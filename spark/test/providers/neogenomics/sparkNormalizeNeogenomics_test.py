import pytest

import datetime

import spark.providers.neogenomics.sparkNormalizeNeogenomics as neogenomics

results = []


@pytest.mark.usefixtures("spark")
def test_init(spark):
    neogenomics.run(spark['spark'], spark['runner'], '2016-12-31', True)
    global results
    results = spark['sqlContext'].sql('select * from lab_common_model') \
                                 .collect()


def test_date_parsing(spark):
    "Ensure that dates are correctly parsed"
    assert filter(lambda r: r.claim_id == 'test1', results)[0] \
        .date_service == datetime.date(2017, 4, 14)


def test_diag_explosion(spark):
    "Ensure that diagnosis codes were exploded on '^'"
    diags = map(
        lambda r: str(r.diagnosis_code),
        filter(lambda r: r.claim_id == 'test2', results)
    )
    diags.sort()

    assert diags == ['DIAG1', 'DIAG2', 'DIAG3']


def test_nodiag_inclusion(spark):
    "Ensure that claims with no diagnosis codes were included"
    claim = filter(lambda r: r.claim_id == 'test3', results)

    assert len(claim) == 1
