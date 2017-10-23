import pytest

import spark.providers.visonex.sparkCleanUpVisonex as visonex

results = []

@pytest.mark.usefixtures("spark")
def test_init(spark):
    visonex.run(spark['spark'], spark['runner'], '2017-04-01', True)
    global results
    results = spark['sqlContext'].sql('select * from clean_patientdata') \
                                        .collect()

def test_populated():
    assert len(results) == 10

def test_contains_hvids():
    assert filter(lambda r: r.hvid == '5', results)[0] \
        .analyticrowidnumber == '50'

def test_zip_truncating():
    assert filter(lambda r: r.hvid == '7', results)[0] \
        .zipcode == "991"
