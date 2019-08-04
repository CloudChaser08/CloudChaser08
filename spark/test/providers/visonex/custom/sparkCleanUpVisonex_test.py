import pytest

import spark.providers.visonex.sparkCleanUpVisonex as visonex

results = []

def cleanup(spark):
    for table in visonex.TABLES:
        try:
            spark['sqlContext'].sql('DROP VIEW IF EXISTS {}'.format(table))
        except:
            spark['sqlContext'].sql('DROP TABLE IF EXISTS {}'.format(table))


@pytest.mark.usefixtures("spark")
def test_init(spark):
    cleanup(spark)
    visonex.run(spark['spark'], spark['runner'], '2017-04-01', True)
    global results
    results = spark['sqlContext'].sql('select * from clean_patientdata') \
                                        .collect()

def test_populated():
    assert len(results) == 10

def test_contains_hvids():
    assert [r for r in results if r.hvid == '5'][0] \
        .analyticrowidnumber == '50'

def test_zip_truncating():
    assert [r for r in results if r.hvid == '7'][0] \
        .zipcode == "991"

def test_cleanup(spark):
    cleanup(spark)
