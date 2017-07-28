import datetime
import pytest

import spark.providers.mindbody.sparkNormalizeMindBody as mindbody

results = []

def cleanup(spark):
    spark['sqlContext'].dropTempTable('event_common_model')


@pytest.mark.usefixtures("spark")
def test_init(spark):
    mindbody.run(spark['spark'], spark['runner'], '2017-07-27', test=True)
    global results
    results = spark['sqlContext'].sql('select * from event_common_model') \
                                 .collect()


def test_event_date_capping():
    min_date = datetime.date(2016, 1, 1)
    max_date = datetime.date(2017, 7, 27)
    for row in results:
        if row.event_date is not None:
            row_date = datetime.datetime.strptime(row.event_date, '%Y-%m-%d').date()
            assert row_date >= min_date and row_date <= max_date 


def test_event_val_mapping():
    #TODO: Put more robust tests here once we have whitelist mapping
    for row in results:
        assert row.event_val is None


def test_event_val_uom_mapping():
    #TODO: Put more robust tests here once we have whitelist mapping
    for row in results:
        assert row.event_val_uom is None


def test_cleanup(spark):
    cleanup(spark)

