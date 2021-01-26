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
    min_date = datetime.date(2014, 1, 1)
    max_date = datetime.date(2017, 7, 27)
    for row in results:
        if row.event_date is not None:
            assert min_date <= row.event_date <= max_date


def test_event_val_mapping():
    #TODO: Put more robust tests here once we have whitelist mapping
    for row in results:
        assert row.event_val is None


def test_event_val_uom_mapping():
    #TODO: Put more robust tests here once we have whitelist mapping
    for row in results:
        assert row.event_val_uom is None


def test_source_record_id_not_null():
    for row in results:
        assert row.source_record_id is not None


def test_state_codes_upper_case():
    for row in results:
        assert row.patient_state.isupper()


def test_cleanup(spark):
    cleanup(spark)

