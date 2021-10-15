import datetime
import pytest

import spark.providers.acxiom.events.normalize as ac

results = []


def cleanup(spark):
    spark['sqlContext'].dropTempTable('acxiom_norm_event')


@pytest.mark.usefixtures('spark')
def test_init(spark):
    ac.run('2017-10-06', test=True, spark=spark['spark'], runner=spark['runner'])
    global results
    results = spark['sqlContext'].sql('select * from acxiom_norm_event').collect()


def test_all_rows_are_normalized():
    assert len(results) == 10


def test_source_record_id_is_mapped():
    for row in results:
        assert row.source_record_id is not None


def test_target_logical_delete_reason_is_set_if_id_found_in_ids_table():
    assert [x for x in results if x.source_record_id == 'c'][0].logical_delete_reason == 'DELETE'


def test_cleanup(spark):
    cleanup(spark)
