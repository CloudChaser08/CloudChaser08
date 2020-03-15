import datetime
import pytest

from pyspark.sql import Row
import spark.providers.acxiom.events.sparkNormalizeAcxiom as ac

source = None
results = None

def cleanup(spark):
    spark['sqlContext'].dropTempTable('event_common_model')
    spark['sqlContext'].dropTempTable('ref_gen_ref')


@pytest.mark.usefixtures('spark')
def test_init(spark):
    spark['spark'].sparkContext.parallelize([
        Row(
            hvm_vdr_feed_id=ac.FEED_ID,
            gen_ref_domn_nm='EARLIEST_VALID_SERVICE_DATE',
            gen_ref_itm_nm='',
            gen_ref_1_dt=datetime.date(1901, 1, 1),
            whtlst_flg=''
        ),
        Row(
            hvm_vdr_feed_id=ac.FEED_ID,
            gen_ref_domn_nm='HVM_AVAILABLE_HISTORY_START_DATE',
            gen_ref_itm_nm='',
            gen_ref_1_dt=datetime.date(1901, 1, 1),
            whtlst_flg=''
        )
    ]).toDF().createOrReplaceTempView('ref_gen_ref')

    ac.run(spark['spark'], spark['runner'], '2017-10-06', test=True)
    global results
    results = spark['sqlContext'] \
                .sql('select * from event_common_model') \
                .collect()


def test_all_rows_are_normalized():
    assert len(results) == 10


def test_source_record_id_is_mapped():
    for row in results:
        assert row.source_record_id is not None


def test_target_logical_delete_reason_is_set_if_id_found_in_ids_table():
    assert [x for x in results if x.source_record_id == 'c'][0].logical_delete_reason == 'DELETE'


def test_cleanup(spark):
    cleanup(spark)
