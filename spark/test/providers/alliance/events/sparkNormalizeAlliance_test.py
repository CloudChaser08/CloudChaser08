import datetime
import pytest

from pyspark.sql import Row
import spark.providers.alliance.events.sparkNormalizeAlliance as al

source = None
results = None

def cleanup(spark):
    spark['sqlContext'].dropTempTable('event_common_model')
    spark['sqlContext'].dropTempTable('ref_gen_ref')


@pytest.mark.usefixtures('spark')
def test_init(spark):
    spark['spark'].sparkContext.parallelize([
        Row(
            hvm_vdr_feed_id=al.FEED_ID,
            gen_ref_domn_nm='EARLIEST_VALID_SERVICE_DATE',
            gen_ref_itm_nm='',
            gen_ref_cd='',
            gen_ref_1_dt=datetime.date(1901, 1, 1),
            whtlst_flg=''
        ),
        Row(
            hvm_vdr_feed_id=al.FEED_ID,
            gen_ref_domn_nm='HVM_AVAILABLE_HISTORY_START_DATE',
            gen_ref_itm_nm='',
            gen_ref_cd='',
            gen_ref_1_dt=datetime.date(1901, 1, 1),
            whtlst_flg=''
        ),
        Row(
            hvm_vdr_feed_id=al.FEED_ID,
            gen_ref_domn_nm='transaction.naics_code',
            gen_ref_itm_nm='',
            gen_ref_cd='448130',
            gen_ref_1_dt=None,
            whtlst_flg='Y'
        ),
        Row(
            hvm_vdr_feed_id=al.FEED_ID,
            gen_ref_domn_nm='transaction.naics_code',
            gen_ref_itm_nm='',
            gen_ref_cd='511210',
            gen_ref_1_dt=None,
            whtlst_flg='Y'
        ),
        Row(
            hvm_vdr_feed_id=al.FEED_ID,
            gen_ref_domn_nm='transaction.naics_code',
            gen_ref_itm_nm='',
            gen_ref_cd='444110',
            gen_ref_1_dt=None,
            whtlst_flg='Y'
        ),
        Row(
            hvm_vdr_feed_id=al.FEED_ID,
            gen_ref_domn_nm='transaction.naics_code',
            gen_ref_itm_nm='',
            gen_ref_cd='722110',
            gen_ref_1_dt=None,
            whtlst_flg='Y'
        ),
        Row(
            hvm_vdr_feed_id=al.FEED_ID,
            gen_ref_domn_nm='transaction.naics_code',
            gen_ref_itm_nm='',
            gen_ref_cd='813813',
            gen_ref_1_dt=None,
            whtlst_flg='Y'
        )
    ]).toDF().createOrReplaceTempView('ref_gen_ref')

    al.run(spark['spark'], spark['runner'], '2017-10-06', None, test=True)
    global results
    results = spark['sqlContext'] \
                .sql('select * from event_common_model') \
                .collect()


def test_that_event_category_fields_are_nullified_when_naisc_code_starts_with_813():
    row_of_interest = [x for x in results if x.hvid == '7'][0]
    assert row_of_interest.event_category_code is None


def test_that_event_category_fields_are_populated_properly():
    has_cat = [x for x in results if x.event_category_code is not None]
    no_cat = [x for x in results if x.event_category_code is None]

    for r in has_cat:
        assert r.event_category_code_qual == 'NAICS_CODE'
        assert r.event_category_name is not None

    for r in no_cat:
        assert r.event_category_code_qual is None
        assert r.event_category_name is None


def test_that_event_category_flag_fields_are_populated_properly():
    has_flag = [x for x in results if x.event_category_flag is not None]
    no_flag = [x for x in results if x.event_category_flag is None]

    for r in has_flag:
        assert r.event_category_flag_qual == 'CNP_IND'

    for r in no_flag:
        assert r.event_category_flag_qual is None


def test_that_naics_codes_not_on_whitelist_are_nullified():
    row_of_interest = [x for x in results if x.hvid == '8'][0]
    assert row_of_interest.event_category_code is None


def test_actives():
    assert sorted([(r.hvid, r.event_date, r.logical_delete_reason) for r in results]) == [
        ('1', datetime.date(2016, 1, 1), None),
        ('10', None, None),
        ('2', datetime.date(2016, 1, 15), None),
        ('3', datetime.date(2016, 1, 13), None),
        ('4', datetime.date(2016, 1, 13), None),
        ('5', datetime.date(2016, 1, 5), None),
        ('6', datetime.date(2016, 1, 30), None),
        ('7', datetime.date(2016, 1, 10), None),
        ('8', datetime.date(2016, 1, 8), None),
        ('9', datetime.date(2016, 1, 31), 'Inactive')
    ]


def test_cleanup(spark):
    cleanup(spark)
