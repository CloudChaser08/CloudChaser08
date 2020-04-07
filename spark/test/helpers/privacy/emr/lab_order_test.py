import pytest
import spark.helpers.privacy.emr.lab_order as lab_order_priv
from pyspark.sql.types import StructField, StructType, StringType, Row


@pytest.mark.usefixtures("spark")
def test_init(spark):
    # whitelists
    spark['spark'].sparkContext.parallelize([
        ['GOODVAL', 'Y', 'emr_lab_ord.lab_ord_alt_cd'],
        ['GOODVAL', 'Y', 'emr_lab_ord.lab_ord_test_nm'],
        ['DUMMYVAL', 'Y', 'emr_lab_ord_test.notransform']
    ]).toDF(StructType([
        StructField('gen_ref_itm_nm', StringType()),
        StructField('whtlst_flg', StringType()),
        StructField('gen_ref_domn_nm', StringType())
    ])).createOrReplaceTempView('ref_gen_ref')


def test_filter(spark):
    # test df including commonly filtered fields
    test_df = spark['spark'].sparkContext.parallelize([
        ['100', '1880', '2017-01-01', 'dummyval', 'GOODVAL', 'badval', None, None],
        ['100', '1880', '2017-01-01', 'dummyval2', 'badval', 'goodval', None, None]
    ]).toDF(StructType([
        StructField('ptnt_age_num', StringType()),
        StructField('ptnt_birth_yr', StringType()),
        StructField('enc_dt', StringType()),
        StructField('notransform', StringType()),
        StructField('lab_ord_alt_cd', StringType()),
        StructField('lab_ord_test_nm', StringType()),
        StructField('lab_ord_snomed_cd', StringType()),
        StructField('rec_stat_cd', StringType())
    ]))

    # assertion with no additional transforms
    assert lab_order_priv.filter(spark['sqlContext'])(test_df).collect() \
        == [Row('90', '1927', '2017-01-01', 'dummyval', 'GOODVAL', 'badval', None, None),
            Row('90', '1927', '2017-01-01', 'dummyval2', 'badval', 'goodval', None, None)]

    # save original state of built-in transformer
    old_whitelists = list(lab_order_priv.whitelists)

    def whitelist_update(whitelist):
        return whitelist + [{
            'column_name': 'notransform',
            'domain_name': 'emr_lab_ord_test.notransform'
        }, {
            'column_name': 'lab_ord_alt_cd',
            'domain_name': 'emr_lab_ord.lab_ord_alt_cd'
        }, {
            'column_name': 'lab_ord_test_nm',
            'domain_name': 'emr_lab_ord.lab_ord_test_nm'
        }, ]

    # assertion including additional transforms
    assert lab_order_priv.filter(
        spark['sqlContext'],
        update_whitelists=whitelist_update
    )(test_df).collect() == [Row('90', '1927', '2017-01-01', 'DUMMYVAL', 'GOODVAL', None, None, None),
                             Row('90', '1927', '2017-01-01', None, None, 'GOODVAL', None, None)]

    # assert original transformer and whitelist was not modified by
    # additional args
    assert lab_order_priv.whitelists == old_whitelists
