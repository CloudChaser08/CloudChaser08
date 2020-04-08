import pytest
from spark.helpers.privacy.common import Transformer, TransformFunction
import spark.helpers.privacy.emr.diagnosis as diagnosis_priv
from pyspark.sql.types import StructField, StructType, StringType, Row


@pytest.mark.usefixtures("spark")
def test_init(spark):
    # whitelists
    spark['spark'].sparkContext.parallelize([
        ['GOODVAL', 'Y', 'emr_diag.diag_nm'],
        ['GOODVAL', 'Y', 'emr_diag.diag_desc'],
        ['DUMMYVAL', 'Y', 'emr_diag_test.notransform']
    ]).toDF(StructType([
        StructField('gen_ref_itm_nm', StringType()),
        StructField('whtlst_flg', StringType()),
        StructField('gen_ref_domn_nm', StringType())
    ])).createOrReplaceTempView('ref_gen_ref')


def test_filter(spark):
    # test df including commonly filtered fields
    test_df = spark['spark'].sparkContext.parallelize([
        ['100', '1880', '2017-01-01', 'dummyval', 'GOODVAL', 'badval', None, None, None],
        ['100', '1880', '2017-01-01', 'dummyval2', 'badval', 'goodval', None, None, None]
    ]).toDF(StructType([
        StructField('ptnt_age_num', StringType()),
        StructField('ptnt_birth_yr', StringType()),
        StructField('enc_dt', StringType()),
        StructField('notransform', StringType()),
        StructField('diag_nm', StringType()),
        StructField('diag_desc', StringType()),
        StructField('diag_resltn_desc', StringType()),
        StructField('diag_stat_desc', StringType()),
        StructField('diag_meth_nm', StringType())
    ]))

    # assertion with no additional transforms and no additional whitelisting
    assert diagnosis_priv.filter(spark['sqlContext'])(test_df).collect() \
        == [Row('90', '1927', '2017-01-01', 'dummyval', 'GOODVAL', 'badval', None, None, None),
            Row('90', '1927', '2017-01-01', 'dummyval2', 'badval', 'goodval', None, None, None)]

    # save original state of built-in transformer
    old_transformer = Transformer(**dict(diagnosis_priv.diagnosis_transformer.transforms))
    old_whitelists = list(diagnosis_priv.whitelists)

    def whitelist_update(whitelist):
        return whitelist + [{
            'column_name': 'notransform',
            'domain_name': 'emr_diag_test.notransform'
        }, {
            'column_name': 'diag_nm',
            'domain_name': 'emr_diag.diag_nm'
        }, {
            'column_name': 'diag_desc',
            'domain_name': 'emr_diag.diag_desc'
        }]

    # assertion including additional transforms and whitelisting
    assert diagnosis_priv.filter(
        spark['sqlContext'],
        update_whitelists=whitelist_update,
        additional_transformer=Transformer(
            diag_nm=[
                TransformFunction(lambda c: c.replace('bad', 'good'), ['diag_nm'])
            ]
        ))(test_df).collect() == [Row('90', '1927', '2017-01-01', 'DUMMYVAL', 'GOODVAL', None, None, None, None),
                                  Row('90', '1927', '2017-01-01', None, 'GOODVAL', 'GOODVAL', None, None, None)]

    # assert original transformer and whitelist was not modified by
    # additional args
    assert diagnosis_priv.diagnosis_transformer.transforms == old_transformer.transforms
    assert diagnosis_priv.whitelists == old_whitelists
