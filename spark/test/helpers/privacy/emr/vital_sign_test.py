import pytest
from spark.helpers.privacy.common import Transformer
import spark.helpers.privacy.emr.vital_sign as vital_sign_priv
from pyspark.sql.types import StructField, StructType, StringType, Row


@pytest.mark.usefixtures("spark")
def test_init(spark):
    # whitelists
    spark['spark'].sparkContext.parallelize([
        ['DUMMYVAL', 'Y', 'emr_vit_sign_test.notransform']
    ]).toDF(StructType([
        StructField('gen_ref_itm_nm', StringType()),
        StructField('whtlst_flg', StringType()),
        StructField('gen_ref_domn_nm', StringType())
    ])).createOrReplaceTempView('ref_gen_ref')


def test_filter(spark):
    # test df including commonly filtered fields
    test_df = spark['spark'].sparkContext.parallelize([
        ['100', '1880', '2017-01-01', 'dummyval'],
        ['100', '1880', '2017-01-01', 'dummyval2']
    ]).toDF(StructType([
        StructField('ptnt_age_num', StringType()),
        StructField('ptnt_birth_yr', StringType()),
        StructField('enc_dt', StringType()),
        StructField('notransform', StringType())
    ]))

    # assertion with no additional transforms
    assert vital_sign_priv.filter(spark['sqlContext'])(test_df).collect() \
        == [Row('90', '1927', '2017-01-01', 'dummyval'),
            Row('90', '1927', '2017-01-01', 'dummyval2')]

    # save original state of built-in transformer
    old_transformer = Transformer(**dict(vital_sign_priv.vital_sign_transformer.transforms))
    old_whitelists = list(vital_sign_priv.whitelists)

    def whitelist_update(whitelist):
        return whitelist + [{
            'column_name': 'notransform',
            'domain_name': 'emr_vit_sign_test.notransform'
        }]

    # assertion including additional transforms
    assert vital_sign_priv.filter(
        spark['sqlContext'],
        update_whitelists=whitelist_update,
    )(test_df).collect() == [Row('90', '1927', '2017-01-01', 'DUMMYVAL'),
                             Row('90', '1927', '2017-01-01', None)]

    # assert original transformer and whitelist was not modified by
    # additional args
    assert vital_sign_priv.vital_sign_transformer.transforms == old_transformer.transforms
    assert vital_sign_priv.whitelists == old_whitelists
