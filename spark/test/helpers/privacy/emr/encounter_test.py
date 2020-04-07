import pytest
from spark.helpers.privacy.common import Transformer, TransformFunction
import spark.helpers.privacy.emr.encounter as encounter_priv
from pyspark.sql.types import StructField, StructType, StringType, Row


@pytest.mark.usefixtures("spark")
def test_init(spark):
    # whitelists
    spark['spark'].sparkContext.parallelize([
        ['GOODVAL', 'Y', 'emr_enc.enc_typ_nm'],
        ['GOODVAL', 'Y', 'emr_enc.enc_desc'],
        ['DUMMYVAL', 'Y', 'emr_enc_test.notransform']
    ]).toDF(StructType([
        StructField('gen_ref_itm_nm', StringType()),
        StructField('whtlst_flg', StringType()),
        StructField('gen_ref_domn_nm', StringType())
    ])).createOrReplaceTempView('ref_gen_ref')


def test_filter(spark):
    # test df including commonly filtered fields
    test_df = spark['spark'].sparkContext.parallelize([
        ['100', '1880', '2017-01-01', 'dummyval', 'GOODVAL', 'badval'],
        ['100', '1880', '2017-01-01', 'dummyval2', 'badval', 'goodval']
    ]).toDF(StructType([
        StructField('ptnt_age_num', StringType()),
        StructField('ptnt_birth_yr', StringType()),
        StructField('enc_start_dt', StringType()),
        StructField('notransform', StringType()),
        StructField('enc_typ_nm', StringType()),
        StructField('enc_desc', StringType())
    ]))

    # assertion with no additional transforms and no whitelisting
    assert encounter_priv.filter(spark['sqlContext'])(test_df).collect() \
        == [Row('90', '1927', '2017-01-01', 'dummyval', 'GOODVAL', 'badval'),
            Row('90', '1927', '2017-01-01', 'dummyval2', 'badval', 'goodval')]

    # save original state of built-in transformer
    old_transformer = Transformer(**dict(encounter_priv.encounter_transformer.transforms))
    old_whitelists = list(encounter_priv.whitelists)

    def whitelist_update(whitelist):
        return whitelist + [{
            'column_name': 'notransform',
            'domain_name': 'emr_enc_test.notransform'
        }, {
            'column_name': 'enc_typ_nm',
            'domain_name': 'emr_enc.enc_typ_nm'
        }, {
            'column_name': 'enc_desc',
            'domain_name': 'emr_enc.enc_desc'
        }]

    # assertion including additional transforms and whitelisting
    assert encounter_priv.filter(
        spark['sqlContext'],
        update_whitelists=whitelist_update,
        additional_transformer=Transformer(
            enc_typ_nm=[
                TransformFunction(lambda c: c.replace('bad', 'good'), ['enc_typ_nm'])
            ]
        ))(test_df).collect() == [Row('90', '1927', '2017-01-01', 'DUMMYVAL', 'GOODVAL', None),
                                  Row('90', '1927', '2017-01-01', None, 'GOODVAL', 'GOODVAL')]

    # assert original transformer and whitelist was not modified by
    # additional args
    assert encounter_priv.encounter_transformer.transforms == old_transformer.transforms
    assert encounter_priv.whitelists == old_whitelists
