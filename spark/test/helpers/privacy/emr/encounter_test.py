import pytest
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

    # assertion with no additional transforms
    assert encounter_priv.filter(spark['sqlContext'])(test_df).collect() \
        == [Row('90', '1927', '2017-01-01', 'dummyval', 'GOODVAL', None),
            Row('90', '1927', '2017-01-01', 'dummyval2', None, 'GOODVAL')]

    # save original state of built-in transformer
    old_transformer = dict(encounter_priv.encounter_transformer)
    old_whitelists = list(encounter_priv.whitelists)

    def whitelist_update(whtlist):
        return whtlist + [{
            'column_name': 'notransform',
            'domain_name': 'emr_enc_test.notransform'
        }]

    # assertion including additional transforms
    assert encounter_priv.filter(
        spark['sqlContext'],
        update_whitelists=whitelist_update,
        additional_transforms={
            'enc_typ_nm': {
                'func': lambda c: c.replace('bad', 'good'),
                'args': ['enc_typ_nm']
            }
        })(test_df).collect()  == [Row('90', '1927', '2017-01-01', 'DUMMYVAL', 'GOODVAL', None),
                                   Row('90', '1927', '2017-01-01', None, 'GOODVAL', 'GOODVAL')]

    # assert original transformer and whitelist was not modified by
    # additional args
    assert encounter_priv.encounter_transformer == old_transformer
    assert encounter_priv.whitelists == old_whitelists
