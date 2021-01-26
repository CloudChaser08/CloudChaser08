import pytest
from spark.helpers.privacy.common import Transformer, TransformFunction
import spark.helpers.privacy.emr.lab_result as lab_result_priv
from pyspark.sql.types import StructField, StructType, StringType, Row


@pytest.mark.usefixtures("spark")
def test_init(spark):
    # whitelists
    spark['spark'].sparkContext.parallelize([
        ['GOODVAL', 'Y', 'emr_lab_result.lab_test_nm'],
        ['GOODVAL', 'Y', 'SNOMED'],
        ['DUMMYVAL', 'Y', 'emr_lab_result_test.notransform']
    ]).toDF(StructType([
        StructField('gen_ref_itm_nm', StringType()),
        StructField('whtlst_flg', StringType()),
        StructField('gen_ref_domn_nm', StringType())
    ])).createOrReplaceTempView('ref_gen_ref')


def test_filter(spark):
    # test df including commonly filtered fields
    test_df = spark['spark'].sparkContext.parallelize([
        ['100', '1880', '2017-01-01', 'dummyval', 'GOODVAL', 'badval', None, None, None, None, None],
        ['100', '1880', '2017-01-01', 'dummyval2', 'badval', 'goodval', None, None, None, None, None]
    ]).toDF(StructType([
        StructField('ptnt_age_num', StringType()),
        StructField('ptnt_birth_yr', StringType()),
        StructField('enc_dt', StringType()),
        StructField('notransform', StringType()),
        StructField('lab_test_nm', StringType()),
        StructField('lab_test_snomed_cd', StringType()),
        StructField('lab_test_vdr_cd', StringType()),
        StructField('lab_test_vdr_cd_qual', StringType()),
        StructField('lab_result_nm', StringType()),
        StructField('rec_stat_cd', StringType()),
        StructField('lab_result_uom', StringType())
    ]))

    # assertion with no additional transforms
    assert lab_result_priv.filter(spark['sqlContext'])(test_df).collect() \
        == [Row('90', '1927', '2017-01-01', 'dummyval', 'GOODVAL', 'badval', None, None, None, None, None),
            Row('90', '1927', '2017-01-01', 'dummyval2', 'badval', 'goodval', None, None, None, None, None)]

    # save original state of built-in transformer
    old_transformer = Transformer(**dict(lab_result_priv.lab_result_transformer.transforms))
    old_whitelists = list(lab_result_priv.whitelists)

    def whitelist_update(whitelist):
        return whitelist + [{
            'column_name': 'notransform',
            'domain_name': 'emr_lab_result_test.notransform'
        }, {
            'column_name': 'lab_test_snomed_cd',
            'domain_name': 'SNOMED'
        }, {
            'column_name': 'lab_test_nm',
            'domain_name': 'emr_lab_result.lab_test_nm'
        }]

    # assertion including additional transforms
    assert lab_result_priv.filter(
        spark['sqlContext'],
        update_whitelists=whitelist_update,
        additional_transformer=Transformer(
            lab_test_nm=[
                TransformFunction(lambda c: c.replace('bad', 'good'), ['lab_test_nm'])
            ]
        ))(test_df).collect() == [Row('90', '1927', '2017-01-01', 'DUMMYVAL', 'GOODVAL'
                                      , None, None, None, None, None, None),
                                  Row('90', '1927', '2017-01-01', None, 'GOODVAL', 'GOODVAL'
                                      , None, None, None, None, None)]

    # assert original transformer and whitelist was not modified by
    # additional args
    assert lab_result_priv.whitelists == old_whitelists
    assert lab_result_priv.lab_result_transformer.transforms == old_transformer.transforms
