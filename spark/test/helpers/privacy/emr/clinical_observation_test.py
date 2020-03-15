import pytest
from spark.helpers.privacy.common import Transformer, TransformFunction
import spark.helpers.privacy.emr.clinical_observation as clinical_observation_priv
from pyspark.sql.types import StructField, StructType, StringType, Row


@pytest.mark.usefixtures("spark")
def test_init(spark):
    # whitelists
    spark['spark'].sparkContext.parallelize([
        ['GOODVAL', 'Y', 'emr_clin_obsn.clin_obsn_nm'],
        ['GOODVAL', 'Y', 'emr_clin_obsn.clin_obsn_diag_nm'],
        ['DUMMYVAL', 'Y', 'emr_clin_obsn_test.notransform']
    ]).toDF(StructType([
        StructField('gen_ref_itm_nm', StringType()),
        StructField('whtlst_flg', StringType()),
        StructField('gen_ref_domn_nm', StringType())
    ])).createOrReplaceTempView('ref_gen_ref')


def test_filter(spark):
    # test df including commonly filtered fields
    test_df = spark['spark'].sparkContext.parallelize([
        ['100', '1880', '2017-01-01', 'dummyval', 'GOODVAL', 'badval', 'desc', 'desc'],
        ['100', '1880', '2017-01-01', 'dummyval2', 'badval', 'goodval', 'desc', 'desc']
    ]).toDF(StructType([
        StructField('ptnt_age_num', StringType()),
        StructField('ptnt_birth_yr', StringType()),
        StructField('enc_dt', StringType()),
        StructField('notransform', StringType()),
        StructField('clin_obsn_nm', StringType()),
        StructField('clin_obsn_diag_nm', StringType()),
        StructField('clin_obsn_diag_desc', StringType()),
        StructField('clin_obsn_result_desc', StringType())
    ]))

    # assertion with no additional transforms
    assert clinical_observation_priv.filter(spark['sqlContext'])(test_df).collect() \
        == [Row('90', '1927', '2017-01-01', 'dummyval', 'GOODVAL', 'badval', 'desc', 'desc'),
            Row('90', '1927', '2017-01-01', 'dummyval2', 'badval', 'goodval', 'desc', 'desc')]

    # save original state of built-in transformer
    old_transformer = Transformer(**dict(clinical_observation_priv.clinical_observation_transformer.transforms))
    old_whitelists = list(clinical_observation_priv.whitelists)

    def whitelist_update(whitelist):
        return whitelist + [{
            'column_name': 'notransform',
            'domain_name': 'emr_clin_obsn_test.notransform'
        }, {
            'column_name': 'clin_obsn_nm',
            'domain_name': 'emr_clin_obsn.clin_obsn_nm'
        }, {
            'column_name': 'clin_obsn_diag_nm',
            'domain_name': 'emr_clin_obsn.clin_obsn_diag_nm'
        }]

    # assertion including additional transforms
    assert clinical_observation_priv.filter(
        spark['sqlContext'],
        update_whitelists=whitelist_update,
        additional_transformer=Transformer(
            clin_obsn_nm=[
                TransformFunction(lambda c: c.replace('bad', 'good'), ['clin_obsn_nm'])
            ]
        ))(test_df).collect() == [Row('90', '1927', '2017-01-01', 'DUMMYVAL', 'GOODVAL', None, 'desc', 'desc'),
                                  Row('90', '1927', '2017-01-01', None, 'GOODVAL', 'GOODVAL', 'desc', 'desc')]

    # assert original transformer and whitelist was not modified by
    # additional args
    assert clinical_observation_priv.clinical_observation_transformer.transforms == old_transformer.transforms
    assert clinical_observation_priv.whitelists == old_whitelists
