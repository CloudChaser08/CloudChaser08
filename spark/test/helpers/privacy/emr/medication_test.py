import pytest
from spark.helpers.privacy.common import Transformer, TransformFunction
import spark.helpers.privacy.emr.medication as medication_priv
from pyspark.sql.types import StructField, StructType, StringType, Row


@pytest.mark.usefixtures("spark")
def test_init(spark):
    # whitelists
    spark['spark'].sparkContext.parallelize([
        ['GOODVAL', 'Y', 'emr_medctn.medctn_admin_sig_cd'],
        ['GOODVAL', 'Y', 'emr_medctn.medctn_admin_sig_txt'],
        ['DUMMYVAL', 'Y', 'emr_medctn_test.notransform']
    ]).toDF(StructType([
        StructField('gen_ref_itm_nm', StringType()),
        StructField('whtlst_flg', StringType()),
        StructField('gen_ref_domn_nm', StringType())
    ])).createOrReplaceTempView('ref_gen_ref')


def test_filter(spark):
    # test df including commonly filtered fields
    test_df = spark['spark'].sparkContext.parallelize([
        ['100', '1880', '2017-01-01', 'dummyval', 'GOODVAL', 'badval', None, None, None, None],
        ['100', '1880', '2017-01-01', 'dummyval2', 'badval', 'goodval', None, None, None, None]
    ]).toDF(StructType([
        StructField('ptnt_age_num', StringType()),
        StructField('ptnt_birth_yr', StringType()),
        StructField('enc_dt', StringType()),
        StructField('notransform', StringType()),
        StructField('medctn_admin_sig_cd', StringType()),
        StructField('medctn_admin_sig_txt', StringType()),
        StructField('medctn_admin_form_nm', StringType()),
        StructField('medctn_strth_txt', StringType()),
        StructField('medctn_strth_txt_qual', StringType()),
        StructField('medctn_admin_rte_txt', StringType())
    ]))

    # assertion with no additional transforms
    assert medication_priv.filter(spark['sqlContext'])(test_df).collect() \
        == [Row('90', '1927', '2017-01-01', 'dummyval', 'GOODVAL', 'badval', None, None, None, None),
            Row('90', '1927', '2017-01-01', 'dummyval2', 'badval', 'goodval', None, None, None, None)]

    # save original state of built-in transformer
    old_transformer = Transformer(**dict(medication_priv.medication_transformer.transforms))
    old_whitelists = list(medication_priv.whitelists)

    def whitelist_update(whitelist):
        return whitelist + [{
            'column_name': 'notransform',
            'domain_name': 'emr_medctn_test.notransform'
        }, {
            'column_name': 'medctn_admin_sig_cd',
            'domain_name': 'emr_medctn.medctn_admin_sig_cd'
        }, {
            'column_name': 'medctn_admin_sig_txt',
            'domain_name': 'emr_medctn.medctn_admin_sig_txt'
        }]

    # assertion including additional transforms
    assert medication_priv.filter(
        spark['sqlContext'],
        update_whitelists=whitelist_update,
        additional_transformer=Transformer(
            medctn_admin_sig_cd=[
                TransformFunction(lambda c: c.replace('bad', 'good'), ['medctn_admin_sig_cd'])
            ]
        ))(test_df).collect() == [Row('90', '1927', '2017-01-01', 'DUMMYVAL', 'GOODVAL', None, None, None, None, None),
                                  Row('90', '1927', '2017-01-01', None, 'GOODVAL', 'GOODVAL', None, None, None, None)]

    # assert original transformer and whitelist was not modified by
    # additional args
    assert medication_priv.medication_transformer.transforms == old_transformer.transforms
    assert medication_priv.whitelists == old_whitelists
