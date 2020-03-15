import pytest
import spark.helpers.privacy.emr.provider_order as provider_order_priv
from pyspark.sql.types import StructField, StructType, StringType, Row


@pytest.mark.usefixtures("spark")
def test_init(spark):
    # whitelists
    spark['spark'].sparkContext.parallelize([
        ['', 'GOODVAL', '', 'Y', 'emr_prov_ord.prov_ord_alt_cd'],
        ['GOODVAL', '', '', 'Y', 'emr_prov_ord.prov_ord_alt_nm'],
        ['DUMMYVAL', '', '', 'Y', 'emr_prov_ord_test.notransform']
    ]).toDF(StructType([
        StructField('gen_ref_itm_nm', StringType()),
        StructField('gen_ref_cd', StringType()),
        StructField('gen_ref_itm_desc', StringType()),
        StructField('whtlst_flg', StringType()),
        StructField('gen_ref_domn_nm', StringType())
    ])).createOrReplaceTempView('ref_gen_ref')


def test_filter(spark):
    # test df including commonly filtered fields
    test_df = spark['spark'].sparkContext.parallelize([
        ['100', '1880', '2017-01-01', 'dummyval', 'GOODVAL', 'goodval_qual',
         'badval', None, None, None, None, None, None, None, None, None, None, None, None, None, None, None],
        ['100', '1880', '2017-01-01', 'dummyval2', 'badval', 'badval_qual',
         'goodval', None, None, None, None, None, None, None, None, None, None, None, None, None, None, None]
    ]).toDF(StructType([
        StructField('ptnt_age_num', StringType()),
        StructField('ptnt_birth_yr', StringType()),
        StructField('enc_dt', StringType()),
        StructField('notransform', StringType()),
        StructField('prov_ord_alt_cd', StringType()),
        StructField('prov_ord_alt_cd_qual', StringType()),
        StructField('prov_ord_alt_nm', StringType()),
        StructField('prov_ord_alt_desc', StringType()),
        StructField('prov_ord_diag_nm', StringType()),
        StructField('prov_ord_rsn_cd', StringType()),
        StructField('prov_ord_rsn_cd_qual', StringType()),
        StructField('prov_ord_rsn_nm', StringType()),
        StructField('prov_ord_stat_cd', StringType()),
        StructField('prov_ord_stat_cd_qual', StringType()),
        StructField('prov_ord_complt_rsn_cd', StringType()),
        StructField('prov_ord_cxld_rsn_cd', StringType()),
        StructField('prov_ord_result_desc', StringType()),
        StructField('prov_ord_trtmt_typ_cd', StringType()),
        StructField('prov_ord_trtmt_typ_cd_qual', StringType()),
        StructField('prov_ord_rfrd_speclty_cd', StringType()),
        StructField('prov_ord_rfrd_speclty_cd_qual', StringType()),
        StructField('prov_ord_specl_instrs_desc', StringType())
    ]))

    # assertion with no additional transforms
    assert provider_order_priv.filter(spark['sqlContext'])(test_df).collect() \
        == [Row('90', '1927', '2017-01-01', 'dummyval', 'GOODVAL', 'goodval_qual', 'badval', None,
                None, None, None, None, None, None, None, None, None, None, None, None, None, None),
            Row('90', '1927', '2017-01-01', 'dummyval2', 'badval', 'badval_qual', 'goodval', None,
                None, None, None, None, None, None, None, None, None, None, None, None, None, None)]

    # save original state of built-in transformer
    old_whitelists = list(provider_order_priv.whitelists)

    def whitelist_update(whitelist):
        return whitelist + [{
            'column_name': 'notransform',
            'domain_name': 'emr_prov_ord_test.notransform'
        }, {
            'column_name': 'prov_ord_alt_cd',
            'domain_name': 'emr_prov_ord.prov_ord_alt_cd',
            'whitelist_col_name': 'gen_ref_cd',
            'comp_col_names': ['prov_ord_alt_cd_qual']
        }, {
            'column_name': 'prov_ord_alt_nm',
            'domain_name': 'emr_prov_ord.prov_ord_alt_nm'
        }]

    # assertion including additional transforms
    assert provider_order_priv.filter(
        spark['sqlContext'],
        update_whitelists=whitelist_update
    )(test_df).collect() == [Row('90', '1927', '2017-01-01', 'DUMMYVAL', 'GOODVAL', 'goodval_qual', None, None,
                                  None, None, None, None, None, None, None, None, None, None, None, None, None, None),
                             Row('90', '1927', '2017-01-01', None, None, None, 'GOODVAL', None,
                                  None, None, None, None, None, None, None, None, None, None, None, None, None, None)]

    # assert original transformer and whitelist was not modified by
    # additional args
    assert provider_order_priv.whitelists == old_whitelists
