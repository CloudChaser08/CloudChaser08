import pytest
from pyspark.sql import Row
import datetime

import spark.providers.auoradx.labtests.sparkNormalizeAuroradx as auroradx

@pytest.mark.usefixtures("spark")
def test_init(spark):
    """
    Run the normalization routine and gather results
    """
    spark['spark'].sparkContext.parallelize([
	Row(
	    hvm_vdr_feed_id=auroradx.FEED_ID,
	    gen_ref_domn_nm='EARLIEST_VALID_SERVICE_DATE',
	    gen_ref_itm_nm='',
	    gen_ref_1_dt=datetime.date(2010, 1, 1),
	    whtlst_flg=''
	)
    ]).toDF().createOrReplaceTempView('ref_gen_ref')

    auroradx.run(spark['spark'], spark['runner'], '2018-02-08', True)

    spark['spark'].table('auroradx_labtests').repartition(1).write.csv('/Users/amcmullen/output/', mode='overwrite')
    spark['spark'].table('auroradx_transactions').repartition(1).write.csv('/Users/amcmullen/transactions/', mode='overwrite')
    spark['spark'].table('auroradx_transactions_dedup').repartition(1).write.csv('/Users/amcmullen/dedup/', mode='overwrite')
