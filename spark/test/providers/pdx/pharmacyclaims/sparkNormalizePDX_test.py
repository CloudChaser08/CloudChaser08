import pytest
from pyspark.sql import Row
import datetime
import spark.providers.pdx.pharmacyclaims.sparkNormalizePDX as pdx


@pytest.mark.usefixtures("spark")
def test_init(spark):
    """
    Run the normalization routine and gather results
    """
    spark['spark'].sparkContext.parallelize([
        Row(
            hvm_vdr_feed_id=pdx.FEED_ID,
            gen_ref_domn_nm='EARLIEST_VALID_SERVICE_DATE',
            gen_ref_itm_nm='',
            gen_ref_1_dt=datetime.date(1901, 1, 1),
            whtlst_flg=''
        )
    ]).toDF().createOrReplaceTempView('ref_gen_ref')

    pdx.run(spark['spark'], spark['runner'], '2019-01-15', test=True)

