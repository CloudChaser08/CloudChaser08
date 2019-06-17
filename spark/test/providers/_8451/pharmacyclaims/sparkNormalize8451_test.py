import pytest
from pyspark.sql import Row
import datetime
import spark.providers._8451.pharmacyclaims.sparkNormalize8451Rx as _8451


@pytest.mark.usefixtures("spark")
def test_init(spark):
    """
    Run the normalization routine and gather results
    """
    spark['spark'].sparkContext.parallelize([
        Row(
            hvm_vdr_feed_id=_8451.FEED_ID,
            gen_ref_domn_nm='EARLIEST_VALID_SERVICE_DATE',
            gen_ref_itm_nm='',
            gen_ref_1_dt=datetime.date(1901, 1, 1),
            whtlst_flg=''
        )
    ]).toDF().createOrReplaceTempView('ref_gen_ref')

    spark['spark'].sparkContext.parallelize([
        Row(
            geo_state_pstl_cd='191'
        )
    ]).toDF().createOrReplaceTempView('ref_geo_state')

    _8451.run(spark['spark'], spark['runner'], '2019-01-15', test=True)

