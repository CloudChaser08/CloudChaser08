import pytest
from pyspark.sql import Row
import datetime
import spark.providers.waystar.medicalclaims.normalize as waystar


@pytest.mark.usefixtures("spark")
def test_init(spark):
    """
    Run the normalization routine and gather results
    """
    spark['spark'].sparkContext.parallelize([
        Row(
            hvm_vdr_feed_id=waystar.FEED_ID,
            gen_ref_domn_nm='EARLIEST_VALID_SERVICE_DATE',
            gen_ref_itm_nm='',
            gen_ref_1_dt=datetime.date(1901, 1, 1),
            whtlst_flg=''
        )
    ]).toDF().createOrReplaceTempView('ref_gen_ref')

    spark['spark'].sparkContext.parallelize([
        Row(
            d=0
        )
    ]).toDF().createOrReplaceTempView('date_explode_indices')

    waystar.run(spark['spark'], spark['runner'], '2019-01-15', test=True)
