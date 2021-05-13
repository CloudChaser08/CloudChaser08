import pytest
from pyspark.sql import Row
import datetime

import spark.providers.practice_fusion.emr.normalize as practice_fusion

@pytest.mark.usefixtures("spark")
def test_init(spark):
    """
    Run the normalization routine and gather results
    """
    spark['spark'].sparkContext.parallelize([
        Row(
            gen_ref_domn_nm='EARLIEST_VALID_SERVICE_DATE',
            hvm_vdr_feed_id='136',
            gen_ref_cd='',
            gen_ref_itm_nm='',
            gen_ref_itm_desc='',
            whtlst_flg='',
            gen_ref_1_txt='',
            gen_ref_2_txt='',
            gen_ref_1_num=0,
            gen_ref_2_num=0,
            gen_ref_1_dt=datetime.date(2010, 1, 1),
            gen_ref_2_dt=datetime.date(2010, 1, 1),
            crt_dt=datetime.date(2019, 4, 17)
        )
    ]).toDF().createOrReplaceTempView('ref_gen_ref')

    practice_fusion.run(date_input='2019-04-17',
                        end_to_end_test=False, test=True, spark=spark['spark'], runner=spark['runner'])
