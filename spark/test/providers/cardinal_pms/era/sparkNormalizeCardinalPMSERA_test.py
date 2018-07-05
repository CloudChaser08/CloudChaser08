import pytest

import datetime
from pyspark.sql import Row

import spark.providers.cardinal_pms.era.sparkNormalizeCardinalPMSERA as cardinal_pms

detail_line_results = []
summary_results = []

def cleanup(spark):
    pass

@pytest.mark.usefixtures("spark")
def test_init(spark):
    spark['spark'].sparkContext.parallelize([
        Row(
            hvm_vdr_feed_id='66',
            gen_ref_domn_nm='EARLIEST_VALID_SERVICE_DATE',
            gen_ref_itm_nm='',
            gen_ref_1_dt=datetime.date(2013, 1, 1),
            whtlst_flg=''
        )
    ]).toDF().createOrReplaceTempView('ref_gen_ref')

    cardinal_pms.run(spark['spark'], spark['runner'], '2018-02-13', test=True)
    global detail_line_results, summary_results
    detail_line_results = spark['sqlContext'].sql('select * from era_detail_line_common_model') \
                                 .collect()
    summary_results = spark['sqlContext'].sql('select * from era_detail_line_common_model') \
                                 .collect()
