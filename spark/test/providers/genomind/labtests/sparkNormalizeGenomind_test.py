import datetime
import pytest

from pyspark.sql import Row
import spark.providers.genomind.labtests.sparkNormalizeGenomind as gm

results = None

@pytest.mark.usefixtures('spark')
def test_init(spark):
    spark['spark'].sparkContext.parallelize([
        Row(
            hvm_vdr_Feed_id = gm.FEED_ID,
            gen_ref_domn_nm = 'EARLIEST_VALID_SERVICE_DATE',
            gen_ref_itm_nm = '',
            gen_ref_1_dt = datetime.date(1900, 1, 1),
            whtlst_flg = ''
        )
    ]).toDF().createOrReplaceTempView('ref_gen_ref')

    gm.run(spark['spark'], spark['runner'], '2018-06-20', test=True)
    global results
    results = spark['sqlContext'].sql('select * from labtests_common_model').collect()

def test_something():
    print len(results)
