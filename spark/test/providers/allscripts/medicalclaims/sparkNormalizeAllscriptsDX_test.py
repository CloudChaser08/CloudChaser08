import pytest
import shutil
import datetime

import spark.providers.allscripts.medicalclaims.sparkNormalizeAllscriptsDX as allscripts_dx
import spark.helpers.file_utils as file_utils

from pyspark.sql import Row

results = None

script_path = __file__


def cleanup(spark):
    spark['sqlContext'].dropTempTable('ref_gen_ref')

    try:
        shutil.rmtree(file_utils.get_abs_path(script_path, './output/'))
    except:
        pass


@pytest.mark.usefixtures("spark")
def test_init(spark):
    cleanup(spark)

    spark['spark'].sparkContext.parallelize([
        Row(
            hvm_vdr_feed_id='26',
            gen_ref_domn_nm='EARLIEST_VALID_SERVICE_DATE',
            gen_ref_itm_nm='',
            gen_ref_cd='',
            gen_ref_1_dt=datetime.date(2016, 1, 1),
            gen_ref_1_txt='',
            gen_ref_2_txt='',
            gen_ref_itm_desc='',
            whtlst_flg=''
        )
    ]).toDF().createOrReplaceTempView('ref_gen_ref')

    allscripts_dx.run(spark['spark'], spark['runner'], '2016-12-01', test=True)

    global results

    results = spark['sqlContext'].read.parquet(
        file_utils.get_abs_path(script_path, './output/*/*')
    ).collect()

def test_cleanup(spark):
    cleanup(spark)
