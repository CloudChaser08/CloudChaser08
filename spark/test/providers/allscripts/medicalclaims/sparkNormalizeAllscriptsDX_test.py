import pytest
import shutil
import datetime

import spark.providers.allscripts.medicalclaims.normalize as allscripts_dx
import spark.helpers.file_utils as file_utils

from pyspark.sql import Row

results = None

script_path = __file__


def cleanup(spark):
    spark['sqlContext'].dropTempTable('ref_gen_ref')

    try:
        shutil.rmtree('./test/marketplace/resources/output/')
    except:
        pass


@pytest.mark.usefixtures("spark")
def test_init(spark):
    cleanup(spark)

    spark['spark'].sparkContext.parallelize([
        Row(
            hvm_vdr_feed_id='26',
            gen_ref_domn_nm='EARLIEST_VALID_SERVICE_DATE',
            gen_ref_cd='',
            whtlst_flg='',
            gen_ref_1_dt=datetime.date(2012, 1, 1),
            gen_ref_2_dt=''
        ),
        Row(
            hvm_vdr_feed_id='26',
            gen_ref_domn_nm='HVM_AVAILABLE_HISTORY_START_DATE',
            gen_ref_cd='',
            whtlst_flg='',
            gen_ref_1_dt=datetime.date(2015, 1, 1),
            gen_ref_2_dt=''
        )
    ]).toDF().createOrReplaceTempView('ref_gen_ref')

    allscripts_dx.run(date_input='2016-12-01', test=True, spark=spark['spark'], runner=spark['runner'])

    global results

    results = spark['sqlContext'].read.parquet('./test/marketplace/resources/output/medicalclaims/*').collect()


def test_cleanup(spark):
    cleanup(spark)
