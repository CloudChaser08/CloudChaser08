import pytest

import shutil
import logging
from pyspark.sql import Row

import spark.providers.cardinal_tsi.emr.sparkNormalizeCardinalTSI as cardinal
import spark.helpers.file_utils as file_utils

medication_results = []
diagnosis_results = []


def cleanup(spark):
    spark['sqlContext'].dropTempTable('medication_common_model')
    spark['sqlContext'].dropTempTable('transactions_medication')
    spark['sqlContext'].dropTempTable('diagnosis_common_model')
    spark['sqlContext'].dropTempTable('transactions_diagnosis')

    try:
        shutil.rmtree(file_utils.get_abs_path(__file__, './resources/output/'))
    except:
        logging.warn('No output directory.')


@pytest.mark.usefixtures("spark")
def test_init(spark):
    cleanup(spark)

    spark['spark'].sparkContext.parallelize([
        Row(
            hvm_vdr_feed_id=None,
            gen_ref_domn_nm=None,
            gen_ref_itm_nm=None,
            gen_ref_1_dt=None,
            whtlst_flg=None
        )
    ]).toDF().createTempView('ref_gen_ref')

    cardinal.run(spark['spark'], spark['runner'], '2017-08-31', True)
    global medication_results, diagnosis_results
    medication_results = spark['sqlContext'].sql('select * from medication_common_model') \
                                            .collect()
    diagnosis_results = spark['sqlContext'].sql('select * from diagnosis_common_model') \
                                           .collect()


def test_result_counts():
    "Ensure that results have the correct number of rows - the matching payload is missing 2 rows"
    assert len(diagnosis_results) == 10
    assert len(medication_results) == 10


def test_cleanup(spark):
    cleanup(spark)
