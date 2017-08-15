import pytest

import datetime
import shutil
import os
import logging

import spark.providers.emdeon.medicalclaims.sparkNormalizeEmdeonDX as emdeon
import spark.helpers.file_utils as file_utils

results = []


def cleanup(spark):
    spark['sqlContext'].dropTempTable('medicalclaims_common_model')

    try:
        shutil.rmtree(file_utils.get_abs_path(__file__, './resources/output/'))
    except:
        logging.warn('No output directory.')


@pytest.mark.usefixtures("spark")
def test_init(spark):
    cleanup(spark)
    emdeon.run(spark['spark'], spark['runner'], '2017-08-31', True)
    global results
    results = spark['sqlContext'].sql('select * from medicalclaims_common_model') \
                                 .collect()


def test_cleanup(spark):
    cleanup(spark)
