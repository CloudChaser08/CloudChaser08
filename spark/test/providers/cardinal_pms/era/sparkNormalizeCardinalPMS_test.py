import pytest

import logging

import spark.providers.cardinal_pms.era.sparkNormalizeCardinalPMS as cardinal_pms

detail_line_results = []
summary_results = []

def cleanup(spark):
    pass

@pytest.mark.usefixtures("spark")
def test_init(spark):
    cardinal_pms.run(spark['spark'], spark['runner'], '2018-02-13', test=True)
    global detail_line_results, summary_results
    detail_line_results = spark['sqlContext'].sql('select * from era_detail_line_common_model') \
                                 .collect()
    summary_results = spark['sqlContext'].sql('select * from era_detail_line_common_model') \
                                 .collect()

