import datetime
import pytest

import spark.providers.apothecary_by_design.sparkNormalizeApothecaryByDesign as abd

results = None

def cleanup(spark):
    spark['sqlContext'].dropTempTable('pharmacyclaims_common_model')
    spark['sqlContext'].dropTempTable('abd_transaction')
    spark['sqlContext'].dropTempTable('abd_additional_data')


@pytest.mark.usefixturesf('spark')
def test_init(spark):
    abd.run(spark['spark'], spark['runner'], '2017-10-06', test = True)
    global results
    results = spark['sqlContext'] \
                .sql('select * from pharmacyclaims_common_model') \
                .collect()


def test_ran():
    print results


