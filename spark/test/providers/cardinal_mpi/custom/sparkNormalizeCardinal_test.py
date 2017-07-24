import pytest

import datetime

import spark.providers.cardinal_mpi.custom.sparkNormalizeCardinal as cardinal
from spark.helpers.udf.general_helpers import slightly_obfuscate_hvid

results = []

def cleanup(spark):
    pass


@pytest.mark.usefixtures("spark")
def test_init(spark):
    # new model run
    cardinal.run(spark['spark'], spark['runner'], '2017-04-01', True)
    global results
    results = spark['sqlContext'].sql('select * from cardinal_mpi_model') \
                                        .collect()

def test_hvid_obfuscation():
    assert filter(lambda r: r.claimId == 'claim-1', results)[0] \
        .hvid == str(slightly_obfuscate_hvid(206845800, 'Cardinal_MPI-0'))

def test_multimatch_quality_present():
    assert filter(lambda r: r.claimId == 'claim-3', results)[0] \
        .multiMatchQuality == "med"

def test_cleanup(spark):
    cleanup(spark)
