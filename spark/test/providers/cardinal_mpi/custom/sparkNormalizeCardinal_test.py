import pytest

import datetime
import json

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
    assert filter(lambda r: r.claimid == 'claim-1', results)[0] \
        .hvid == str(slightly_obfuscate_hvid(206845800, 'Cardinal_MPI-0'))

def test_multimatch_candidates_present():
    claim3 = filter(lambda r: r.claimid == 'claim-3', results)[0]
    candidates = json.loads(claim3.candidates)
    assert  sorted(candidates, key=lambda x: x['hvid']) == \
             [{"hvid": "1138184017", "confidence": "0.0"}, {"hvid": "1345493044", "confidence": "0.0"}, {"hvid": "1439843664", "confidence": "0.0"}, {"hvid": "1515695924", "confidence": "0.0"}, {"hvid": "1573190641", "confidence": "0.0"}]

def test_extact_match_candidates_empty():
    claim8 = filter(lambda r: r.claimid == 'claim-8', results)[0]
    assert 'candidates' in claim8
    assert claim8.candidates == ''

def test_candidate_hvid_obfuscation():
    claim4 = filter(lambda r: r.claimid == 'claim-4', results)[0]
    claim4_candidate_hvids = map(lambda x: x['hvid'], json.loads(claim4.candidates))
    assert str(slightly_obfuscate_hvid(36024084, 'Cardinal_MPI-0')) in claim4_candidate_hvids

def test_cleanup(spark):
    cleanup(spark)
