import pytest

import datetime
import json

import spark.providers.liquidhub.custom.sparkNormalizeLiquidhub as liquidhub
from spark.helpers.udf.general_helpers import obfuscate_hvid

results = []

def cleanup(spark):
    pass

@pytest.mark.usefixtures("spark")
def test_init(spark):
    # new model run
    liquidhub.run(spark['spark'], spark['runner'], 'TERRADATA', '2018-06-01', True)
    global results
    results = spark['sqlContext'].sql('select * from liquidhub_deliverable') \
                                        .collect()

def test_hvid_obfuscation():
    assert filter(lambda r: r.lhid == 'claim-1', results)[0] \
        .hvid == str(obfuscate_hvid('206845800', 'LHv2')).lower()

def test_multimatch_candidates_present():
    claim3 = filter(lambda r: r.lhid == 'claim-3', results)[0]
    candidates = json.loads(claim3.matching_meta_data)
    assert sorted(candidates, key=lambda x: x[0]) == \
            [
                ["0280a5d0d8d2eb49e0b3e8edeb8a2a48", 0.0001113792],
                ["5da109bef0ef3764ae70dfd5930ad508", 0.0006224556],
                ["6bd6a1da587e4a88449fe4e1e002dc46", 0.0],
                ["b8a6a6ef2941a0a18dca74de4331e6ff", 0.0],
                ["e42f92d8690b8eb232c6439f44e97801", 0.0]
            ]

def test_extact_match_candidates_empty():
    claim8 = filter(lambda r: r.lhid == 'claim-8', results)[0]
    assert 'matching_meta_data' in claim8
    assert claim8.matching_meta_data == None

def test_candidate_hvid_obfuscation():
    claim4 = filter(lambda r: r.lhid == 'claim-4', results)[0]
    claim4_candidate_hvids = map(lambda x: x[0], json.loads(claim4.matching_meta_data))
    assert str(obfuscate_hvid('36024084', 'LHv2')).lower() in claim4_candidate_hvids

def test_header_row():
    assert results[0].lhid == 'LHID'
    assert results[0].hvid == 'HVID'
    assert results[0].matching_meta_data == 'Matching Meta Data'

def test_cleanup(spark):
    cleanup(spark)
