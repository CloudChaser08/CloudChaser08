import pytest

import datetime
import json

import spark.providers.liquidhub.custom.sparkNormalizeLiquidhub as liquidhub
from spark.helpers.udf.general_helpers import obfuscate_hvid

results = {}

GROUPS = [
    'LHV1_Source1_PatDemo_20180717_v1',         # t1
    'LHV1_Manufacturer1_Source1_20180717_v1',   # t2
    'LHV2_Source1_PatDemo_20180717_v1',         # t3
    'LHV2_Manufacturer1_Source1_20180717_v1'    # t4
]

def cleanup(spark):
    pass

@pytest.mark.usefixtures("spark")
def test_init(spark):
    global results
    # new model run
    for g in GROUPS:
        liquidhub.run(spark['spark'], spark['runner'], g, 1, True)
        results[g] = spark['sqlContext'].table('liquidhub_deliverable').collect()

def test_hvid_obfuscation():
    # Manufacturer is Amgen, salt is just 'LHv2'
    assert filter(lambda r: r.source_patient_id == 'claim-1', results[GROUPS[2]])[0] \
        .hvid == str(obfuscate_hvid('206845800', 'LHv2')).lower()

    # Manufacturer is not Amgen, salt is a combination of 'LHv2' and manufacturer name

    # Manufacturer name in file content
    assert filter(lambda r: r.source_patient_id == 'claim-2', results[GROUPS[2]])[0] \
        .hvid == str(obfuscate_hvid('161340392', 'LHv2' + 'NOVARTIS'.lower())).lower()

    # Manufacturer name in file file
    assert filter(lambda r: r.source_patient_id == 'claim-2', results[GROUPS[3]])[0] \
        .hvid == str(obfuscate_hvid('161340392', 'LHv2' + 'Manufacturer1'.lower())).lower()

def test_multimatch_candidates_present():
    claim3 = filter(lambda r: r.source_patient_id == 'claim-3', results[GROUPS[2]])[0]
    candidates = json.loads(claim3.matching_meta)
    assert sorted(candidates, key=lambda x: x[0]) == \
            [
                [obfuscate_hvid("95878097", 'LHv2').lower(), 0.0001113792],
                [obfuscate_hvid("463818609", 'LHv2').lower(), 0.0006224556],
                [obfuscate_hvid("228607344", 'LHv2').lower(), 0.0],
                [obfuscate_hvid("36024084", 'LHv2').lower(), 0.0],
                [obfuscate_hvid("138450964", 'LHv2').lower(), 0.0]
            ]

def test_extact_match_candidates_empty():
    claim8 = filter(lambda r: r.source_patient_id == 'claim-8', results[GROUPS[2]])[0]
    assert 'matching_meta' in claim8
    assert claim8.matching_meta == None

def test_candidate_hvid_obfuscation():
    claim4 = filter(lambda r: r.source_patient_id == 'claim-4', results[GROUPS[2]])[0]
    claim4_candidate_hvids = map(lambda x: x[0], json.loads(claim4.matching_meta))
    assert str(obfuscate_hvid('36024084', 'LHv2')).lower() in claim4_candidate_hvids

def test_header_row():
    assert results[GROUPS[2]][0].source_patient_id == 'Source Patient Id'
    assert results[GROUPS[2]][0].hvid == 'HVID'
    assert results[GROUPS[2]][0].matching_meta == 'Matching Meta'

def test_cleanup(spark):
    cleanup(spark)
