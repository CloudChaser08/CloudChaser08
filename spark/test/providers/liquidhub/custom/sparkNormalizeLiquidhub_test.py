import pytest

import datetime
import json

import spark.providers.liquidhub.custom.sparkNormalizeLiquidhub as liquidhub
from spark.helpers.udf.general_helpers import obfuscate_hvid

results = {}
return_file_name = {}
errors = {}

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
    global results, return_file_name
    # new model run
    for g in GROUPS:
        return_file_name[g] = liquidhub.run(spark['spark'], spark['runner'], g, 1, True)
        results[g] = spark['sqlContext'].table('liquidhub_deliverable').collect()
        errors[g]  = spark['sqlContext'].table('liquidhub_error').collect()

def test_hvid_obfuscation():
    # Manufacturer is Amgen, salt is just 'LHv2'
    amgen_row = [r for r in results[GROUPS[2]] if r['Source Patient Id'] == 'claim-1'][0]
    assert amgen_row['HVID'] == str(obfuscate_hvid('206845800', 'LHv2'))

    # Manufacturer is not Amgen, salt is a combination of 'LHv2' and manufacturer name

    # Manufacturer name in file content
    novartis_row_lhv2 = [r for r in results[GROUPS[2]] if r['Source Patient Id'] == 'claim-2'][0]
    assert novartis_row_lhv2['HVID'] == str(obfuscate_hvid('161340392', 'LHv2' + 'NOVARTIS'.lower()))
    novartis_row_lhv1 = [r for r in results[GROUPS[0]] if r['Source Patient Id'] == 'claim-2'][0]
    assert novartis_row_lhv1['HVID'] == str(obfuscate_hvid('161340392', 'LHv2' + 'NOVARTIS'.lower()))

    # Manufacturer name in file file
    manu_row_lhv2 = [r for r in results[GROUPS[3]] if r['Source Patient Id'] == 'claim-2'][0]
    assert manu_row_lhv2['HVID'] == str(obfuscate_hvid('161340392', 'LHv2' + 'Manufacturer1'.lower()))
    manu_row_lhv1 = [r for r in results[GROUPS[1]] if r['Source Patient Id'] == 'claim-2'][0]
    assert manu_row_lhv1['HVID'] == str(obfuscate_hvid('161340392', 'LHv2' + 'Manufacturer1'.lower()))

def test_multimatch_candidates_present():
    claim3 = [r for r in results[GROUPS[2]] if r['Source Patient Id'] == 'claim-3'][0]
    candidates = json.loads(claim3['Matching Meta'])
    assert sorted(candidates, key=lambda x: x[0]) == \
            [
                [obfuscate_hvid("95878097", 'LHv2').lower(), 0.0001113792],
                [obfuscate_hvid("463818609", 'LHv2').lower(), 0.0006224556],
                [obfuscate_hvid("228607344", 'LHv2').lower(), 0.0],
                [obfuscate_hvid("36024084", 'LHv2').lower(), 0.0],
                [obfuscate_hvid("138450964", 'LHv2').lower(), 0.0]
            ]

def test_extact_match_candidates_empty():
    claim8 = [r for r in results[GROUPS[2]] if r['Source Patient Id'] == 'claim-8'][0]
    assert 'Matching Meta' in claim8
    assert claim8['Matching Meta'] == None

def test_candidate_hvid_obfuscation():
    claim4 = [r for r in results[GROUPS[2]] if r['Source Patient Id'] == 'claim-4'][0]
    claim4_candidate_hvids = [x[0] for x in json.loads(claim4['Matching Meta'])]
    assert str(obfuscate_hvid('36024084', 'LHv2')).lower() in claim4_candidate_hvids

def test_manufacturer_column():
    # Manufacturer name in file content
    assert [r for r in results[GROUPS[2]] if r['Source Patient Id'] == 'claim-2'][0] \
        ['Manufacturer'] == "NOVARTIS"
    assert [r for r in results[GROUPS[0]] if r['Source Patient Id'] == 'claim-2'][0] \
        ['Manufacturer'] == "NOVARTIS"

    # Manufacturer name in file file
    assert [r for r in results[GROUPS[3]] if r['Source Patient Id'] == 'claim-2'][0] \
        ['Manufacturer'] == "Manufacturer1"
    assert [r for r in results[GROUPS[1]] if r['Source Patient Id'] == 'claim-2'][0] \
        ['Manufacturer'] == "Manufacturer1"

def test_file_name():
    for g in GROUPS:
        return_file_name[g] == g + '20180715v1.txt'

def test_bad_manufacturers():
    assert(len(errors[GROUPS[2]]) == 2)
    assert [r for r in errors[GROUPS[2]] if r.manufacturer == 'UNKNOWN'][0] \
        .bad_patient_ids == ['claim-14']
    assert [r for r in errors[GROUPS[2]] if r.manufacturer == 'UNKNOWN'][0] \
        .bad_patient_count == 1
    assert [r for r in errors[GROUPS[2]] if r.manufacturer == 'AIMOVIG'][0] \
        .bad_patient_ids == None
    assert [r for r in errors[GROUPS[2]] if r.manufacturer == 'AIMOVIG'][0] \
        .bad_patient_count == 7

def test_cleanup(spark):
    cleanup(spark)
