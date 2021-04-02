import datetime
import json
from mock import patch

import pytest

from spark.census.lhv2.hvXXXXXX.driver import LiquidhubCensusDriver
from spark.helpers.udf.general_helpers import obfuscate_hvid

GROUPS = [
    'LHV1_Source1_PatDemo_20180717_v1',         # t1
    'LHV1_Manufacturer1_Source1_20180717_v1',   # t2
    'LHV2_Source1_PatDemo_20180717_v1',         # t3
    'LHV2_Manufacturer1_Source1_20180717_v1'    # t4
]

@pytest.fixture(scope="module")
@pytest.mark.usefixtures("patch_spark_init")
def driver(patch_spark_init):
    driver = LiquidhubCensusDriver('hvXXXXXX_all')

    # Use test path locations
    driver.matching_path_template = "./test/census/{client}/hvXXXXXX/resources/matching/{{batch_id_path}}/".format(
        client=driver._client_name
    )

    driver.records_path_template = "./test/census/{client}/hvXXXXXX/resources/incoming/{{batch_id_path}}/".format(
        client=driver._client_name
    )
    return driver

@pytest.fixture(scope="module")
@pytest.mark.usefixtures("driver")
@patch('spark.census.lhv2.hvXXXXXX.driver._get_date', return_value=datetime.date(2018, 7, 15))
def transformation_results(_, driver):
    results = {}
    for grp in GROUPS:
        driver.load(datetime.date(2018, 7, 17), grp)
        results[grp] = {
            'deliverable': driver.transform(datetime.date(2018, 7, 17), grp).collect(),
            'errors': driver._spark.table('liquidhub_error').collect(),
            'return_file_name': driver._output_file_name_template
        }
    return results

@pytest.mark.usefixtures("transformation_results")
def test_hvid_obfuscation(transformation_results):
    # Manufacturer is Amgen, salt is just 'LHv2'
    amgen_row = [r for r in transformation_results[GROUPS[2]]['deliverable'] if r['Source Patient Id'] == 'claim-1'][0]
    assert amgen_row['HVID'] == str(obfuscate_hvid('206845800', 'LHv2'))

    # Manufacturer is not Amgen, salt is a combination of 'LHv2' and manufacturer name

    # Manufacturer name in file content
    novartis_row_lhv2 = [r for r in transformation_results[GROUPS[2]]['deliverable'] if r['Source Patient Id'] == 'claim-2'][0]
    assert novartis_row_lhv2['HVID'] == str(obfuscate_hvid('161340392', 'LHv2' + 'NOVARTIS'.lower()))
    novartis_row_lhv1 = [r for r in transformation_results[GROUPS[0]]['deliverable'] if r['Source Patient Id'] == 'claim-2'][0]
    assert novartis_row_lhv1['HVID'] == str(obfuscate_hvid('161340392', 'LHv2' + 'NOVARTIS'.lower()))

    # Manufacturer name in file file
    manu_row_lhv2 = [r for r in transformation_results[GROUPS[3]]['deliverable'] if r['Source Patient Id'] == 'claim-2'][0]
    assert manu_row_lhv2['HVID'] == str(obfuscate_hvid('161340392', 'LHv2' + 'Manufacturer1'.lower()))
    manu_row_lhv1 = [r for r in transformation_results[GROUPS[1]]['deliverable'] if r['Source Patient Id'] == 'claim-2'][0]
    assert manu_row_lhv1['HVID'] == str(obfuscate_hvid('161340392', 'LHv2' + 'Manufacturer1'.lower()))

@pytest.mark.usefixtures("transformation_results")
def test_multimatch_candidates_present(transformation_results):
    claim3 = [r for r in transformation_results[GROUPS[2]]['deliverable'] if r['Source Patient Id'] == 'claim-3'][0]
    candidates = json.loads(claim3['Matching Meta'])
    assert sorted(candidates, key=lambda x: x[0]) == \
            [
                [obfuscate_hvid("95878097", 'LHv2').lower(), 0.0001113792],
                [obfuscate_hvid("463818609", 'LHv2').lower(), 0.0006224556],
                [obfuscate_hvid("228607344", 'LHv2').lower(), 0.0],
                [obfuscate_hvid("36024084", 'LHv2').lower(), 0.0],
                [obfuscate_hvid("138450964", 'LHv2').lower(), 0.0]
            ]

@pytest.mark.usefixtures("transformation_results")
def test_extact_match_candidates_empty(transformation_results):
    claim8 = [r for r in transformation_results[GROUPS[2]]['deliverable'] if r['Source Patient Id'] == 'claim-8'][0]
    assert 'Matching Meta' in claim8
    assert claim8['Matching Meta'] == None

@pytest.mark.usefixtures("transformation_results")
def test_candidate_hvid_obfuscation(transformation_results):
    claim4 = [r for r in transformation_results[GROUPS[2]]['deliverable'] if r['Source Patient Id'] == 'claim-4'][0]
    claim4_candidate_hvids = [x[0] for x in json.loads(claim4['Matching Meta'])]
    assert str(obfuscate_hvid('36024084', 'LHv2')).lower() in claim4_candidate_hvids

@pytest.mark.usefixtures("transformation_results")
def test_manufacturer_column(transformation_results):
    # Manufacturer name in file content
    assert [r for r in transformation_results[GROUPS[2]]['deliverable'] if r['Source Patient Id'] == 'claim-2'][0] \
        ['Manufacturer'] == "NOVARTIS"
    assert [r for r in transformation_results[GROUPS[0]]['deliverable'] if r['Source Patient Id'] == 'claim-2'][0] \
        ['Manufacturer'] == "NOVARTIS"

    # Manufacturer name in file file
    assert [r for r in transformation_results[GROUPS[3]]['deliverable'] if r['Source Patient Id'] == 'claim-2'][0] \
        ['Manufacturer'] == "Manufacturer1"
    assert [r for r in transformation_results[GROUPS[1]]['deliverable'] if r['Source Patient Id'] == 'claim-2'][0] \
        ['Manufacturer'] == "Manufacturer1"

@pytest.mark.usefixtures("transformation_results")
def test_file_name(transformation_results):
    for g in GROUPS:
        transformation_results[g]['return_file_name'] == g + '20180715v1.txt'

@pytest.mark.usefixtures("transformation_results")
def test_bad_manufacturers(transformation_results):
    assert(len(transformation_results[GROUPS[2]]['errors']) == 2)
    assert [r for r in transformation_results[GROUPS[2]]['errors'] if r.manufacturer == 'UNKNOWN'][0] \
        .bad_patient_ids == ['claim-14']
    assert [r for r in transformation_results[GROUPS[2]]['errors'] if r.manufacturer == 'UNKNOWN'][0] \
        .bad_patient_count == 1
    assert [r for r in transformation_results[GROUPS[2]]['errors'] if r.manufacturer == 'AIMOVIG'][0] \
        .bad_patient_ids == None
    assert [r for r in transformation_results[GROUPS[2]]['errors'] if r.manufacturer == 'AIMOVIG'][0] \
        .bad_patient_count == 7
