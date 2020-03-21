import pytest

import datetime
import json

import spark.providers.haystack.custom.sparkNormalizeHaystack as haystack
from spark.helpers.udf.general_helpers import obfuscate_hvid

results = {}
return_file_name = {}

GROUPS = ['123456']

def cleanup(spark):
    pass

@pytest.mark.usefixtures("spark")
def test_init(spark):
    global results, return_file_name
    # new model run
    for g in GROUPS:
        return_file_name[g] = haystack.run(spark['spark'], spark['runner'], 'test', g, test=True)
        results[g] = spark['sqlContext'].table('haystack_deliverable').collect()

def test_hvid_obfuscation():
    result_w_hvid = [r for r in results[GROUPS[0]] if r['HVID'] is not None][0]
    assert result_w_hvid['HVID'] == str(obfuscate_hvid('650226624', 'hvid265'))

def test_temporary_id():
    result_w_hvid = [r for r in results[GROUPS[0]] if r['HVID'] is not None][0]
    assert result_w_hvid['Temporary ID'] is None

    result_wo_hvid = [r for r in results[GROUPS[0]] if r['HVID'] is  None][0]
    assert len(result_wo_hvid['Temporary ID']) == 36

def test_file_name():
    for g in GROUPS:
        return_file_name[g] == g + '_daily_report.psv.gz'

def test_cleanup(spark):
    cleanup(spark)
