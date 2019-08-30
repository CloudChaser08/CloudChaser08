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
    assert [r for r in results[GROUPS[0]] if r.hvid is not None and r.hvid != 'HVID'][0] \
        .hvid == str(obfuscate_hvid('650226624', 'hvid265'))

def test_temporary_id():
    assert [r for r in results[GROUPS[0]] if r.hvid is not None and r.hvid != 'HVID'][0] \
        .temporary_id is None

    assert len([r for r in results[GROUPS[0]] if r.hvid is None][0]
            .temporary_id) == 36

def test_header_row():
    assert results[GROUPS[0]][0].temporary_id == 'Temporary ID'
    assert results[GROUPS[0]][0].hvid == 'HVID'
    assert results[GROUPS[0]][0].activity_date == 'Activity Date'

def test_file_name():
    for g in GROUPS:
        return_file_name[g] == g + '_daily_report.psv.gz'

def test_cleanup(spark):
    cleanup(spark)
