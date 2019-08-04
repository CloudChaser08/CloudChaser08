import pytest
from mock import Mock

import spark.stats.calc.epi as epi

result = None

def test_init():
    old_get_s3_file_contents = epi._get_s3_file_contents
    epi._get_s3_file_contents = Mock(return_value = 'part-00001;82;515151;0-17\npart-00002;82;2727;18-44\npart-00003;127;8;45-64;extra woops!\n')

    global result
    result = epi.calculate_epi({'datafeed_id': '27'}, 'age')

    epi._get_s3_file_contents = old_get_s3_file_contents


def test_lines_split_properly():
    assert len(result) == 3


def test_keys_are_named_correctly():
    for r in result:
        assert 'field' in r
        assert 'value' in r


def test_extra_separators_added_in_field():
    assert result[2]['field'] == '45-64;extra woops!'


def test_list_populated_correctly():
    assert [x for x in result if x['field'] == '0-17'][0]['value'] == '515151'
    assert [x for x in result if x['field'] == '18-44'][0]['value'] == '2727'
    assert [x for x in result if x['field'] == '45-64;extra woops!'][0]['value'] == '8'

