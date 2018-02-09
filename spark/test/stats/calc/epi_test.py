import pytest
from mock import Mock

import spark.stats.calc.epi as epi

result = None

def test_init():
    old_get_s3_file_contents = epi._get_s3_file_contents
    epi._get_s3_file_contents = Mock(return_value = 'junk;more junk;515151;0-17\njunk;even more junk;2727;18-44\njunk;more junk;8;45-64\n')

    global result
    result = epi.calculate_epi({'datafeed_id': '27'}, 'age')

    epi._get_s3_file_contents = old_get_s3_file_contents


def test_lines_split_properly():
    assert len(result) == 3


def test_keys_are_named_correctly():
    for r in result:
        assert 'field' in r.keys()
        assert 'value' in r.keys()


def test_list_populated_correctly():
    assert filter(lambda x: x['field'] == '0-17', result)[0]['value'] == '515151'
    assert filter(lambda x: x['field'] == '18-44', result)[0]['value'] == '2727'
    assert filter(lambda x: x['field'] == '45-64', result)[0]['value'] == '8'

