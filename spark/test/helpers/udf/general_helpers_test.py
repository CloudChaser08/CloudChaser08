import datetime
import pytest

import spark.helpers.udf.general_helpers as gh


def test_extract_number():
    assert gh.extract_number('10') == 10
    assert gh.extract_number('xx') is None
    assert gh.extract_number('$1.90') == 1.9
    assert gh.extract_number('$.1.90') == 190

def test_clean_up_freetext():
    assert "3123LLL &&&" == gh.clean_up_freetext("3123LLL   ,,,&&&   ", remove_periods=True)
    assert gh.clean_up_freetext(None) is None

def test_cap_date():
    test_date = datetime.date(1990, 6, 1)
    assert test_date == gh.cap_date(test_date, None, None)
    assert test_date == gh.cap_date(test_date, datetime.date(1980, 6, 1), None)
    assert test_date == gh.cap_date(test_date, datetime.date(1980, 6, 1), datetime.date(1991, 6, 1))
    assert not gh.cap_date(test_date, datetime.date(1995, 6, 1), None)
    assert not gh.cap_date(test_date, None, datetime.date(1985, 6, 1))

def test_extract_date():
    assert gh.extract_date(None, None) is None

    assert gh.extract_date('1990-Jun-01', '%Y-%b-%d') == datetime.date(1990, 6, 1)

    # max date cap
    assert gh.extract_date(
        '1990-Jun-01', '%Y-%b-%d', max_date=datetime.date(1990, 1, 1)
    ) == None

    # min date cap
    assert gh.extract_date(
        '1990-Jun-01', '%Y-%b-%d', min_date=datetime.date(2000, 1, 1)
    ) == None


def test_extract_currency():
    assert gh.extract_currency('2016') == 2016
    assert gh.extract_currency('xxx') == None
    assert gh.extract_currency(None) == None


def test_convert_celsius_to_fahrenheit():
    assert gh.convert_celsius_to_fahrenheit(None) is None

    assert gh.convert_celsius_to_fahrenheit(0) == 32
    assert gh.convert_celsius_to_fahrenheit(-40) == -40
    assert gh.convert_celsius_to_fahrenheit(100) == 212

    assert gh.convert_celsius_to_fahrenheit(34) == 93.2


def test_convert_cm_to_in():
    assert gh.convert_cm_to_in(None) is None

    assert gh.convert_cm_to_in(0) == 0
    assert gh.convert_cm_to_in(0.1) == 0.04
    assert gh.convert_cm_to_in(2) == 0.79
    assert gh.convert_cm_to_in(100) == 39.37


def test_convert_m_to_in():
    assert gh.convert_m_to_in(None) is None

    assert gh.convert_m_to_in(0) == 0
    assert gh.convert_m_to_in(0.1) == 3.94
    assert gh.convert_m_to_in(1) == 39.37
    assert gh.convert_m_to_in(5) == 196.85


def test_convert_kg_to_lb():
    assert gh.convert_kg_to_lb(None) is None

    assert gh.convert_kg_to_lb(0) == 0
    assert gh.convert_kg_to_lb(0.5) == 1.1
    assert gh.convert_kg_to_lb(5) == 11.02
    assert gh.convert_kg_to_lb(10) == 22.05
    assert gh.convert_kg_to_lb(59) == 130.07


def test_create_range():
    assert gh.create_range(10) == '0,1,2,3,4,5,6,7,8,9'

    # invalid input
    assert gh.create_range('') is None
    assert gh.create_range(None) is None


def test_string_set_diff():
    # TODO: should these also return 30?
    assert gh.string_set_diff('10_x:20_x', '10_x:30_x') == '20'
    assert set(gh.string_set_diff('10_x:20_x:40_x', '10_x:30_x').split(':')) == set(['20', '40'])


def test_uniquify():
    assert gh.uniquify('10:10:10') == '10'
    assert sorted(gh.uniquify('10:10:20').split(':')) == sorted('10:20'.split(':'))

def test_is_int():
    assert gh.is_int('1')
    assert gh.is_int(1)
    assert gh.is_int(1.0)
    assert not gh.is_int('1.1')
    assert not gh.is_int(1.1)
    assert not gh.is_int('string')
    assert not gh.is_int(None)

def test_obfuscate_hvid():
    assert gh.obfuscate_hvid('1234567', 'CPQ-013') == 'ceb8f9b33421e4deb16ce7fe1358d554'
    with pytest.raises(ValueError) as excinfo:
        gh.obfuscate_hvid('1234567', None)
    assert str(excinfo.value) == 'A project-specific salt must be provided to properly obfuscate the HVID'
    assert gh.obfuscate_hvid('', 'CPQ-013') is None
    assert gh.obfuscate_hvid('    ', 'CPQ-013') is None
    assert gh.obfuscate_hvid(None, 'CPQ-013') is None

def test_slightly_obfuscate_hvid():
    assert gh.slightly_obfuscate_hvid(1234567, 'CPQ-013') == 2004332781
    assert gh.slightly_obfuscate_hvid(None, 'CPQ-013') is None
    with pytest.raises(ValueError) as excinfo:
        gh.slightly_obfuscate_hvid(1234567, None)
    assert str(excinfo.value) == 'A project-specific key must be provided to properly obfuscate the HVID'
    with pytest.raises(ValueError) as excinfo:
        gh.slightly_obfuscate_hvid('1234567', 'CPQ-013')
    assert str(excinfo.value) == 'Only integer HVIDs are expected'

def test_slightly_deobfuscate_hvid():
    assert gh.slightly_deobfuscate_hvid(2004332781, 'CPQ-013') == 1234567
    assert gh.slightly_deobfuscate_hvid(gh.slightly_obfuscate_hvid(1234567, 'CPQ-013'), 'CPQ-013') == 1234567


def test_remove_split_suffix():
    assert gh.remove_split_suffix('/parent/dir/myfile.txt') == 'myfile.txt'
    assert gh.remove_split_suffix('/parent/dir/myfile.txt.aa.bz2') == 'myfile.txt'
    assert gh.remove_split_suffix('/parent/dir/myfile.txt', True) == '/parent/dir/myfile.txt'
    assert gh.remove_split_suffix('/parent/dir/myfile.txt.aa.bz2', True) == '/parent/dir/myfile.txt'

def test_densify_scalar_array():
    assert gh.densify_scalar_array([1,None,2,None,3]) == [1,2,3]
    assert gh.densify_scalar_array([None,None]) == []
    assert gh.densify_scalar_array([[1],[None]]) == [[1], [None]]

def test_densify_2d_array():
    assert gh.densify_2d_array([[1],[None]]) == [[1]]
    assert gh.densify_2d_array([[1,None,2],[None,None,None]]) == [[1,None,2]]
    assert gh.densify_2d_array([[None,None,None],[None,None,None]]) == [[None,None,None]]

def test_densify_2d_array_by_key():
    assert gh.densify_2d_array_by_key([[1, 2, 3], [4, 5, 6]]) == [[1, 2, 3], [4, 5, 6]]
    assert gh.densify_2d_array_by_key([[None, 2, 3], [4, 5, 6]]) == [[4, 5, 6]]
    assert gh.densify_2d_array_by_key([[1, None, 3], [None, 5, 6]]) == [[1, None, 3]]
    assert gh.densify_2d_array_by_key([[None, 2, 3], [None, 5, None]]) == [[None, 2, 3]]

def test_obfuscate_candidate_hvids():
    assert gh.obfuscate_candidate_hvids([['1234567', 1]], 'CPQ-013') == [['ceb8f9b33421e4deb16ce7fe1358d554', 1]]


def test_find_descendants_recursively():
    parents = [1, 1, 2, 2, 2, 2, 3, 4, 5, 5, 5, 5, 6, 6, 6, 7, 8, 9, 9, 10, 10, 11, 11, 12, 13]
    children = [1, 2, 1, 2, 3, 4, 3, 4, 5, 6, 7, 8, 6, 7, 8, 7, 8, 9, 10, 10, 11, 11, 12, 12, 13]
    expected_result = {1: {1, 2, 3, 4}, 13: {13}, 5: {8, 5, 6, 7}, 9: {9, 10, 11, 12}}
    results = gh.find_descendants_recursively(parents, children)

    for key in results.keys():
        assert key in expected_result.keys()

    for value in results.values():
        assert value in expected_result.values()
