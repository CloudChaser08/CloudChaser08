import datetime
import pytest

import spark.helpers.udf.general_helpers as gh


def test_extract_number():
    assert gh.extract_number('10') == 10
    assert gh.extract_number('xx') is None
    assert gh.extract_number('$1.90') == 1.9
    assert gh.extract_number('$.1.90') == 190


def test_extract_date():
    assert gh.extract_date(None, None) is None

    assert gh.extract_date('1990-Jun-01', '%Y-%b-%d') == '1990-06-01'

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


def test_create_range():
    assert gh.create_range(10) == '0,1,2,3,4,5,6,7,8,9'

    # invalid input
    assert gh.create_range('') is None
    assert gh.create_range(None) is None


def test_string_set_diff():
    # TODO: should these also return 30?
    assert gh.string_set_diff('10_x:20_x', '10_x:30_x') == '20'
    assert gh.string_set_diff('10_x:20_x:40_x', '10_x:30_x') == '20:40'


def test_uniquify():
    assert gh.uniquify('10:10:10') == '10'
    assert gh.uniquify('10:10:20') == '10:20'

def test_is_int():
    assert gh.is_int('1')
    assert gh.is_int(1)
    assert gh.is_int(1.0)
    assert not gh.is_int('1.1')
    assert not gh.is_int(1.1)
    assert not gh.is_int('string')
    assert not gh.is_int(None)

def test_obfuscate_hvid():
    assert gh.obfuscate_hvid('1234567', 'CPQ-013') == 'CEB8F9B33421E4DEB16CE7FE1358D554'
    with pytest.raises(ValueError) as excinfo:
        gh.obfuscate_hvid('1234567', None)
    assert str(excinfo.value) == 'A project-specific salt must be provided to properly obfuscate the HVID'

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
