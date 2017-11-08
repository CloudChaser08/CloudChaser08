import pytest

import spark.stats.config.reader.config_reader as config_reader
from spark.helpers.file_utils import get_abs_path

def test_reads_sub_conf():
    conf_file = get_abs_path(__file__, 'resources/main_config.json')
    conf = config_reader.generate_get_provider_config_function(conf_file, 'test')
    assert 'fill_rate' in conf
    assert conf['fill_rate'] != None
    assert conf['fill_rate']['sub_field'] == 'dang'
    assert conf['fill_rate']['cool_stuff'] == 'not really'


def test_does_not_read_sub_conf_when_null():
    conf_file = get_abs_path(__file__, 'resources/main_config.json')
    conf = config_reader.generate_get_provider_config_function(conf_file, 'other_test')
    assert 'fill_rate' in conf
    assert conf['fill_rate'] == None


def test_exception_raised_when_provider_conf_datatype_is_null():
    with pytest.raises(Exception) as e_info:
        conf_file = get_abs_path(__file__, 'resources/main_config.json')
        conf = config_reader.generate_get_provider_config_function(conf_file, 'bad_conf')

    exception = e_info.value
    assert exception.message.startswith('datatype is not specified for provider')


def test_exception_raised_when_provider_conf_datatype_not_specified():
    with pytest.raises(Exception) as e_info:
        conf_file = get_abs_path(__file__, 'resources/main_config.json')
        conf = config_reader.generate_get_provider_config_function(conf_file, 'bad_conf_2')

    exception = e_info.value
    assert exception.message.startswith('datatype is not specified for provider')


def test_exception_raised_when_provider_not_in_providers_conf_file():
    with pytest.raises(Exception) as e_info:
        conf_file = get_abs_path(__file__, 'resources/main_config.json')
        conf = config_reader.generate_get_provider_config_function(conf_file, 'lol')

    exception = e_info.value
    assert exception.message == 'lol is not in the providers config file'


def test_default_config_used_when_key_not_specified_in_json():
    conf_file = get_abs_path(__file__, 'resources/main_config.json')
    conf = config_reader.generate_get_provider_config_function(conf_file, 'test_missing_key')

    assert conf['fill_rate'] is not None
    assert conf['fill_rate']['cols'] == ['one', 'two']
    assert conf['fill_rate']['other_info'] == 'hi'

