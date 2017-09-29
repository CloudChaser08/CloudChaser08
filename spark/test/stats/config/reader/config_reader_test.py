import pytest

import spark.stats.config.reader.config_reader as config_reader
from spark.helpers.file_utils import get_abs_path

def test_reads_sub_conf():
    conf_file = get_abs_path(__file__, 'resources/main_config.json')
    conf = config_reader.get_provider_config('test', conf_file)
    assert 'fill_rate' in conf
    assert conf['fill_rate'] != None
    assert conf['fill_rate']['sub_field'] == 'dang'
    assert conf['fill_rate']['cool_stuff'] == 'not really'


def test_does_not_read_sub_conf_when_null():
    conf_file = get_abs_path(__file__, 'resources/main_config.json')
    conf = config_reader.get_provider_config('other_test', conf_file)
    assert 'fill_rate' in conf
    assert conf['fill_rate'] == None


def test_exception_raised_when_provider_conf_missing_stat_conf_field():
    with pytest.raises(Exception) as e_info:
        conf_file = get_abs_path(__file__, 'resources/main_config.json')
        conf = config_reader.get_provider_config('bad_conf', conf_file)

    exception = e_info.value
    assert exception.message.startswith('No config for')


def test_exception_raised_when_provider_not_in_providers_conf_file():
    with pytest.raises(Exception) as e_info:
        conf_file = get_abs_path(__file__, 'resources/main_config.json')
        conf = config_reader.get_provider_config('lol', conf_file)

    exception = e_info.value
    assert exception.message == 'lol is not in the providers config file'


