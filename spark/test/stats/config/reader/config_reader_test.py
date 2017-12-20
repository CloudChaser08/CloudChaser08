import pytest
from mock import Mock

import spark.stats.config.reader.config_reader as config_reader
from spark.helpers.file_utils import get_abs_path

@pytest.fixture(autouse=True)
def setup_teardown():
    old_get_fill_rate_cols_fn = config_reader._get_fill_rate_columns

    yield

    config_reader._get_fill_rate_columns = old_get_fill_rate_cols_fn


def test_fill_rate_get_columns():

    config_reader._get_fill_rate_columns = Mock(return_value=["one", "two"])

    conf_file = get_abs_path(__file__, 'resources/main_config.json')
    conf = config_reader.get_provider_config(conf_file, 'test')
    assert 'fill_rate' in conf
    assert 'fill_rate_conf' in conf
    assert conf['fill_rate']
    assert conf['fill_rate_conf']
    assert conf['fill_rate_conf'] == {"columns": ['one', 'two']}


def test_does_not_read_sub_conf_when_null():
    conf_file = get_abs_path(__file__, 'resources/main_config.json')
    conf = config_reader.get_provider_config(conf_file, 'other_test')
    assert 'fill_rate' in conf
    assert conf['fill_rate'] == None


def test_exception_raised_when_provider_conf_datatype_is_null():
    with pytest.raises(Exception) as e_info:
        conf_file = get_abs_path(__file__, 'resources/main_config.json')
        conf = config_reader.get_provider_config(conf_file, 'bad_conf')

    exception = e_info.value
    assert exception.message.startswith('datatype is not specified for provider')


def test_exception_raised_when_provider_conf_datatype_not_specified():
    with pytest.raises(Exception) as e_info:
        conf_file = get_abs_path(__file__, 'resources/main_config.json')
        conf = config_reader.get_provider_config(conf_file, 'bad_conf_2')

    exception = e_info.value
    assert exception.message.startswith('datatype is not specified for provider')


def test_exception_raised_when_provider_not_in_providers_conf_file():
    with pytest.raises(Exception) as e_info:
        conf_file = get_abs_path(__file__, 'resources/main_config.json')
        conf = config_reader.get_provider_config(conf_file, 'lol')

    exception = e_info.value
    assert exception.message == 'lol is not in the providers config file'
