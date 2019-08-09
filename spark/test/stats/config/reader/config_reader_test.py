import pytest
from mock import Mock, patch

import spark.stats.config.reader.config_reader as config_reader
from spark.helpers.file_utils import get_abs_path
import spark.stats.config.dates as dates
from spark.stats import models

@patch('spark.stats.config.reader.config_reader._get_fill_rate_config')
def test_fill_rate_get_columns(get_fill_mock):
    fill_rate_conf = models.FillRateConfig(columns={})
    get_fill_mock.return_value = fill_rate_conf

    conf_file = get_abs_path(__file__, 'resources/main_config.json')
    conf = config_reader.get_provider_config(conf_file, '1')
    assert conf.fill_rate
    assert conf.fill_rate_conf
    assert conf.fill_rate_conf == fill_rate_conf


@patch('spark.stats.config.reader.config_reader._get_fill_rate_config')
def test_does_not_read_sub_conf_when_null(_):
    conf_file = get_abs_path(__file__, 'resources/main_config.json')
    conf = config_reader.get_provider_config(conf_file, '2')
    assert conf.fill_rate is False
    assert conf.fill_rate_conf is None


@patch('spark.stats.config.reader.config_reader._get_fill_rate_config')
def test_exception_raised_when_provider_conf_datatype_is_null(_):
    with pytest.raises(ValueError) as e_info:
        conf_file = get_abs_path(__file__, 'resources/main_config.json')
        config_reader.get_provider_config(conf_file, '3')

    exception = e_info.value
    assert 'datatype' in exception.message


@patch('spark.stats.config.reader.config_reader._get_fill_rate_config')
def test_exception_raised_when_provider_conf_datatype_not_specified(_):
    with pytest.raises(TypeError):
        conf_file = get_abs_path(__file__, 'resources/main_config.json')
        config_reader.get_provider_config(conf_file, '4')


@patch('spark.stats.config.reader.config_reader._get_fill_rate_config')
def test_exception_raised_when_provider_not_in_providers_conf_file(_):
    with pytest.raises(ValueError) as e_info:
        conf_file = get_abs_path(__file__, 'resources/main_config.json')
        config_reader.get_provider_config(conf_file, 'lol')

    exception = e_info.value
    assert exception.message == 'Feed lol is not in the providers config file'
