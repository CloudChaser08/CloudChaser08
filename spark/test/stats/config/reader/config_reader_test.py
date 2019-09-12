import json

import pytest
from mock import Mock, patch

import spark.stats.config.reader.config_reader as config_reader
from spark.helpers.file_utils import get_abs_path
from spark.stats import models

TABLE = models.TableMetadata(
    name='tbl',
    description='desc',
    columns=[
        models.Column(
            name='col-a',
            field_id='1',
            sequence='1',
            datatype='bigint',
            description='Column A',
            top_values=True,
            category='Baseline',
        )
    ]
)

@pytest.fixture(name='get_config_file', autouse=True)
def _mock_providers_json():
    with patch.object(config_reader,
                      '_load_providers_json_from_s3') as load_json_mock:
        conf_file = get_abs_path(__file__, 'resources/main_config.json')
        with open(conf_file) as inf:
            contents = json.load(inf)
        load_json_mock.return_value = contents
        yield contents

@pytest.fixture(name='get_table', autouse=True)
def _mock_get_table():
    with patch.object(config_reader, 'get_table_metadata') as get_cols_mock:
        get_cols_mock.return_value = TABLE
        yield get_cols_mock



def test_get_table(get_table):
    """ Tests gets columns for the provider """
    sql_context = Mock()
    conf = config_reader.get_provider_config(sql_context, '1')
    assert conf.table == TABLE
    get_table.assert_called_with(
        sql_context, 'labtests'
    )


def test_gets_emr_columns(get_table):
    """ Tests gets columns for the provider """

    sql_context = Mock()
    conf = config_reader.get_provider_config(sql_context, '6')
    get_table.assert_called_with(sql_context, 'emr_clin_obsn')
    assert not conf.table
    assert conf.models[0].table == TABLE


def test_datatype_null():
    """ Tests raises an exception when datatype is null or missing """
    with pytest.raises(ValueError, match='datatype'):
        config_reader.get_provider_config(Mock(), '3')


def test_datatype_missing():
    """ Tests raises an exception when datatype is missing """
    with pytest.raises(TypeError):
        config_reader.get_provider_config(Mock(), '4')


def test_missing_provider():
    """ Tests raises an exception when provdier is not in providers file """
    with pytest.raises(ValueError) as e_info:
        config_reader.get_provider_config(Mock(), 'lol')

    exception = e_info.value
    assert str(exception) == 'Feed lol is not in the providers config file'
