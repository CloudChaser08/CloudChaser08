import pytest
from datetime import datetime
import airflow.models
import mock
import util.date_utils as date_utils

quest = None

ds = {}
kwargs = {
    'execution_date': datetime(2017,4,25),
    'yesterday_ds_nodash': '20170424',
    'ds_nodash': '20170425'
}

expected_formatted_date = '201704220423'
execution_date = kwargs['ds_nodash']

real_airflow_models_Variable_get = airflow.models.Variable.get


@pytest.fixture(autouse=True)
def setup_teardown():
    airflow.models.Variable.get = mock.MagicMock(
        return_value='dev'
    )

    # must be imported after Variable is mocked
    global quest
    import quest_pipeline as quest

    real_s3_validate_file_s3_validate_file = \
        quest.s3_validate_file.s3_validate_file
    real_s3_fetch_file_s3_fetch_file = \
        quest.s3_fetch_file.s3_fetch_file

    yield  # run test

    # reset mocked objects
    airflow.models.Variable.get = real_airflow_models_Variable_get
    quest.s3_validate_file.s3_validate_file = \
        real_s3_validate_file_s3_validate_file
    quest.s3_fetch_file.s3_fetch_file = \
        real_s3_fetch_file_s3_fetch_file


def test_get_formatted_date():
    assert quest.get_formatted_date(ds, kwargs) == expected_formatted_date


def test_insert_formatted_date_function():
    template = '_{}_'
    assert quest.insert_formatted_date_function(template)(ds, kwargs) \
          == template.format(expected_formatted_date)


def test_insert_execution_date_function():
    template = '_{}_'
    assert date_utils.generate_insert_date_into_template_function(
        template.format('{}{}{}')) \
        == '_{}_'.format(execution_date)


def test_insert_formatted_regex_function():
    template = '_{}_'
    assert quest.insert_formatted_regex_function(template)(ds, kwargs) \
        == '_{}_'.format('\d{12}')


def test_inserting_current_date():
    template = '_{}_{}_{}_'
    assert date_utils.insert_date_into_template(template, k, day_offset = -3) \
        == '_2017_04_22_'


def test_inserting_current_date_function():
    template = '_{}_{}_{}_'
    assert template.format(quest.get_formatted_date(ds, kwargs)) \
        == '_2017_04_22_'


def test_encrypted_decrypted_file_paths_function():
    assert quest.encrypted_decrypted_file_paths_function(ds, kwargs) == [[
        '/tmp/quest/labtests/20170425/raw/addon/HealthVerity_{}_1_PlainTxt.txt'.format(
            expected_formatted_date
        ),
        '/tmp/quest/labtests/20170425/raw/addon/HealthVerity_{}_1_PlainTxt.txt.gz'.format(
            expected_formatted_date
        )
    ]]


def test_file_validation_dag():
    "Ensure that the dag generated is called with the correct keys"
    quest.s3_validate_file.s3_validate_file = mock.Mock(
        # return a (mocked) subdag
        return_value=mock.Mock(
            dag_id='quest_pipeline.validate_test_file'
        )
    )

    quest.generate_transaction_file_validation_dag(
        'test', '{}', 0
    )

    quest.s3_validate_file.s3_validate_file.assert_called_with(
        quest.DAG_NAME,
        'validate_test_file',
        quest.default_args['start_date'],
        quest.mdag.schedule_interval,
        {
            'expected_file_name_func': mock.ANY,
            'file_name_pattern_func': mock.ANY,
            'minimum_file_size': 0,
            's3_prefix': mock.ANY,
            's3_bucket': mock.ANY,
            'file_description': 'Quest test file'
        }
    )


def test_file_fetch_dag():
    "Ensure that the dag generated is called with the correct keys"
    quest.s3_fetch_file.s3_fetch_file = mock.Mock(
        # return a (mocked) subdag
        return_value=mock.Mock(
            dag_id='quest_pipeline.fetch_test_file'
        )
    )

    quest.generate_fetch_dag(
        'test', 's3{}', 'local{}', '{}'
    )

    quest.s3_fetch_file.s3_fetch_file.assert_called_with(
        quest.DAG_NAME,
        'fetch_test_file',
        quest.default_args['start_date'],
        quest.mdag.schedule_interval,
        {
            'tmp_path_template': 'local{}',
            'expected_file_name_func': mock.ANY,
            's3_prefix': mock.ANY,
            's3_bucket': mock.ANY,
        }
    )

    assert list(quest.s3_fetch_file.s3_fetch_file.call_args)[0][4][
        'expected_file_name_func'
    ](ds, kwargs) == expected_formatted_date
