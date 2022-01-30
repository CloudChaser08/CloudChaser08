import pytest

import spark.qa.datafeed as qa_datafeed


@pytest.fixture(scope='session')
def datafeed():
    yield qa_datafeed.active_datafeed


# parametrize tests for this datafeed
def pytest_generate_tests(metafunc):

    # validation tests
    if 'validation' in metafunc.fixturenames:
        metafunc.parametrize("validation", qa_datafeed.active_datafeed.validations, ids=[
            validation.column_name for validation in qa_datafeed.active_datafeed.validations
        ]) 

    # full fill rate tests
    elif 'full_fill_column' in metafunc.fixturenames:
        metafunc.parametrize("full_fill_column",
                             qa_datafeed.active_datafeed.target_full_fill_columns)

    # unique_match tests
    elif 'unique_match_pair' in metafunc.fixturenames:
        metafunc.parametrize(
            "unique_match_pair", qa_datafeed.active_datafeed.unique_match_pairs, ids=[
                unique_match_pair.target_column_name for unique_match_pair
                in qa_datafeed.active_datafeed.unique_match_pairs
        ])
