"""
    Stats fixtures
"""
import pytest
from mock import patch
from spark.stats.models.converters import convert_model_list
from spark.stats.models import Provider



@pytest.fixture(autouse=True, scope='session')
def sort_model_lists():
    """ This fixture is a rather hacky way of ensuring that nested lists of
        models are always sorted in testing. This assumes that order is not
        important when collecting nested lists of models, which at the time of
        writing is certainly the case.
    """
    with patch('spark.stats.models.converters.convert_model_list') as mock:
        mock.side_effect = lambda a, b: sorted(convert_model_list(a, b))
        yield


@pytest.fixture(name='provider_conf', scope='session')
def get_provider_config():
    """ Formats a default provider config to provide as a fixture to
        tests
    """
    yield Provider(
        name="test",
        datafeed_id="1",
        datatype="labtests",
        earliest_date="2018-01-01",
        fill_rate=True,
        key_stats=False,
        top_values=False,
        longitudinality=False,
        year_over_year=False,
        epi_calcs=False,
    )
