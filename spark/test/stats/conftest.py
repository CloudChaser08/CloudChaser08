"""
    Stats fixtures
"""
import pytest
from spark.stats.models import Provider

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
