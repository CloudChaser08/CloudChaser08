import pytest

import spark.qa.datafeed as qa_datafeed


@pytest.fixture(scope='session')
def datafeed():
    yield qa_datafeed.active_datafeed
