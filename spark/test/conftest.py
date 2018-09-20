import pytest

from spark.spark_setup import init
from spark.runner import Runner

import shutil


def is_prod(sqlContext):
    return len([
        r.tableName for r in sqlContext.sql('show tables').collect()
    ]) > 100


@pytest.fixture(scope="session")
def spark():
    shutil.rmtree('/tmp/checkpoint/', True)
    spark, sqlContext = init("Tests", True)
    runner = Runner(sqlContext)

    if is_prod(sqlContext):
        raise Exception("This test suite has access to the production metastore.")

    yield {
        "runner": runner,
        "spark": spark,
        "sqlContext": sqlContext
    }

    spark.stop()

@pytest.fixture(scope='module', autouse=True)
def testcase_result(request):
    def fin():
        print("Test module '{}' DURATION={}".format(request.node.nodeid, request.node.duration))
    request.addfinalizer(fin)

@pytest.hookimpl(tryfirst=True, hookwrapper=True)
def pytest_runtest_makereport(item, call):
    # execute all other hooks to obtain the report object
    outcome = yield
    rep = outcome.get_result()

    # set a report attribute for each phase of a call, which can
    # be "setup", "call", "teardown"

    tot_duration = 0
    if hasattr(item.parent, 'duration'):
        tot_duration = item.parent.duration + rep.duration
    else:
        tot_duration = rep.duration
    setattr(item.parent, "duration", tot_duration)
