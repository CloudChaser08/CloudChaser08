import pytest

import spark.spark_setup as spark_setup
import spark.runner as spark_runner

from spark.spark_setup import init
from spark.runner import Runner
from pyspark.sql import DataFrame

import shutil


def is_prod(sqlContext):
    return len([
        r.tableName for r in sqlContext.sql('show tables').collect()
    ]) > 100


@pytest.fixture(scope="session")
def spark():
    shutil.rmtree('/tmp/checkpoint/', True)
    spark, sqlContext = init("Tests", True)
    sqlContext.sql('SET spark.sql.shuffle.partitions=5')
    runner = Runner(sqlContext)

    DataFrame._old_repartition = DataFrame.repartition
    def repart(inst, cnt, *args):
        return inst._old_repartition(5, *args)
    DataFrame.repartition = repart

    if is_prod(sqlContext):
        raise Exception("This test suite has access to the production metastore.")

    yield {
        "runner": runner,
        "spark": spark,
        "sqlContext": sqlContext
    }

    spark.stop()

@pytest.fixture
@pytest.mark.usefixtures("spark")
def patch_spark_init(spark, monkeypatch):
    """
    Patch the spark init and runner instantion to use a spark session
    for local testing
    """
    def spark_init(name):
        return (spark, sqlContext)

    def runner_init(sqlCtx):
        return runner

    monkeypatch.setattr(spark_setup, 'init', spark_init)
    monkeypatch.setattr(spark_runner, 'Runner', runner_init)

@pytest.hookimpl(tryfirst=True, hookwrapper=True)
def pytest_runtest_makereport(item, call):
    # execute all other hooks to obtain the report object
    outcome = yield

    if item.config.getoption('verbose', 0) <= 0:
        return

    rep = outcome.get_result()

    if item.config.getoption('verbose', 0) > 1:
        if call.when == "call":
            setattr(item, "duration", rep.duration)
        elif call.when == "teardown":
            print("\nTest '{}' DURATION={:.2f}s".format(item.nodeid, item.duration))
    else:
        tot_duration = 0
        if call.when == "call":
            if hasattr(item.parent, 'duration'):
                tot_duration = item.parent.duration + rep.duration
            else:
                tot_duration = rep.duration
            setattr(item.parent, "duration", tot_duration)

        if hasattr(item, 'is_last') and item.is_last and call.when == "teardown":
            print("\nTest module '{}' DURATION={:.2f}s".format(item.parent.nodeid, item.parent.duration))


@pytest.hookimpl
def pytest_collection_modifyitems(session, config, items):
    for i in xrange(1, len(items)):
        is_last = items[i].parent != items[i-1].parent
        setattr(items[i-1], "is_last", is_last)

    if items:
        setattr(items[-1], "is_last", True)
