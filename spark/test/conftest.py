import pytest

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

@pytest.hookimpl(tryfirst=True, hookwrapper=True)
def pytest_runtest_makereport(item, call):
    # execute all other hooks to obtain the report object
    outcome = yield
    rep = outcome.get_result()

    tot_duration = 0
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
    setattr(items[-1], "is_last", True)
