import pytest

import spark.spark_setup as spark_setup
import spark.runner as spark_runner

from spark.spark_setup import init
from spark.runner import Runner
from pyspark.sql import DataFrame
from pyspark import RDD

import shutil


def is_prod(sqlContext):
    return len([
        r.tableName for r in sqlContext.sql('show tables').collect()
    ]) > 100


@pytest.fixture(scope="session")
def spark_session():
    shutil.rmtree('/tmp/checkpoint/', True)
    spark, sqlContext = init("Tests", True)
    runner = Runner(sqlContext)

    def df_repart(inst, cnt, *args):
        return inst._old_repartition(5 if cnt > 5 else cnt, *args)

    def rdd_repart(inst, cnt):
        return inst._old_repartition(5 if cnt > 5 else cnt)

    spark.real_stop = spark.stop
    spark.stop = lambda: None
    DataFrame._old_repartition = DataFrame.repartition
    DataFrame.repartition = df_repart
    RDD._old_repartition = RDD.repartition
    RDD.repartition = rdd_repart

    if is_prod(sqlContext):
        raise Exception("This test suite has access to the production metastore.")

    yield {
        "runner": runner,
        "spark": spark,
        "sqlContext": sqlContext
    }

    spark.real_stop()

@pytest.fixture(scope="module")
@pytest.mark.usefixtures("spark_session")
def spark(spark_session):
    spark_session['sqlContext'].clearCache()
    spark_session['sqlContext'].sql('SET spark.sql.shuffle.partitions=5')

    for tbl in spark_session['spark'].catalog.listTables():
        if tbl.tableType.lower() == 'temporary':
            # for temporary tables, there is no indication whether it's a table
            # or a view
            try:
                spark_session['spark'].sql('DROP VIEW {}'.format(tbl.name))
            except:
                spark_session['spark'].sql('DROP TABLE {}'.format(tbl.name))
        elif tbl.tableType.lower() == 'view':
            spark_session['spark'].sql('DROP VIEW {}'.format(tbl.name))
        else:
            spark_session['spark'].sql('DROP TABLE {}'.format(tbl.name))

    return spark_session

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
    for i in range(1, len(items)):
        is_last = items[i].parent != items[i-1].parent
        setattr(items[i-1], "is_last", is_last)

    if items:
        setattr(items[-1], "is_last", True)
