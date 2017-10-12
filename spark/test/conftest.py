import pytest

from spark.spark_setup import init
from spark.runner import Runner


def is_prod(sqlContext):
    return len([
        r.tableName for r in sqlContext.sql('show tables').collect()
    ]) > 100


@pytest.fixture(scope="session")
def spark():
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
