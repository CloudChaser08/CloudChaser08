import pytest

from spark.spark_setup import init
from spark.runner import Runner


@pytest.fixture(scope="session")
def spark():
    spark, sqlContext = init("Tests", True)
    runner = Runner(sqlContext)

    yield {
        "runner": runner,
        "spark": spark,
        "sqlContext": sqlContext
    }

    spark.stop()
