import pytest
import spark.delivery.foresite_hv000321.sparkExtractForesite as foresite


@pytest.mark.usefixtures("spark")
def test_init(spark):
    foresite.run(spark['spark'], spark['runner'], '2017-05-01', True)