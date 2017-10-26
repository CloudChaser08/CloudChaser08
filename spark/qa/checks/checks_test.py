import pytest

from pyspark.sql.functions import col

@pytest.mark.usefixtures("datafeed")
def test_all_claims_in_src_and_target(datafeed):

    # check dependencies
    if not datafeed.source_data                       \
       or not datafeed.target_data                    \
       or not datafeed.source_data_claim_column_name  \
       or not datafeed.source_data_claim_table_name   \
       or not datafeed.target_data_claim_column_name:
        pytest.skip("Missing dependencies")

    src_claims = datafeed.source_data[datafeed.source_data_claim_table_name].select(
        col(datafeed.source_data_claim_column_name)
    ).distinct().rdd
    src_claim_count = src_claims.count()

    target_claims = datafeed.target_data.select(col(datafeed.target_data_claim_column_name)).distinct().rdd
    target_claim_count = target_claims.count()

    common_claim_count = src_claims.intersection(target_claims).count()

    assert common_claim_count == src_claim_count, \
        "Claims in source did not exist in target. Examples: {}".format(', '.join([
            r.column1 for r in src_claims.subtract(target_claims).take(10)
        ]))

    assert common_claim_count == target_claim_count, \
        "Claims in target did not exist in source. Examples: {}".format(', '.join([
            r.column1 for r in target_claims.subtract(src_claims).take(10)
        ]))
