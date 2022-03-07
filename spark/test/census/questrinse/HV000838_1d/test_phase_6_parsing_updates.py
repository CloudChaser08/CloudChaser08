"""
Phase 6 Quest RINSE test conditions

Derived tests from:
https://healthverity.atlassian.net/browse/DE-826
eval_test(result_value, expect_op, expect_num, expect_alpha, expect_passthru, spark)
"""
import pytest
pytest.register_assert_rewrite("spark.test.census.questrinse.HV000838_1c.questrinse_helper")
from spark.test.census.questrinse.HV000838_1d.questrinse_helper import eval_test


# Regression tests were passing before Phase 5
def test_undersired_alpha(spark):

    eval_test('<=0.5     **', '<=', '0.5', '', '', spark)

    eval_test('<4~A', '<', '4', '', '', spark)

    eval_test('<1.0 LL', '<', '1.0', '', '', spark)

    eval_test('>8.0 *', '>', '8.0', '', '', spark)


# Conditions found during validation that had failed, created to prevent regression
# of the NUMBERLIKE regex.
def test_regression_multislash_error(spark):
    eval_test('/457/466/859/861', '', '', '', '/457/466/859/861', spark)
    eval_test('/94032/16900/16902/16904/16905/16909/16911/70208/93039/91258',
              '',
              '',
              '',
              '/94032/16900/16902/16904/16905/16909/16911/70208/93039/91258',
              spark)
    eval_test('/905/3640/17306/36126/5363/10306/36127/91431',
              '',
              '',
              '',
              '/905/3640/17306/36126/5363/10306/36127/91431',
              spark)
    eval_test('333/05/2021"', '', '', '', '333/05/2021"', spark)


@pytest.mark.usefixtures("spark")
def test_numeric_begin_comma(spark):
    # Pass thru due to alpha after
    eval_test('<,300',
              '',
              '',
              '',
              '<,300',
              spark)
