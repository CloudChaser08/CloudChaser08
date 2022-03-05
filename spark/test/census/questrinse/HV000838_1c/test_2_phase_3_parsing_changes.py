"""
Phase 3 Quest RINSE test conditions

Derived tests from:
https://docs.google.com/spreadsheets/d/14JArxwTBSlX44kcmJ5x0yNeEFZD2Go1d8Zt_iXojpyA/edit#gid=0

eval_test(result_value, expect_op, expect_num, expect_alpha, expect_passthru, spark)
"""
import pytest
pytest.register_assert_rewrite("spark.test.census.questrinse.HV000838_1c.questrinse_helper")

from spark.test.census.questrinse.HV000838_UDF_1c_1d.questrinse_helper import eval_test


def test_more_than_n_years(spark):
    """
    Check Variations of 'more than 10 years'
    """
    eval_test('More than 10 years', '>', '10', 'years', '', spark)
    eval_test('more than 10 years', '>', '10', 'years', '', spark)
    eval_test('more than 10 Years', '>', '10', 'Years', '', spark)


def test_less_than_n_years(spark):
    """
    Check Variations of 'less than 10 years'
    """
    eval_test('Less than 10 years', '<', '10', 'years', '', spark)
    eval_test('less than 10 years', '<', '10', 'years', '', spark)
    eval_test('less than 10 Years', '<', '10', 'Years', '', spark)


def test_greater_than_n_years(spark):
    """
    Check Variations of 'greater than 10 years'
    """
    eval_test('Greater than 10 years', '>', '10', 'years', '', spark)
    eval_test('greater than 10 years', '>', '10', 'years', '', spark)
    eval_test('greater than 10 Years', '>', '10', 'Years', '', spark)


def test_tilde_r(spark):
    """
    Check `=16~R`
    """
    eval_test('<=16~R', '<=', '16', 'R', '', spark)
    eval_test('<= 16~R', '<=', '16', 'R', '', spark)
    eval_test('<=16 ~R', '<=', '16', 'R', '', spark)


def test_tilde_s(spark):
    """
    Check `=.023~S`
    """
    eval_test('>=.023~S', '>=', '0.023', 'S', '', spark)


# noinspection PyPep8Naming
def test_date_neg_M_D_YY(spark):
    """
    Check `2-27-18 NEG`
    """
    eval_test('2-27-18 NEG', '', '', '', '2-27-18 NEG', spark)
    eval_test('12-07-18 NEG', '', '', '', '12-07-18 NEG', spark)
    eval_test('2-1-18 NEG', '', '', '', '2-1-18 NEG', spark)
    eval_test('12-1-19 NEG', '', '', '', '12-1-19 NEG', spark)


# noinspection PyPep8Naming
def test_date_negative_M_D_YYYY(spark):
    """
    Check `8-17-2017 NEGATIVE`
    """
    eval_test('8-17-2017 NEGATIVE', '', '', '', '8-17-2017 NEGATIVE', spark)
    eval_test('10-17-2015 NEGATIVE', '', '', '', '10-17-2015 NEGATIVE', spark)
    eval_test('1-9-2020 NEGATIVE', '', '', '', '1-9-2020 NEGATIVE', spark)
    eval_test('08-07-2017 NEGATIVE', '', '', '', '08-07-2017 NEGATIVE', spark)


# noinspection PyPep8Naming
@pytest.mark.skip("No longer the standard per phase 5.")
def test_date_neg_M_slash_YYYY(spark):
    """
    Check `10/2014- neg`
    also 10/2014 - NEG`
    """
    eval_test('10/2014- neg', '', '', '', '10/2014- neg', spark)
    eval_test('10/2014 - NEG', '', '', '', '10/2014 - NEG', spark)


# noinspection PyPep8Naming
def test_double_date_neg_M_slash_YYYY(spark):
    """
    Check `12/16-8/15 NEG`
    """
    eval_test('12/16-8/15 NEG', '', '', '', '12/16-8/15 NEG', spark)
    eval_test('1/21-10/15 NEG', '', '', '', '1/21-10/15 NEG', spark)
    eval_test('10/16-10/15 NEG', '', '', '', '10/16-10/15 NEG', spark)

@pytest.mark.skip("numeric before alpha not passthru")
def test_tnp_14839(spark):
    """
    Check `14839/TNP`
    """
    eval_test('14839/TNP', '', '', 'TEST NOT PERFORMED', '', spark)

@pytest.mark.skip("numeric before alpha not passthru")
def test_indicated_395(spark):
    """
    Check `395-INDICATED`
    """
    eval_test('395-INDICATED', '', '', 'INDICATED', '', spark)

@pytest.mark.skip("numeric before alpha not passthru")
def test_culture_indicated_3020(spark):
    """
    Check `3020/CULTURE INDICATED`
    """
    eval_test('3020/CULTURE INDICATED', '', '', 'CULTURE INDICATED', '', spark)


def test_cfu_range(spark):
    """
    Check 10000-50000 CFU/mL
    """
    eval_test('10000-50000 CFU/mL', '', '', '', '10000-50000 CFU/mL', spark)


def test_equivocal_range(spark):
    """
    Check `0.90-1.10      Equivocal`
    """
    eval_test('0.90-1.10      Equivocal', '', '', '', '0.90-1.10 Equivocal', spark)

@pytest.mark.skip("numeric before alpha not passthru")
def test_do_not_report_7(spark):
    """
    Check `7/DNR`
    """
    eval_test('7/DNR', '', '', 'DO NOT REPORT', '', spark)


def test_gt_slash_numeric_units(spark):
    """
    Check `> 10/20/30 units`
    """
    eval_test('> 10/20/30 units', '>', '', 'units', '10/20/30', spark)


def test_gt_slash_numeric(spark):
    """
    Check `<905/10231/7600/899/867/6448`
    """
    eval_test('<905/10231/7600/899/867/6448', '<', '', '', '905/10231/7600/899/867/6448', spark)


def test_whitespace_normalization(spark):
    eval_test('Space Test', '', '', '', 'Space Test', spark)
    eval_test('Space\tTest', '', '', '', 'Space Test', spark)
    eval_test('Space Test\n ', '', '', '', 'Space Test', spark)
    eval_test('Space     Test', '', '', '', 'Space Test', spark)
    eval_test('     Space     Test', '', '', '', 'Space Test', spark)
    eval_test('Space     Test', '', '', '', 'Space Test', spark)
    eval_test('  Space\t       Test \t ', '', '', '', 'Space Test', spark)
