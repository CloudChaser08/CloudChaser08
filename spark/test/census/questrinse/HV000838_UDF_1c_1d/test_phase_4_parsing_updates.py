"""
Phase 4 Quest RINSE test conditions

Derived tests from:
https://healthverity.atlassian.net/browse/DE-592

eval_test(result_value, expect_op, expect_num, expect_alpha, expect_passthru, spark)
"""
import pytest
pytest.register_assert_rewrite("spark.test.census.questrinse.HV000838_1c.questrinse_helper")

from spark.test.census.questrinse.HV000838_1c.questrinse_helper import eval_test


def test_tnp_prefix_chars(spark):
    """
    Criteria: 'Starts with TNP (1st 3 characters)'
    Alpha only: "TEST NOT PERFORMED"
    """
    eval_test('<=1 TNP is a test', '<=', '1', 'TEST NOT PERFORMED', '', spark)
    eval_test('<=2 TNPPNT', '<=', '2', 'TEST NOT PERFORMED', '', spark)
    eval_test('<=3  TNP ', '<=', '3', 'TEST NOT PERFORMED', '', spark)
    eval_test('<=4/5 TNP', '<=', '4/5', 'TEST NOT PERFORMED', '', spark)
    eval_test('> 10 TNP is a test', '>', '10', 'TEST NOT PERFORMED', '', spark)
    eval_test('< 1000 TNP is a test', '<', '1000', 'TEST NOT PERFORMED', '', spark)
    eval_test('> 1.0 TNP test', '>', '1.0', 'TEST NOT PERFORMED', '', spark)


def test_dnr_prefix_chars(spark):
    """
    Starts with DNR (1st 3 character)
    Alpha only: "DO NOT REPORT"
    """
    eval_test('DNR', '', '', 'DO NOT REPORT', '', spark)
    eval_test('<=1 DNR', '<=', '1', 'DO NOT REPORT', '', spark)
    eval_test('<=2 DNRRDN', '<=', '2', 'DO NOT REPORT', '', spark)
    eval_test('<=3 DNR ', '<=', '3', 'DO NOT REPORT', '', spark)
    eval_test('<=4/5 DNR', '<=', '4/5', 'DO NOT REPORT', '', spark)
    eval_test('> 10 DNR is a test', '>', '10', 'DO NOT REPORT', '', spark)
    eval_test('< 1000 DNR is a test', '<', '1000', 'DO NOT REPORT', '', spark)
    eval_test('> 1.0 DNR test', '>', '1.0', 'DO NOT REPORT', '', spark)


def test_tnp_text_throughout(spark):
    """
    Criteria: "test not performed -> Anywhere in field, lower case or mixed case"
    Alpha only: "TEST NOT PERFORMED"
    """
    eval_test('testing the test not performed', '', '', 'TEST NOT PERFORMED', '', spark)
    eval_test('Test Not Performed', '', '', 'TEST NOT PERFORMED', '', spark)
    eval_test(' Test  Not  Performed', '', '', 'TEST NOT PERFORMED', '', spark)
    eval_test('TEST NOT PERFORMED', '', '', 'TEST NOT PERFORMED', '', spark)
    eval_test('<=1 testing the test not performed', '<=', '1', 'TEST NOT PERFORMED', '', spark)
    eval_test('<=2 test not performed', '<=', '2', 'TEST NOT PERFORMED', '', spark)
    eval_test('> 3 the TEST  NOT PERFOrMED test', '>', '3', 'TEST NOT PERFORMED', '', spark)
    eval_test('<=4/4 the test NOT perFORMED test', '<=', '4/4', 'TEST NOT PERFORMED', '', spark)
    eval_test('<=5.0 TEST NOT PERFORMED ', '<=', '5.0', 'TEST NOT PERFORMED', '', spark)


def test_np_text_throughout(spark):
    """
    Criteria: "not performed -> Anywhere in field, lower case or mixed case"
    Alpha only: "TEST NOT PERFORMED"
    """
    eval_test('testing the not performed', '', '', 'TEST NOT PERFORMED', '', spark)
    eval_test('Not Performed', '', '', 'TEST NOT PERFORMED', '', spark)
    eval_test(' Not  Performed ', '', '', 'TEST NOT PERFORMED', '', spark)
    eval_test('NOT PERFORMED', '', '', 'TEST NOT PERFORMED', '', spark)
    eval_test('<=1 testing the not performed', '<=', '1', 'TEST NOT PERFORMED', '', spark)
    eval_test('<=2 not performed', '<=', '2', 'TEST NOT PERFORMED', '', spark)
    eval_test('> 3 the   NOT PERFOrMED test', '>', '3', 'TEST NOT PERFORMED', '', spark)
    eval_test('<=4/4 the  NOT perFORMED test', '<=', '4/4', 'TEST NOT PERFORMED', '', spark)
    eval_test('<=5.0  NOT PERFORMED ', '<=', '5.0', 'TEST NOT PERFORMED', '', spark)


def test_ng_text_throughout(spark):
    """
    Criteria: "not given -> Anywhere in field, lower case or mixed case"
    Alpha only: "NOT GIVEN"
    """
    eval_test('testing the not given', '', '', 'NOT GIVEN', '', spark)
    eval_test('Not Given', '', '', 'NOT GIVEN', '', spark)
    eval_test(' not given ', '', '', 'NOT GIVEN', '', spark)
    eval_test('NOT GIVEN', '', '', 'NOT GIVEN', '', spark)
    eval_test('<=1 testing the not given', '<=', '1', 'NOT GIVEN', '', spark)
    eval_test('<=2 Not Given', '<=', '2', 'NOT GIVEN', '', spark)
    eval_test('> 3 the   NOT GIVEN test', '>', '3', 'NOT GIVEN', '', spark)
    eval_test('<=4/4 the  not not GIVEN test', '<=', '4/4', 'NOT GIVEN', '', spark)
    eval_test('<=5.0  nOt gIVEN ', '<=', '5.0', 'NOT GIVEN', '', spark)


def test_dnr_text_throughout(spark):
    """
    Criteria: "do not report -> Anywhere in field, lower case or mixed case"
    Alpha only: "DO NOT REPORT"
    """
    eval_test('Do Not Report ', '', '', 'DO NOT REPORT', '', spark)
    eval_test('testing the do not report', '', '', 'DO NOT REPORT', '', spark)
    eval_test(' do not report ', '', '', 'DO NOT REPORT', '', spark)
    eval_test('DO NOT REPORT', '', '', 'DO NOT REPORT', '', spark)
    eval_test('<=1 testing the do not report', '<=', '1', 'DO NOT REPORT', '', spark)
    eval_test('<=2 do not report', '<=', '2', 'DO NOT REPORT', '', spark)
    eval_test('> 3 the DO  NOT REPoRT test', '>', '3', 'DO NOT REPORT', '', spark)
    eval_test('<=4/4 the do NOT repORT test', '<=', '4/4', 'DO NOT REPORT', '', spark)
    eval_test('<=5.0 DO NOT REPORT ', '<=', '5.0', 'DO NOT REPORT', '', spark)


def test_nt_only(spark):
    """
    Criteria: "NT (by itself, no other characters)"
    Alpha only: "TEST NOT PERFORMED"
    """
    eval_test('NT', '', '', 'TEST NOT PERFORMED', '', spark)
    eval_test('<=1 NT', '<=', '1', 'TEST NOT PERFORMED', '', spark)
    eval_test('<=2   NT   ', '<=', '2', 'TEST NOT PERFORMED', '', spark)
    eval_test('> 3 NT ', '>', '3', 'TEST NOT PERFORMED', '', spark)
    eval_test('<=4/4 NT', '<=', '4/4', 'TEST NOT PERFORMED', '', spark)
    eval_test('<=5.0  NT', '<=', '5.0', 'TEST NOT PERFORMED', '', spark)


def test_ng_only(spark):
    """
    Criteria: "NG (by itself, no other characters)"
    Alpha only: "NOT GIVEN"
    """
    eval_test('NG', '', '', 'NOT GIVEN', '', spark)
    eval_test('<=1 NG', '<=', '1', 'NOT GIVEN', '', spark)
    eval_test('<=2   NG   ', '<=', '2', 'NOT GIVEN', '', spark)
    eval_test('> 3 NG ', '>', '3', 'NOT GIVEN', '', spark)
    eval_test('<=4/4 NG', '<=', '4/4', 'NOT GIVEN', '', spark)
    eval_test('<=5.0  NG', '<=', '5.0', 'NOT GIVEN', '', spark)


def test_trim_all_numeric_leading_trailing_whitespace(spark):
    eval_test(' .3 / .3 ', '', '0.3/0.3', '', '', spark)
    eval_test(' LESS THAN    10000', '<', '10000', '', '', spark)
    eval_test('<  1.30    NOT DETECTED', '<', '1.30', 'NOT DETECTED', '', spark)
    eval_test('<  1.30   ', '<', '1.30', '', '', spark)

    # Now parsed as numeric per phase 5
    eval_test(' 1   IN    1352  ', '', '1 IN 1352', '', '', spark)
