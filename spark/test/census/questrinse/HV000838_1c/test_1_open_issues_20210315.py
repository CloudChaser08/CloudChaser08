from spark.test.census.questrinse.HV000838_1c.questrinse_helper import eval_test


def test_02(spark):
    """
    8
    """
    eval_test('None Detected', '', '', 'NOT DETECTED', '', spark)


def test_03(spark):
    """
    22
    """
    eval_test('<1:5000             <1:270', '', '', '', '<1:5000             <1:270', spark)


def test_04(spark):
    """
    22
    """
    eval_test(
        '> 0% - < 10%   Positive  / Favorable   >= 10%   Positive  / Unfavorable     0   Negative / Favorable', '',
        '', '',
        '> 0% - < 10%   Positive  / Favorable   >= 10%   Positive  / Unfavorable     0   Negative / Favorable', spark)


def test_05(spark):
    """
    25
    """
    eval_test('>=3 COPIES in SMN2', '>=', '3', 'COPIES in SMN2', '', spark)


def test_06(spark):
    """
    25
    """
    eval_test('>25 EPI, MIXM GNB GPCX2 GPB', '>', '25', 'EPI, MIXM GNB GPCX2 GPB', '', spark)


def test_07(spark):
    """
    2.1
    """
    eval_test('3.0 mL IN EDTA TUB.E', '', '', '', '3.0 mL IN EDTA TUB.E', spark)


def test_08(spark):
    """
    2.1
    """
    eval_test('2 mL in NaHEP', '', '', '', '2 mL in NaHEP', spark)


def test_09(spark):
    """
    2.3
    """
    eval_test('0.7 x 0.4 x 0.2 cm, beige/pink, stringy tissue in RPMI', '', '', '',
              '0.7 x 0.4 x 0.2 cm, beige/pink, stringy tissue in RPMI', spark)


def test_10(spark):
    """
    3.1
    """
    eval_test('TNP/632', '', '', 'TEST NOT PERFORMED', '', spark)


def test_11(spark):
    """
    3.1
    """
    eval_test('TNP/564', '', '', 'TEST NOT PERFORMED', '', spark)


def test_12(spark):
    """
    3.1
    """
    eval_test('TNP/521', '', '', 'TEST NOT PERFORMED', '', spark)


def test_13(spark):
    """
    3.1
    """
    eval_test('TNP/379', '', '', 'TEST NOT PERFORMED', '', spark)


def test_14(spark):
    """
    3.2
    """
    # See test_zero_leading_trailing_remain
    # eval_test('< 1.30 NOT DETECTED', '<', '1.3', 'NOT DETECTED', '', spark)
    eval_test('< 1.30 NOT DETECTED', '<', '1.30', 'NOT DETECTED', '', spark)


def test_15(spark):
    """
    3.2
    """
    eval_test('<15 DETECTED', '<', '15', 'DETECTED', '', spark)


def test_16(spark):
    """
    3.2
    """
    # See test_zero_leading_trailing_remain
    # eval_test('<1.0 NEG', '<', '1', 'NEGATIVE', '', spark)
    eval_test('<1.0 NEG', '<', '1.0', 'NEGATIVE', '', spark)


def test_17(spark):
    """
    3.3
    """
    eval_test('<1 IN 5000 (PSEUDORISK)', '<', '1 IN 5000', 'PSEUDORISK', '', spark)


def test_18(spark):
    """
    3.4
    """
    eval_test('1 IN 462', '', '', '', '1 IN 462', spark)


def test_19(spark):
    """
    3.4
    """
    eval_test('1 IN 4958', '', '', '', '1 IN 4958', spark)


def test_20(spark):
    """
    3.4
    """
    eval_test('1 IN 1352', '', '', '', '1 IN 1352', spark)


def test_21(spark):
    """
    3.4
    """
    eval_test('.25/4.75', '', '0.25/4.75', '', '', spark)
    # Both sides of a fraction need the leading zero in a decimal
    eval_test('.25/.75', '', '0.25/0.75', '', '', spark)
