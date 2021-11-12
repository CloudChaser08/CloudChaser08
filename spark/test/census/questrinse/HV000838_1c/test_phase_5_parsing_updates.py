"""
Phase 5 Quest RINSE test conditions

Derived tests from:
https://healthverity.atlassian.net/browse/DE-634
and documentation:
https://docs.google.com/spreadsheets/d/1m0LWGbb4vmxcv0DiTEuaL1jlptgs1iecY5MUbCr9UUI/edit#gid=1747383290

eval_test(result_value, expect_op, expect_num, expect_alpha, expect_passthru, spark)
"""
import pytest
pytest.register_assert_rewrite("spark.test.census.questrinse.HV000838_1c.questrinse_helper")
from spark.test.census.questrinse.HV000838_1c.questrinse_helper import eval_test


# Regression tests were passing before Phase 5
def test_regression_pure_numeric(spark):
    # pure numeric, includes negative numbers and decimal points only
    eval_test('1000', '', '1000', '', '', spark)

    eval_test('<=0.5     mcg/mL  Susceptible', '<=', '0.5', 'mcg/mL Susceptible', '', spark)

    eval_test('<4~S', '<', '4', 'S', '', spark)

    eval_test('>10000', '>', '10000', '', '', spark)

    eval_test('gnbx2,ghs; hsta?>bap 08/28', '', '', '', 'gnbx2,ghs; hsta?>bap 08/28', spark)

    eval_test('mutation detected', '', '', '', 'mutation detected', spark)

    eval_test('08/28/20 lf1, ghs?>can', '', '', '', '08/28/20 lf1, ghs?>can', spark)

    eval_test('<1.0 NEG', '<', '1.0', 'NEGATIVE', '', spark)

    eval_test('FINAL RSLT: NEGATIVE', '', '', 'NEGATIVE', '', spark)

    eval_test('NEGATIVE', '', '', 'NEGATIVE', '', spark)

    eval_test('NEGATIVE CONFIRMED', '', '', 'NEGATIVE CONFIRMED', '', spark)

    eval_test('>8.0 POS', '>', '8.0', 'POSITIVE', '', spark)

    # Numeric followed by a categorization or abbreviation (POS, NEG, column f) split to
    # numeric_final and alpha_final (after applying standardization);
    # includes negative numbers and decimal points only
    eval_test('1.3 POS', '', '1.3', 'POSITIVE', '', spark)

    eval_test('POSITIVE', '', '', 'POSITIVE', '', spark)

    eval_test('DETECTED', '', '', 'DETECTED', '', spark)

    eval_test('DNRTNP', '', '', 'DO NOT REPORT', '', spark)

    eval_test('DNR', '', '', 'DO NOT REPORT', '', spark)

    # TP - UDF did not parse, not a GOLD alpha STD
    eval_test('Not Reported', '', '', '', 'Not Reported', spark)

    # when matched, keep original form. Line 697 from 'Result_Value_Sample data'
    # tab says it should keep original case
    eval_test('Test Not Performed', '', '', 'TEST NOT PERFORMED', '', spark)

    eval_test('TNP', '', '', 'TEST NOT PERFORMED', '', spark)

    # Pass thru. Multiple operators
    eval_test('> 0% - < 10% Positive / Favorable >= 10% Positive'
              ' / Unfavorable 0 Negative / Favorable',
              '', '', '',
              '> 0% - < 10% Positive / Favorable >= 10% Positive'
              ' / Unfavorable 0 Negative / Favorable', spark)

    # WjV - Should be pass through
    eval_test('-1639G>A G/A', '', '', '', '-1639G>A G/A', spark)

    # WjV - Should be pass through
    eval_test('1236T>C C/C', '', '', '', '1236T>C C/C', spark)

    # This is good but should UDF convert the Alpha portion or should it be (0.01%)?
    eval_test('<1/10,000 (0.01%)', '<', '1/10000', '0.01%', '', spark)

    # This is pass thru but how is this different than line 39?
    eval_test('Enterococcus species < 10,000 CFU/mL',
              '',
              '',
              '',
              'Enterococcus species < 10,000 CFU/mL',
              spark)

    # Correct Pass thru?
    eval_test('Negative <1:40', '', '', '', 'Negative <1:40', spark)

    eval_test('+2 YRS', '', '', '', '+2 YRS', spark)

    # Great example of exact match vs like match. Should this be a pass thru or try to parse
    # because 7/14 (numeric) and NEG (Gold Alpha)? Rule says exact match only
    eval_test('+7/14 NEG HPV N', '', '', '', '+7/14 NEG HPV N', spark)

    # good. date
    eval_test('12/26/13 NEG', '', '', '', '12/26/13 NEG', spark)

    eval_test('0.5/.25', '', '0.5/0.25', '', '', spark)

    eval_test('31.01/32', '', '31.01/32', '', '', spark)


# Conditions found during validation that had failed, created to prevent regression
# of the NUMBERLIKE regex.
def test_regression_multislash_error(spark):
    eval_test('/457/466/859/861', '', '', '', '/457/466/859/861', spark)
    eval_test('/18/17-11/24MEXICO', '', '', '', '/18/17-11/24MEXICO', spark)
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
    eval_test('/05/2021"', '', '', '', '/05/2021"', spark)


def test_phase_5_should_strip_parens(spark):
    # Pass thru due to alpha after
    eval_test('1 IN 369 - SCREEN POSITIVE (SEE BELOW)',
              '',
              '',
              '',
              '1 IN 369 - SCREEN POSITIVE (SEE BELOW)',
              spark)


def test_phase_5_failures(spark):
    # Should be pass thru. UDF should always at least make sure the numeric field contain a number
    eval_test('ABSCESS-,INGVINAL', '', '', '', 'ABSCESS-,INGVINAL', spark)  # Not Passing


def test_numeric_strip_chars(spark):

    # numeric followed by one of H/L/*/**/HH/LL/A-includes negative numbers and decimal points only.
    # Allowable values (i.e. H, L, A, HH, LL, AA, N) should be stripped
    eval_test('1000 H', '', '1000', '', '', spark)  # Not Passing

    # L is allowable value, strip L
    eval_test('0.1 L', '', '0.1', '', '', spark)    # Not Passing

    # strip *
    eval_test('5000*', '', '5000', '', '', spark)   # Not Passing

    # UDF should strip LL
    eval_test('07262014LL', '', '07262014', '', '', spark)  # Not Passing

    # UDF should strip HH
    eval_test('2 HH', '', '2', '', '', spark)   # Not Passing

    # * should be stripped
    eval_test('11177*', '', '11177', '', '', spark)     # Not Passing


def test_near_miss_gold_alpha(spark):

    # TP - UDF did not parse this as it's not in the Gold Alpha list.
    # Should it? If it were to ignore the '*', are there any other values to ignore?
    # or should DETECTED* be added to Gold Alpha list
    eval_test('DETECTED*', '', '', '', 'DETECTED*', spark)  # Not Passing

    # TP - Not exact match to 'Gold Alpha'. Gold Alpha is TNP/113 or added to Gold Alpha
    eval_test('TNP 113', '', '', '', 'TNP 113', spark)  # Not Passing


def test_phase_5_date_edge_cases(spark):
    # UDF pass thru. How is this different than line 5? UDF should parse
    eval_test('>25 secs seen, no wbc\'s noted 01/05/19',
              '>',
              '25',
              'secs seen, no wbc\'s noted 01/05/19',
              '', spark)    # Not Passing

    # UDF stripped the comma and should it be parsed because of the *? UDF also stripped the comma
    # here but not in other cases like line 53
    eval_test('04/2019,*RTN', '', '', '', '04/2019,*RTN', spark)    # Not Passing

    # how is this different than some that got parsed? Perhaps Date at the end but Dates only
    # matters when validating numbers as numbers at the beginning of string
    eval_test('<10 ghs 03/19/15', '<', '10', 'ghs 03/19/15', '', spark)     # Not Passing

    # This would be parsed. Also line 48 was parsed so why this wasn't?
    eval_test('08/2012 NEGATIVE', '', '08/2012', 'NEGATIVE', '', spark)   # Not Passing


def test_phase_5_numeric_pos_sign(spark):
    # UDF should treat +number as a number
    eval_test('+174.7', '', '174.7', '', '', spark)     # Not Passing


def test_phase_5_addresslike(spark):
    # should be pass thru. UDF issue
    eval_test('252 JOHNNYS LN', '', '', '', '252 JOHNNYS LN', spark)    # Not Passing

    # UDF is parsing when shouldn't. Is '=' considered as an operator? Not a lot of records.
    eval_test('=110 SEGONIA AVE 6', '', '', '', '=110 SEGONIA AVE 6', spark)    # Not Passing


def test_should_be_gold_alpha(spark):
    # darch.labtest_quest_rinse_result_gold_alpha? is this not the Gold Alpha STD table?
    # ^TNP124 should be a match
    eval_test('^TNP124', '', '', 'TEST NOT PERFORMED', '', spark)   # Not Passing


def test_bad_slash_parsing(spark):
    # Is this the case of UDF doing too much or should it try to match as if 10/12 L
    # (allowable alpha)
    eval_test('10/12 Light1,Dark2', '', '', '', '10/12 Light1,Dark2', spark)    # Not Passing

    # Not sure what is expected here. Pass thru?
    eval_test('17.66/', '', '', '', '17.66/', spark)    # Not Passing

    # What is the expected here if we don't enforce exact match and/or a space after allowable
    # alpha, Pass thru?
    eval_test('9/10 AHS1,C4', '', '', '', '9/10 AHS1,C4', spark)    # Not Passing

    # Is there logic to remove/ignore /?
    eval_test('>/=1.2', '', '', '', '>/=1.2', spark)    # Not Passing


def test_numeric_disallowed_values(spark):
    # Should not parse. PM is not an allowable value
    eval_test('11:31 PM', '', '', '', '11:31 PM', spark)    # Not Passing


def test_hashlike_values(spark):
    # Pass thru or parse because A is an allowable value to follow a number
    eval_test('75330A2F943A92C9208951E2819936669609E3AF30C946EE68C4208251B85D15',
              '',
              '',
              '',
              '75330A2F943A92C9208951E2819936669609E3AF30C946EE68C4208251B85D15',
              spark)    # Not Passing

    # Pass thru. And not an exponential field
    eval_test('61e3cf4d29e3503f2a9bb402c3f02f03',
              '',
              '',
              '',
              '61e3cf4d29e3503f2a9bb402c3f02f03',
              spark)    # Not Passing

    # Pass thru. And not an exponential field
    eval_test('5342e85ea...', '', '', '', '5342e85ea...', spark)    # Not Passing


def test_phase_5_numeric_edge_cases(spark):

    # should be pass thru. UDF issue
    eval_test('MANY (>15)', '', '', '', 'MANY (>15)', spark)    # Not Passing

    # UDF is parsing when shouldn't
    eval_test('..BACK MASS', '', '', '', '..BACK MASS', spark)  # Not Passing

    # Pass thru per Sample data
    eval_test('19955 Patient <4', '', '', '', '19955 Patient <4', spark)    # Not Passing

    eval_test('>2+ YRS', '>', '2', '+ YRS', '', spark)  # Not Passing

    # UDF should check that it's a valid ratio
    eval_test('0127:RL00017R', '', '', '', '0127:RL00017R', spark)  # Not Passing

    # Should not parse
    eval_test('90@10:00', '', '', '', '90@10:00', spark)    # Not Passing

    # correct to parse but A should be stripped
    eval_test('9:22A', '', '9:22', '', '', spark)  # Not Passing


def test_passthru_multiple_separators(spark):
    # Should be pass thru
    eval_test('954.560.7207', '', '', '', '954.560.7207', spark)    # Not Passing

    # Is this expected? UDF strips commas first. Should be pass thru
    eval_test('1759,10165,8847,763,5463', '', '', '', '1759,10165,8847,763,5463',
              spark)    # Not Passing

    # UDF kept the first number due to the space after. Should be pass thru
    eval_test('10165,6399 ,8847,763', '', '', '', '10165,6399 ,8847,763', spark)    # Not Passing

    # Is this expected? UDF strips commas first. Should be a pass thru
    eval_test('11363,6447.91431,36126', '', '', '', '11363,6447.91431,36126',
              spark)    # Not Passing

    # should be pass thru. UDF issue
    eval_test('110-120-1340', '', '', '', '110-120-1340', spark)    # Not Passing

    # should be pass thru. UDF issue
    eval_test('7600;899;496;17306;499', '', '', '', '7600;899;496;17306;499',
              spark)    # Not Passing


def test_phase_5_operators(spark):

    # though = is not an operator. I think =19 should still be parsed for the numeric.
    # It should still be parsed as if beginning with a number
    eval_test('=19', '', '19', '', '', spark)   # Not Passing


def test_range_in(spark):
    # UDF should parse. IN Clause
    eval_test('1 IN 4362', '', '1 IN 4362', '', '', spark)  # Not Passing


def test_dash_range(spark):
    # In the case of a dash in a range, if the left hand side < RHS consider valid range

    # How to determine a valid range? Must first number be less than second number?
    # UDF issue. always keeping the first number
    eval_test('20181118144600-0600', '', '', '', '20181118144600-0600', spark)  # Not Passing

    # How should UDF enforce a 'range' or should it?
    eval_test('50398-7', '', '', '', '50398-7', spark)  # Not Passing

    # UDF should treat this as numeric range
    eval_test('34-4776', '', '34-4776', '', '', spark)  # Not Passing
