"""
Phase 5 Quest RINSE test conditions

Derived tests from:
https://healthverity.atlassian.net/browse/DE-634
and documentation:
https://docs.google.com/spreadsheets/d/1m0LWGbb4vmxcv0DiTEuaL1jlptgs1iecY5MUbCr9UUI/edit#gid=1747383290
https://docs.google.com/spreadsheets/d/1RhqXfCBjZF7L09W9vrtiWAT8LBgc3b7oif9HSUFcl2g/edit#gid=1800287098
eval_test(result_value, expect_op, expect_num, expect_alpha, expect_passthru, spark)
"""
import pytest
pytest.register_assert_rewrite("spark.test.census.questrinse.HV000838_1c.questrinse_helper")
from spark.test.census.questrinse.HV000838_1d.questrinse_helper import eval_test


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
    eval_test('ABSCESS-,INGVINAL', '', '', '', 'ABSCESS-,INGVINAL', spark)


def test_numeric_strip_chars(spark):

    # numeric followed by one of H/L/*/**/HH/LL/A-includes negative numbers and decimal points only.
    # Allowable values (i.e. H, L, A, HH, LL, AA, N) should be stripped
    eval_test('1000 H', '', '1000', '', '', spark)

    # L is allowable value, strip L
    eval_test('0.1 L', '', '0.1', '', '', spark)

    # strip *
    eval_test('5000*', '', '5000', '', '', spark)

    # UDF should strip LL
    eval_test('07262014LL', '', '07262014', '', '', spark)

    # UDF should strip HH
    eval_test('2 HH', '', '2', '', '', spark)

    # * should be stripped
    eval_test('11177*', '', '11177', '', '', spark)


def test_near_miss_gold_alpha(spark):

    # TP - UDF did not parse this as it's not in the Gold Alpha list.
    # Should it? If it were to ignore the '*', are there any other values to ignore?
    # or should DETECTED* be added to Gold Alpha list
    eval_test('DETECTED*', '', '', '', 'DETECTED*', spark)

    # TP - Not exact match to 'Gold Alpha'. Gold Alpha is TNP/113 or added to Gold Alpha
    eval_test('TNP 113', '', '', '', 'TNP 113', spark)


def test_phase_5_date_edge_cases(spark):
    # UDF pass thru. How is this different than line 5? UDF should parse
    eval_test('>25 secs seen, no wbc\'s noted 01/05/19',
              '>',
              '25',
              'secs seen, no wbc\'s noted 01/05/19',
              '', spark)

    # UDF stripped the comma and should it be parsed because of the *? UDF also stripped the comma
    # here but not in other cases like line 53
    eval_test('04/2019,*RTN', '', '', '', '04/2019,*RTN', spark)

    # how is this different than some that got parsed? Perhaps Date at the end but Dates only
    # matters when validating numbers as numbers at the beginning of string
    eval_test('<10 ghs 03/19/15', '<', '10', 'ghs 03/19/15', '', spark)

    # This would be parsed. Also line 48 was parsed so why this wasn't?
    eval_test('08/2012 NEGATIVE', '', '08/2012', 'NEGATIVE', '', spark)


def test_phase_5_numeric_pos_sign(spark):
    # UDF should treat +number as a number
    eval_test('+174.7', '', '174.7', '', '', spark)


def test_phase_5_address_like(spark):
    eval_test('252 JOHNNYS LN', '', '', '', '252 JOHNNYS LN', spark)

    # UDF is parsing when shouldn't. Is '=' considered as an operator? Not a lot of records.
    eval_test('=110 SEGONIA AVE 6', '', '', '', '=110 SEGONIA AVE 6', spark)


def test_should_be_gold_alpha(spark):
    # darch.labtest_quest_rinse_result_gold_alpha? is this not the Gold Alpha STD table?
    # ^TNP124 should be a match
    eval_test('^TNP124', '', '', 'TEST NOT PERFORMED', '', spark)


def test_bad_slash_parsing(spark):
    # Is this the case of UDF doing too much or should it try to match as if 10/12 L
    # (allowable alpha)
    eval_test('10/12 Light1,Dark2', '', '', '', '10/12 Light1,Dark2', spark)

    # Not sure what is expected here. Pass thru?
    eval_test('17.66/', '', '', '', '17.66/', spark)

    # What is the expected here if we don't enforce exact match and/or a space after allowable
    # alpha, Pass thru?
    eval_test('9/10 AHS1,C4', '', '', '', '9/10 AHS1,C4', spark)

    # Is there logic to remove/ignore /?
    eval_test('>/=1.2', '', '', '', '>/=1.2', spark)


def test_numeric_disallowed_values(spark):
    # Should not parse. PM is not an allowable value
    eval_test('11:31 PM', '', '', '', '11:31 PM', spark)


def test_hashlike_values(spark):
    # Pass thru 16,32 or 64 character long 0-9a-f strings with at least one a-f
    eval_test('75330A2F943A92C9208951E2819936669609E3AF30C946EE68C4208251B85D15',
              '',
              '',
              '',
              '75330A2F943A92C9208951E2819936669609E3AF30C946EE68C4208251B85D15',
              spark)

    # Pass thru. And not an exponential field
    eval_test('61e3cf4d29e3503f2a9bb402c3f02f03',
              '',
              '',
              '',
              '61e3cf4d29e3503f2a9bb402c3f02f03',
              spark)

    # Pass thru. And not an exponential field
    eval_test('5342e85ea...', '', '', '', '5342e85ea...', spark)


def test_phase_5_numeric_edge_cases(spark):

    # should be pass thru. UDF issue
    eval_test('MANY (>15)', '', '', '', 'MANY (>15)', spark)

    # UDF is parsing when shouldn't
    eval_test('..BACK MASS', '', '', '', '..BACK MASS', spark)

    # Pass thru per Sample data
    eval_test('19955 Patient <4', '', '', '', '19955 Patient <4', spark)

    eval_test('>2+ YRS', '>', '2', '+ YRS', '', spark)

    # UDF should check that it's a valid ratio
    eval_test('0127:RL00017R', '', '', '', '0127:RL00017R', spark)

    # Should not parse
    eval_test('90@10:00', '', '', '', '90@10:00', spark)

    # correct to parse but A should be stripped
    eval_test('9:22A', '', '9:22', '', '', spark)


def test_passthru_multiple_separators(spark):
    # Should be pass thru
    eval_test('954.560.7207', '', '', '', '954.560.7207', spark)
    eval_test('1759,10165,8847,763,5463', '', '', '', '1759,10165,8847,763,5463',
              spark)

    # UDF kept the first number due to the space after. Should be pass thru
    eval_test('10165,6399 ,8847,763', '', '', '', '10165,6399 ,8847,763', spark)

    # Is this expected? UDF strips commas first. Should be a pass thru
    eval_test('11363,6447.91431,36126', '', '', '', '11363,6447.91431,36126',
              spark)

    eval_test('110-120-1340', '', '', '', '110-120-1340', spark)

    # should be pass thru. UDF issue
    eval_test('7600;899;496;17306;499', '', '', '', '7600;899;496;17306;499',
              spark)

    eval_test('2.27.16', '', '', '', '2.27.16', spark)

    eval_test('5.30.17.', '', '', '', '5.30.17.', spark)

    eval_test('6.1.2016', '', '', '', '6.1.2016', spark)

    eval_test('899,457,927,496,17306', '', '', '', '899,457,927,496,17306', spark)

    eval_test('303,457,496,622,17306', '', '', '', '303,457,496,622,17306', spark)

    eval_test('76,58', '', '', '', '76,58', spark)

    eval_test('466,927,17306', '', '', '', '466,927,17306', spark)

    eval_test('\'36399.', '', '', '', '\'36399.', spark)


def test_phase_5_operators(spark):
    # though = is not an operator. I think =19 should still be parsed for the numeric.
    # It should still be parsed as if beginning with a number
    eval_test('=19', '', '19', '', '', spark)


def test_range_in(spark):
    # UDF should parse. IN Clause
    eval_test('1 IN 4362', '', '1 IN 4362', '', '', spark)


def test_dash_range(spark):
    # In the case of a dash in a range, if the left-hand side < RHS consider valid range

    # How to determine a valid range? Must first number be less than second number?
    # UDF issue. always keeping the first number
    eval_test('20181118144600-0600', '', '', '', '20181118144600-0600', spark)

    # How should UDF enforce a 'range' or should it?
    eval_test('50398-7', '', '', '', '50398-7', spark)

    # UDF should treat this as numeric range
    eval_test('34-4776', '', '34-4776', '', '', spark)


# DNR failures
def test_regression_dnr_gold_alpha_fail(spark):
    """
    1/.5~DNR
    1/.5~(DNR)
    1/.5DNR
    """
    eval_test('1/.5~DNR', '', '1/0.5', 'DO NOT REPORT', '', spark)
    eval_test('1/.5DNR', '', '1/0.5', 'DO NOT REPORT', '', spark)
    eval_test('1/.5~(DNR)', '', '1/0.5', 'DO NOT REPORT', '', spark)


def test_regression_plain_number_long(spark):
    """
    Numbers are failing because of the Hashlike length checks, if all numeric, should pass
    """
    eval_test('63991023176005463', '', '63991023176005463', '', '', spark)
    eval_test('1.592592593', '', '1.592592593', '', '', spark)
    eval_test('1.666666666666666667', '', '1.666666666666666667', '', '', spark)


def test_ratio_formatting_no_space(spark):
    """
    Ratios should have no spaces
    """
    eval_test('1: 40600', '', '1:40600', '', '', spark)


def test_regression_milligrams(spark):
    eval_test('<0.2 mg/dL (0.0-1.2)', '<', '0.2', 'mg/dL 0.0-1.2', '', spark)
    eval_test('< 0.1*  mg/dL (0.2-1.5)', '<', '0.1', '* mg/dL 0.2-1.5', '', spark)
    eval_test('< 2.5  L mg/L  (10.0-20.0)', '<', '2.5', 'L mg/L 10.0-20.0', '', spark)
    eval_test('<8.0  MG/L  (5.0-49.0)', '<', '8.0', 'MG/L 5.0-49.0', '', spark)
    eval_test('< 0.2 mg/dL (0.0-1.2)', '<', '0.2', 'mg/dL 0.0-1.2', '', spark)
    eval_test('< 0.1 mg/dL (0.0-1.2)', '<', '0.1', 'mg/dL 0.0-1.2', '', spark)
    eval_test('< 0.2  H mg/dL (0.0-1.2)', '<', '0.2', 'H mg/dL 0.0-1.2', '', spark)
    eval_test('< 8.0 MG/L  (5.0-49.0)', '<', '8.0', 'MG/L 5.0-49.0', '', spark)
    eval_test('< 0.2  L mg/dL (0.0-1.2)', '<', '0.2', 'L mg/dL 0.0-1.2', '', spark)
    eval_test('<0.2  mg/dL (0.0-1.2)', '<', '0.2', 'mg/dL 0.0-1.2', '', spark)
    eval_test('<8.0 MG/L  (5.0-49.0)', '<', '8.0', 'MG/L 5.0-49.0', '', spark)


def test_regression_other_measurements(spark):
    eval_test('<0.603 L uM  (0.6-2.9)', '<', '0.603', 'L uM 0.6-2.9', '', spark)
    eval_test('>22.0 ng/mL (37.20-345.67)', '>', '22.0', 'ng/mL 37.20-345.67', '', spark)
    eval_test('< 0.603  L uM  (0.6-2.9)', '<', '0.603', 'L uM 0.6-2.9', '', spark)
    eval_test('< 1.1 uM  (1.1-2.7)', '<', '1.1', 'uM 1.1-2.7', '', spark)
    eval_test('< 5.5 ng/dL (20.0-80.0)', '<', '5.5', 'ng/dL 20.0-80.0', '', spark)
    eval_test('< 1.059  L uM  (1.1-2.7)', '<', '1.059', 'L uM 1.1-2.7', '', spark)
    eval_test('< 1.1  L uM  (1.1-2.7)', '<', '1.1', 'L uM 1.1-2.7', '', spark)
    eval_test('<0.8 L umol/L  (0.8-9.7)', '<', '0.8', 'L umol/L 0.8-9.7', '', spark)
    eval_test('<1.1  L uM  (1.1-2.7)', '<', '1.1', 'L uM 1.1-2.7', '', spark)
    eval_test('<0.8  L umol/L 0.8-5.8', '<', '0.8', 'L umol/L 0.8-5.8', '', spark)
    eval_test('<0.6  L umol/L  (0.7-1.9)', '<', '0.6', 'L umol/L 0.7-1.9', '', spark)
    eval_test('<1.1  L umol/L  (1.1-2.7)', '<', '1.1', 'L umol/L 1.1-2.7', '', spark)
    eval_test('< 0.839 L uM  (0.8-9.7)', '<', '0.839', 'L uM 0.8-9.7', '', spark)
    eval_test('<1.059  L  uM  (1.1-2.7)', '<', '1.059', 'L uM 1.1-2.7', '', spark)
    eval_test('<1.059  uM  (1.1-2.7)', '<', '1.059', 'uM 1.1-2.7', '', spark)
    eval_test('< 1  L umol/L  (2.2-27.3)', '<', '1', 'L umol/L 2.2-27.3', '', spark)
    eval_test('< 100.0 pg/mL (221.0-3004.0)', '<', '100.0', 'pg/mL 221.0-3004.0', '', spark)
    eval_test('<0.839 L uM  (0.8-9.7)', '<', '0.839', 'L uM 0.8-9.7', '', spark)
    eval_test('Less than 0.5 ml.  H-2375-18', '<', '0.5', 'ml. H-2375-18', '', spark)
    eval_test('<1.1 uM  (1.1-2.7)', '<', '1.1', 'uM 1.1-2.7', '', spark)
    eval_test('Less than 0.5 ml.  S-4161-18', '<', '0.5', 'ml. S-4161-18', '', spark)
    eval_test('< 0.839  L uM  (0.8-9.7)', '<', '0.839', 'L uM 0.8-9.7', '', spark)


def test_regression_24hr_collection(spark):
    eval_test('<24HR COLLECTION TIME FOR 381,11315,113', '<', '24',
              'HR COLLECTION TIME FOR 381,11315,113', '', spark)
    eval_test('<24HR COLLECTION FOR 14962,39627', '<', '24',
              'HR COLLECTION FOR 14962,39627', '', spark)
    eval_test('<24HR COLLECTION TIME FOR 11280,7943', '<', '24',
              'HR COLLECTION TIME FOR 11280,7943', '', spark)
    eval_test('<24HR COLLECTION TIME FOR 11280,7943', '<', '24',
              'HR COLLECTION TIME FOR 11280,7943', '', spark)
    eval_test('>24HR COLLECTION TIME FOR 14962,523', '>', '24',
              'HR COLLECTION TIME FOR 14962,523', '', spark)
    eval_test('<24HR COLLECTION TIME FOR 7943,11320', '<', '24',
              'HR COLLECTION TIME FOR 7943,11320', '', spark)
    eval_test('<24HR COLLECTION FOR 7943,757', '<', '24',
              'HR COLLECTION FOR 7943,757', '', spark)
    eval_test('<24HR COLLECTION TIME FOR 682,11313.', '<', '24',
              'HR COLLECTION TIME FOR 682,11313.', '', spark)
    eval_test('<24HR COLLECTION TIME GIVEN FOR 7943,73', '<', '24',
              'HR COLLECTION TIME GIVEN FOR 7943,73', '', spark)
    eval_test('<24HR COLLECTION TIME FOR 14962,318', '<', '24',
              'HR COLLECTION TIME FOR 14962,318', '', spark)
    eval_test('<24HR COLLECTION TIME FOR 93958,94444', '<', '24',
              'HR COLLECTION TIME FOR 93958,94444', '', spark)
    eval_test('<24HR COLLECTION TIME GIVEN FOR 19118,7', '<', '24',
              'HR COLLECTION TIME GIVEN FOR 19118,7', '', spark)
    eval_test('<24HR COLLECTION TIME FOR 11313,381', '<', '24',
              'HR COLLECTION TIME FOR 11313,381', '', spark)
    eval_test('>24HR COLLECTION GIVEN FOR 7943,11320', '>', '24',
              'HR COLLECTION GIVEN FOR 7943,11320', '', spark)
    eval_test('<24HR COLLECTION TIME FOR 7943,11313', '<', '24',
              'HR COLLECTION TIME FOR 7943,11313', '', spark)
    eval_test('<24HR COLLECTION FOR 10243,94444,93958', '<', '24',
              'HR COLLECTION FOR 10243,94444,93958', '', spark)
    eval_test('>24HR COLLECTION FOR 11320,973,7943', '>', '24',
              'HR COLLECTION FOR 11320,973,7943', '', spark)
    eval_test('<24HR COLLECTION TIME GIVEN 11322,11313', '<', '24',
              'HR COLLECTION TIME GIVEN 11322,11313', '', spark)
    eval_test('>24HR COLLECTION FOR 7943,11320', '>', '24',
              'HR COLLECTION FOR 7943,11320', '', spark)
    eval_test('<24HR COLLECTION TIME FOR 38944,750,757', '<', '24',
              'HR COLLECTION TIME FOR 38944,750,757', '', spark)
    eval_test('<24HR COLLECTION TIME FOR 1635,11280', '<', '24',
              'HR COLLECTION TIME FOR 1635,11280', '', spark)
    eval_test('<24HR COLLECTION TIME FOR 7943,757', '<', '24',
              'HR COLLECTION TIME FOR 7943,757', '', spark)
    eval_test('>25HR COLLECTION TIME FOR 11313,381', '>', '25',
              'HR COLLECTION TIME FOR 11313,381', '', spark)


def test_regression_epi_patterns(spark):
    eval_test('<10secs,nwbc,gnb,gpcx2,gpb, runny cloudy', '<', '10',
              'secs,nwbc,gnb,gpcx2,gpb, runny cloudy', '', spark)
    eval_test('<10epi,gpcx2,gpb,gnb 04/01/21', '<', '10',
              'epi,gpcx2,gpb,gnb 04/01/21', '', spark)
    eval_test('<10 EPI, WBC, MXM CWNAP, GPCX2, GNB', '<', '10',
              'EPI, WBC, MXM CWNAP, GPCX2, GNB', '', spark)
    eval_test('<10 EPI,WBC MXM GPC GPB GNBX2, THICK PURULENT', '<', '10',
              'EPI,WBC MXM GPC GPB GNBX2, THICK PURULENT', '', spark)
    eval_test('<10epi,gpcx2,gnbx2,gpb,yst,mixed morphs', '<', '10',
              'epi,gpcx2,gnbx2,gpb,yst,mixed morphs', '', spark)
    eval_test('<10 EPI,GNB,GPCx2, GPB,YST THICK,YELLOW', '<', '10',
              'EPI,GNB,GPCx2, GPB,YST THICK,YELLOW', '', spark)
    eval_test('<10 EPI, WBC MXM, CWNAP, GPCX2, GNB, GNCB', '<', '10',
              'EPI, WBC MXM, CWNAP, GPCX2, GNB, GNCB', '', spark)
    eval_test('<10 epi, gpcx2,gnb,gpb', '<', '10',
              'epi, gpcx2,gnb,gpb', '', spark)
    eval_test('<10 EPI, WBC,MXM CWNAP, GPC GPBX2,GNBX2 THICK, YELLOW', '<', '10',
              'EPI, WBC,MXM CWNAP, GPC GPBX2,GNBX2 THICK, YELLOW', '', spark)
    eval_test('<10EPI,GNBX2,GPC GPB', '<', '10', 'EPI,GNBX2,GPC GPB', '', spark)
    eval_test('<10 epi, gpcx2,gnb,gpb,gndc', '<', '10', 'epi, gpcx2,gnb,gpb,gndc', '', spark)
    eval_test('<10epi,gpc2,gpb,gnb,no predom org', '<', '10',
              'epi,gpc2,gpb,gnb,no predom org', '', spark)
    eval_test('>10 epis, nwbc,gnc,gpc,gpbx2,gnbx2', '>', '10',
              'epis, nwbc,gnc,gpc,gpbx2,gnbx2', '', spark)
    eval_test('<10epi,cwnap gpcx2,gpb,(rgnb)', '<', '10',
              'epi,cwnap gpcx2,gpb,rgnb', '', spark)
    eval_test('<10 EPI, WBCS, NCRESP, GPCX2,GNB GPB', '<', '10',
              'EPI, WBCS, NCRESP, GPCX2,GNB GPB', '', spark)
    eval_test('<10epi,cwnap gnbx2,gpc,gpb', '<', '10', 'epi,cwnap gnbx2,gpc,gpb', '', spark)
    eval_test('<10 EPI, WBCS, MXM CWNAP GPCX2, GPB, GNB', '<', '10',
              'EPI, WBCS, MXM CWNAP GPCX2, GPB, GNB', '', spark)
    eval_test('>25wbc, no wbc, gpcx2, gpbx2, gnb', '>', '25',
              'wbc, no wbc, gpcx2, gpbx2, gnb', '', spark)
    eval_test('> 25 epi, 0 wbc, gpcx2,gnbx2,gpbx2,gncb, yst', '>', '25',
              'epi, 0 wbc, gpcx2,gnbx2,gpbx2,gncb, yst', '', spark)


def test_regression_pathogen_pattern(spark):
    eval_test('>3 pathogens. nlfx2 c4, mlf c3,lf c3', '>', '3', 'pathogens. nlfx2 c4, mlf c3,lf c3',
              '', spark)
    eval_test('>3 pathogens: EC, Morg, Prov, Prot, gpf. not urol. mix', '>', '3',
              'pathogens: EC, Morg, Prov, Prot, gpf. not urol. mix', '', spark)
    eval_test('>3 pathogens. ghs c4, nlfx2 c4, lf c1', '>', '3',
              'pathogens. ghs c4, nlfx2 c4, lf c1', '', spark)


def test_regression_year_comparisons_no_space(spark):
    eval_test('MORETHAN5YRSAGO', '>', '5', 'YRSAGO', '', spark)
    eval_test('MORETHAN10YRS', '>', '10', 'YRS', '', spark)
    eval_test('MORE THAN10 YRS AGO', '>', '10', 'YRS AGO', '', spark)
    eval_test('MORE THAN7 YEARS AGO', '>', '7', 'YEARS AGO', '', spark)
    eval_test('MORETHAN20-30YR', '>', '20-30', 'YR', '', spark)


def test_regression_comparisons_close_comma(spark):
    eval_test('>25,BLOODY', '>', '25', ',BLOODY', '', spark)
    eval_test('>100col1, 15ppcol2', '>', '100', 'col1, 15ppcol2', '', spark)
    eval_test('<20, MISSE', '<', '20', ', MISSE', '', spark)
    eval_test('>100, reset to confirm 05/08', '>', '100', ', reset to confirm 05/08', '', spark)


def test_regression_cubic_measurments(spark):
    eval_test('Less than 0.1x 0.1 x 0.1 cm', '<', '0.1', 'x 0.1 x 0.1 cm', '', spark)
    eval_test('Less than 0.1 x 0.1 x 0.1 cm cube', '<', '0.1',
              'x 0.1 x 0.1 cm cube', '', spark)
    eval_test('Less than 0.1 x 0.1 x 0.1 cm. S-2770-18A', '<', '0.1',
              'x 0.1 x 0.1 cm. S-2770-18A', '', spark)
    eval_test('Less than 0.1 x 0.1 x 0.1 cm', '<', '0.1', 'x 0.1 x 0.1 cm', '', spark)
    eval_test('Less than 0.1 x 0.1 x 0.1 cm.', '<', '0.1', 'x 0.1 x 0.1 cm.', '', spark)


def test_regression_commma_none_detected(spark):
    eval_test('<1,NONE DETECTED', '<', '1', ',NONE DETECTED', '', spark)
    eval_test('<1,None Detected', '<', '1', ',None Detected', '', spark)
    eval_test('<1, NONE DETECTED', '<', '1', ', NONE DETECTED', '', spark)
    eval_test('<1, NOT DETECTED', '<', '1', ', NOT DETECTED', '', spark)
    eval_test('<1,NONE DETECTED', '<', '1', ',NONE DETECTED', '', spark)
    eval_test('<1, NONE DETECTED', '<', '1', ', NONE DETECTED', '', spark)


def test_regression_phase_5_miscellany(spark):
    eval_test('>100, 3 morphs', '>', '100', ', 3 morphs', '', spark)
    eval_test('>50, no email sent', '>', '50', ', no email sent', '', spark)
    eval_test('Less than 1%. NEGATIVE Internal Control: Benign breast parenchyma is not present.'
              ' PR is repeated with identical negative reactivity.', '<', '1',
              '%. NEGATIVE Internal Control: Benign breast parenchyma is not present. PR is'
              ' repeated with identical negative reactivity.', '', spark)
    eval_test('<=10SEC/LPF >25WBC/LPF MIXED ORGS INTRA-EXTRA CELLULAR 12/18', '', '', '',
              '<=10SEC/LPF >25WBC/LPF MIXED ORGS INTRA-EXTRA CELLULAR 12/18', spark)
    eval_test('> 5 MEDS LISTED FOR TEST 92450,16910', '>', '5',
              'MEDS LISTED FOR TEST 92450,16910', '', spark)
    eval_test('>700 Hand delivered results to MD REDACTED 1/10/2011 2:22 AM by AB to MD REDACTED',
              '>', '700',
              'Hand delivered results to MD REDACTED 1/10/2011 2:22 AM by AB to MD REDACTED',
              '', spark)
    eval_test('>80.0 Hand delivered results to MD REDACTED 1/10/2011 2:22 AM by AB to MD REDACTED',
              '>', '80.0',
              'Hand delivered results to MD REDACTED 1/10/2011 2:22 AM by AB to MD REDACTED',
              '', spark)
    eval_test('> 3600.0  H  (221.0-3004.0)', '>', '3600.0',
              'H 221.0-3004.0', '', spark)
    eval_test('<0.3,AD', '<', '0.3', ',AD', '', spark)
    eval_test('>100, reset r/o contamin 05/09', '>', '100', ', reset r/o contamin 05/09', '', spark)
    eval_test('Less than0.5 cc clear yellow fluid. One ThinPrep slide prepared.', '<', '0.5',
              'cc clear yellow fluid. One ThinPrep slide prepared.', '', spark)
    eval_test('< OR = 0.90,"Sample shows moderate hemolysis which may affect assay performance."',
              '<=', '0.90',
              ',"Sample shows moderate hemolysis which may affect assay performance."', '', spark)
    eval_test('>100smCOL1, 3medCOL2, 2lgCOL3', '>', '100', 'smCOL1, 3medCOL2, 2lgCOL3', '', spark)
    eval_test('>262-412-9352N', '>', '262-412', '-9352N', '', spark)


def test_double_operators_passthrough(spark):
    eval_test('<=10 sec, >25wbc', '', '', '', '<=10 sec, >25wbc', spark)
    eval_test('less than 10% squamous present less than 10% squamous', '', '',
              '', 'less than 10% squamous present less than 10% squamous', spark)
    eval_test('Less than 5 pg/mL (< 41)', '', '', '', 'Less than 5 pg/mL (< 41)', spark)
