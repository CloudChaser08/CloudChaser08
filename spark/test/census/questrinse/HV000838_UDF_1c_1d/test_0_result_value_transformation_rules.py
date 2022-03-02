"""
Original Quest RINSE test conditions

eval_test(result_value, expect_op, expect_num, expect_alpha, expect_passthru, spark)
"""
import pytest
pytest.register_assert_rewrite("spark.test.census.questrinse.HV000838_1c.questrinse_helper")

from spark.test.census.questrinse.HV000838_1c.questrinse_helper import eval_test


def test_clean_less_than(spark):
    """
    Check variations of <, <=, LESS THAN
    """
    eval_test('<10000', '<', '10000', '', '', spark)
    eval_test('< 10000', '<', '10000', '', '', spark)
    eval_test('<=10000', '<=', '10000', '', '', spark)
    eval_test('< =10000', '<=', '10000', '', '', spark)
    eval_test('< = 10000', '<=', '10000', '', '', spark)
    eval_test('< OR = 10000', '<=', '10000', '', '', spark)
    eval_test('<OR= 10000', '<=', '10000', '', '', spark)
    eval_test('LESS THAN 10000', '<', '10000', '', '', spark)
    eval_test('LESSTHAN 10000', '<', '10000', '', '', spark)


def test_clean_greater_than(spark):
    """
    Check variations of >, >=, MORE THAN
    """
    eval_test('>10000', '>', '10000', '', '', spark)
    eval_test('> 10000', '>', '10000', '', '', spark)
    eval_test('>=10000', '>=', '10000', '', '', spark)
    eval_test('> =10000', '>=', '10000', '', '', spark)
    eval_test('> = 10000', '>=', '10000', '', '', spark)
    eval_test('> OR = 10000', '>=', '10000', '', '', spark)
    eval_test('>OR= 10000', '>=', '10000', '', '', spark)
    eval_test('MORE THAN 10000', '>', '10000', '', '', spark)
    eval_test('MORETHAN 10000', '>', '10000', '', '', spark)


def test_whole_decimal(spark):
    """
    Entire decimal must be in the numeric HV field
    """
    eval_test('> 1.201', '>', '1.201', '', '', spark)
    # See test_zero_leading_trailing_remain
    # eval_test('> 1.2010', '>', '1.201', '', '', spark)


def test_precede_zero(spark):
    """
    Entire decimal must be in the numeric HV field
    """
    eval_test('> .201', '>', '0.201', '', '', spark)
    # See test_zero_leading_trailing_remain
    # eval_test('> .2010', '>', '0.201', '', '', spark)
    # eval_test('07242014', '', '7242014', '', '', spark)
    # eval_test('007242014', '', '7242014', '', '', spark)
    # eval_test('-02344', '', '-2344', '', '', spark)


def test_ratio(spark):
    """
    Entire ratio must be in the numeric HV field
    """
    eval_test('1:200', '', '1:200', '', '', spark)
    eval_test('> 1:200', '>', '1:200', '', '', spark)


def test_fraction(spark):
    """
    Entire / value must be in the numeric HV field; Remove space before and after slash
    """
    eval_test('1 / 200', '', '1/200', '', '', spark)
    eval_test('1/ 200', '', '1/200', '', '', spark)
    eval_test('1/200', '', '1/200', '', '', spark)


@pytest.mark.skip("No longer the standard per phase 5.")
def test_range_no_operator(spark):
    """
    Entire range must be in numeric HV field, only with operator
    otherwise passthru
    """
    eval_test('1 IN 5000', '', '', '', '1 IN 5000', spark)


def test_range_with_operator(spark):
    """
    Entire range must be in numeric HV field, only with operator
    """
    eval_test('>1 IN 5000', '>', '1 IN 5000', '', '', spark)


def test_scientific_notation(spark):
    """
    Covert scientific notation to a real number
    """

    eval_test('10E3', '', '10000', '', '', spark)
    eval_test('>10E3', '>', '10000', '', '', spark)

    # This is not a valid Scientific Notation.
    eval_test('5265-E1', '', '', '', '5265-E1', spark)

    eval_test('5265E-1', '', '526.5', '', '', spark)


def test_comma(spark):
    """
    Remove commas from numeric HV field
    """
    eval_test('10,000', '', '10000', '', '', spark)
    eval_test('>10,000,000', '>', '10000000', '', '', spark)
    eval_test('< 10,000,000', '<', '10000000', '', '', spark)


def test_alpha_parenthesis(spark):
    """
    Remove () from the Alpha HV field
    """
    eval_test('<10,000 (FOOBARS)', '<', '10000', 'FOOBARS', '', spark)


def test_alpha_tilde(spark):
    """
    Remove ~ from the Alpha HV field
    """
    eval_test('<10,000 ~FOOBARS', '<', '10000', 'FOOBARS', '', spark)


def test_alpha_not_quantified(spark):
    """
    Remove Not Quantified from Alpha HV field
    """
    eval_test('>10,000 NOT QUANTIFIED', '>', '10000', '', '', spark)


def test_alpha_no_case_change(spark):
    """
    Do not change case on Alpha HV field
    """
    eval_test('>=10,000 FooBar', '>=', '10000', 'FooBar', '', spark)
    eval_test('<=10,000 FOOBAR', '<=', '10000', 'FOOBAR', '', spark)


def test_zero_leading_trailing_remain(spark):
    """
    Confirms leading and trailing zeroes do not get stripped off.
    See: https://healthverity.atlassian.net/browse/DE-176?focusedCommentId=18554
    """
    eval_test('> 1.2010', '>', '1.2010', '', '', spark)
    eval_test('> .2010', '>', '0.2010', '', '', spark)
    eval_test('07242014', '', '07242014', '', '', spark)
    eval_test('007242014', '', '007242014', '', '', spark)
    eval_test('-02344', '', '-02344', '', '', spark)


def test_004(spark):
    """

    """
    eval_test('>10000', '>', '10000', '', '', spark)


def test_005(spark):
    """
    Case leave as-is for Alpha
    """
    eval_test('>100,000 CFU/mL', '>', '100000', 'CFU/mL', '', spark)


def test_006(spark):
    """
    Entire decimal must be in the numeric column
    """
    # See test_zero_leading_trailing_remain
    # eval_test('<0.10', '<', '0.1', '', '', spark)
    eval_test('<0.10', '<', '0.10', '', '', spark)


def test_007(spark):
    """
    Entire Ratio must be included in numeric column
    """
    eval_test('>1:128', '>', '1:128', '', '', spark)


def test_008(spark):
    """
    Remove comma from numeric field
    """
    eval_test('>25,000', '>', '25000', '', '', spark)


def test_009(spark):
    """
    Remove () from Alpha column
    """
    eval_test('<1 (RARE SPERM SEEN)', '<', '1', 'RARE SPERM SEEN', '', spark)


def test_010(spark):
    """
    Covert operator words to symbols (e.g.  > OR = to >=, MORE THAN to >, LESS THAN to <)
    """
    eval_test('> OR = 60', '>=', '60', '', '', spark)


def test_011(spark):
    """
    Entire decimal must be in the numeric column
    """
    # See test_zero_leading_trailing_remain
    # eval_test('>4000.00', '>', '4000', '', '', spark)
    eval_test('>4000.00', '>', '4000.00', '', '', spark)


def test_038(spark):
    """
    Covert operator words to symbols (e.g.  > OR = to >=, MORE THAN to >, LESS THAN to <)
    """
    # See test_zero_leading_trailing_remain
    # eval_test('> OR = 0.10', '>=', '0.1', '', '', spark)
    eval_test('> OR = 0.10', '>=', '0.10', '', '', spark)


def test_488(spark):
    """
    Covert operator words to symbols (e.g.  > OR = to >=, MORE THAN to >, LESS THAN to <)
    """
    eval_test('LESS THAN 1:20', '<', '1:20', '', '', spark)


def test_591(spark):
    """
      If decimal value starts with a decimal point, add 0 (zero) before the decimal
    """
    eval_test('<.8', '<', '0.8', '', '', spark)


def test_653(spark):
    """
    Entire Ratio must be included in numeric column
    """
    eval_test('>=1:1280', '>=', '1:1280', '', '', spark)


def test_661(spark):
    """
    Entire Ratio must be included in numeric column
    """
    eval_test('>1:320 H', '>', '1:320', 'H', '', spark)


def test_687(spark):
    """
    Case leave as-is for Alpha
    """
    eval_test('<1.18 NOT DETECTED', '<', '1.18', 'NOT DETECTED', '', spark)


def test_697(spark):
    """
    Case leave as-is for Alpha
    """
    eval_test('<1.18 Not Detected', '<', '1.18', 'NOT DETECTED', '', spark)


@pytest.mark.skip("No longer the standard per phase 5.")
def test_701(spark):
    """
    Case leave as-is for Alpha
    """
    eval_test('DETECTED <500', '<', '500', 'DETECTED', '', spark)


def test_710(spark):
    """
    remove Not Quantified from Alpha Column
    """
    # See test_zero_leading_trailing_remain
    # eval_test('<1.30 Detected, Not Quantified', '<', '1.3', 'DETECTED', '', spark)
    eval_test('<1.30 Detected, Not Quantified', '<', '1.30', 'DETECTED', '', spark)


def test_711(spark):
    """
    remove Not Quantified from Alpha Column
    """
    eval_test('<20 Detected, Not Quantified', '<', '20', 'DETECTED', '', spark)


def test_724(spark):
    """

    """
    eval_test('> 10 YRS', '>', '10', 'YRS', '', spark)


def test_726(spark):
    """
    Remove ~ from Alpha field
    """
    # See test_zero_leading_trailing_remain
    # eval_test('>=32.0~R', '>=', '32', 'R', '', spark)
    eval_test('>=32.0~R', '>=', '32.0', 'R', '', spark)


def test_727(spark):
    """
    Remove ~ from Alpha field
    """
    eval_test('<=0.06~S', '<=', '0.06', 'S', '', spark)


def test_771(spark):
    """
    Remove ~ from Alpha field
    """
    # See test_zero_leading_trailing_remain
    # eval_test('<=4.0~NR', '<=', '4', 'NR', '', spark)
    eval_test('<=4.0~NR', '<=', '4.0', 'NR', '', spark)


def test_804(spark):
    """
     Entire / value must be in the numeric rinsed field, Remove space before and after slash
    """
    eval_test('>= 16 / 304', '>=', '16/304', '', '', spark)


def test_810(spark):
    """
     Entire / value must be in the numeric rinsed field, Remove space before and after slash
    """
    eval_test('<= 1 / 19', '<=', '1/19', '', '', spark)


def test_813(spark):
    """
      If decimal value starts with a decimal point, add 0 (zero) before the decimal
    """
    eval_test('<= .5 / 9.5', '<=', '0.5/9.5', '', '', spark)


def test_818(spark):
    """
    Remove () from Alpha column, Remove comma from numeric field
    """
    eval_test('<1/10,000 (0.01%)', '<', '1/10000', '0.01%', '', spark)


def test_821(spark):
    """
     Entire / value must be in the numeric rinsed field, Remove space before and after slash, Remove ~
    """
    eval_test('<8/4~R', '<', '8/4', 'R', '', spark)


def test_834(spark):
    """

    """
    eval_test('>25 Epithelial cells', '>', '25', 'Epithelial cells', '', spark)


def test_835(spark):
    """
    Remove () from Alpha column
    """
    eval_test('>=32 (R)', '>=', '32', 'R', '', spark)


def test_849(spark):
    """
    Entire range must be in numeric field
    """
    eval_test('>1 IN 10', '>', '1 IN 10', '', '', spark)


def test_851(spark):
    """
    Entire range must be in numeric field; Remove () from Alpha column
    """
    eval_test('<1 IN 5000 (PSEUDORISK)', '<', '1 IN 5000', 'PSEUDORISK', '', spark)


def test_852(spark):
    """
    Covert scientific notation to a real number
    """
    eval_test('>100.0E7', '>', '1000000000', '', '', spark)


@pytest.mark.skip("No longer the standard per phase 5.")
def test_853(spark):
    """
    Remove () from Alpha column
    """
    eval_test('MANY (>15)', '>', '15', 'MANY', '', spark)


def test_898(spark):
    """
    Reference Range; ignore and do not parse
    """
    eval_test('Negative <1:40', '', '', '', 'Negative <1:40', spark)


def test_857(spark):
    """
    Culture; ignore and do not parse
    """
    eval_test('Gram negative bacilli > 100,000 CFU/mL', '', '', '', 'Gram negative bacilli > 100,000 CFU/mL', spark)


def test_858(spark):
    """
    Culture; ignore and do not parse
    """
    eval_test('Escherichia coli > 100,000 CFU/mL', '', '', '', 'Escherichia coli > 100,000 CFU/mL', spark)


def test_859(spark):
    """
    Culture; ignore and do not parse
    """
    eval_test('Pseudomonas aeruginosa > 100,000 CFU/mL', '', '', '', 'Pseudomonas aeruginosa > 100,000 CFU/mL',
              spark)


def test_860(spark):
    """
    Culture; ignore and do not parse
    """
    eval_test('08/27/20 ?ghs,c1>iso', '', '', '', '08/27/20 ?ghs,c1>iso', spark)


def test_861(spark):
    """
    Culture; ignore and do not parse
    """
    eval_test('08/27/20 ?ghs,c2>iso', '', '', '', '08/27/20 ?ghs,c2>iso', spark)


def test_862(spark):
    """
    Culture; ignore and do not parse
    """
    eval_test('08/27/20 beta>same plt', '', '', '', '08/27/20 beta>same plt', spark)


def test_863(spark):
    """
    Culture; ignore and do not parse
    """
    eval_test('08/27/20 ghs,c1>iso', '', '', '', '08/27/20 ghs,c1>iso', spark)


def test_864(spark):
    """
    Culture; ignore and do not parse
    """
    eval_test('08/27/20 ghs,c2>iso', '', '', '', '08/27/20 ghs,c2>iso', spark)


def test_865(spark):
    """
    Culture; ignore and do not parse
    """
    eval_test('08/27/20 gnb,c1. ghs,c1>iso', '', '', '', '08/27/20 gnb,c1. ghs,c1>iso', spark)


def test_866(spark):
    """
    Culture; ignore and do not parse
    """
    eval_test('08/27/20 gnbx2?gpflora,c4>reis', '', '', '', '08/27/20 gnbx2?gpflora,c4>reis', spark)


def test_867(spark):
    """
    Culture; ignore and do not parse
    """
    eval_test('08/27/20 lfx2,c4>iso', '', '', '', '08/27/20 lfx2,c4>iso', spark)


def test_868(spark):
    """
    Culture; ignore and do not parse
    """
    eval_test('08/27/20 mlf,1 cln>iso jic', '', '', '', '08/27/20 mlf,1 cln>iso jic', spark)


def test_869(spark):
    """
    Culture; ignore and do not parse
    """
    eval_test('08/27/20 mnlf,mlf,c4>iso', '', '', '', '08/27/20 mnlf,mlf,c4>iso', spark)


def test_870(spark):
    """
    Culture; ignore and do not parse
    """
    eval_test('08/27/20 nlf,c2>iso', '', '', '', '08/27/20 nlf,c2>iso', spark)


def test_871(spark):
    """
    Culture; ignore and do not parse
    """
    eval_test('08/27/20 ppt ?gnb,c1>iso', '', '', '', '08/27/20 ppt ?gnb,c1>iso', spark)


def test_872(spark):
    """
    Culture; ignore and do not parse
    """
    eval_test('08/27/20 ppt wht,c3>CNA', '', '', '', '08/27/20 ppt wht,c3>CNA', spark)


def test_873(spark):
    """
    Culture; ignore and do not parse
    """
    eval_test('08/28/20 ? gnb 1cn>iso', '', '', '', '08/28/20 ? gnb 1cn>iso', spark)


def test_874(spark):
    """
    Culture; ignore and do not parse
    """
    eval_test('08/28/20 ?ghs,c2>iso for sens', '', '', '', '08/28/20 ?ghs,c2>iso for sens', spark)


def test_875(spark):
    """
    Culture; ignore and do not parse
    """
    eval_test('08/28/20 MIXED?,GNBX2>B/M', '', '', '', '08/28/20 MIXED?,GNBX2>B/M', spark)


def test_876(spark):
    """
    Culture; ignore and do not parse
    """
    eval_test('08/28/20 MIXED?>GNBX2>B/M', '', '', '', '08/28/20 MIXED?>GNBX2>B/M', spark)


def test_877(spark):
    """
    Culture; ignore and do not parse
    """
    eval_test('08/28/20 gn= PS.luteola?,looks like kleb>b/m for maldi', '', '', '',
              '08/28/20 gn= PS.luteola?,looks like kleb>b/m for maldi', spark)


def test_878(spark):
    """
    Culture; ignore and do not parse
    """
    eval_test('08/28/20 lf1, ghs?>cna', '', '', '', '08/28/20 lf1, ghs?>cna', spark)


def test_879(spark):
    """
    Culture; ignore and do not parse
    """
    eval_test('08/28/20 lfx2?>b/m', '', '', '', '08/28/20 lfx2?>b/m', spark)


def test_880(spark):
    """
    Culture; ignore and do not parse
    """
    eval_test('08/28/20 mdro>kb', '', '', '', '08/28/20 mdro>kb', spark)


def test_881(spark):
    """
    Culture; ignore and do not parse
    """
    eval_test('08/28/20 nhsta,c2>iso', '', '', '', '08/28/20 nhsta,c2>iso', spark)


def test_882(spark):
    """
    Culture; ignore and do not parse
    """
    eval_test('08/28/20 nlf,lf,c4>iso', '', '', '', '08/28/20 nlf,lf,c4>iso', spark)


def test_883(spark):
    """
    Culture; ignore and do not parse
    """
    eval_test('08/29/20 GHS ALPHA GNB C3>B/M', '', '', '', '08/29/20 GHS ALPHA GNB C3>B/M', spark)


def test_884(spark):
    """
    Culture; ignore and do not parse
    """
    eval_test('08/29/20 GHS?C3>CNA', '', '', '', '08/29/20 GHS?C3>CNA', spark)


def test_885(spark):
    """
    Culture; ignore and do not parse
    """
    eval_test('08/29/20 LF GHS C2>B/M', '', '', '', '08/29/20 LF GHS C2>B/M', spark)


def test_886(spark):
    """
    Culture; ignore and do not parse
    """
    eval_test('08/29/20 hold cva, nlf>gni', '', '', '', '08/29/20 hold cva, nlf>gni', spark)


def test_887(spark):
    """
    Culture; ignore and do not parse
    """
    eval_test('08/29/20 nhsc2>bap for sens', '', '', '', '08/29/20 nhsc2>bap for sens', spark)


def test_888(spark):
    """
    Culture; ignore and do not parse
    """
    eval_test('09/03/20 sub>yellow', '', '', '', '09/03/20 sub>yellow', spark)


def test_889(spark):
    """
    Culture; ignore and do not parse
    """
    eval_test('09/04/20 sub>maldi', '', '', '', '09/04/20 sub>maldi', spark)


def test_890(spark):
    """
    Culture; ignore and do not parse
    """
    eval_test('Enterococcus species > 100,000 CFU/mL', '', '', '', 'Enterococcus species > 100,000 CFU/mL', spark)


def test_891(spark):
    """
    Culture; ignore and do not parse
    """
    eval_test('Gram positive cocci > 100,000 CFU/mL', '', '', '', 'Gram positive cocci > 100,000 CFU/mL', spark)


def test_892(spark):
    """
    Culture; ignore and do not parse
    """
    eval_test('Gram negative bacilli < 10,000 CFU/mL', '', '', '', 'Gram negative bacilli < 10,000 CFU/mL', spark)


def test_893(spark):
    """
    Culture; ignore and do not parse
    """
    eval_test('Non-hemolytic Streptococcus < 10,000 CFU/mL', '', '', '',
              'Non-hemolytic Streptococcus < 10,000 CFU/mL', spark)


def test_894(spark):
    """
    Culture; ignore and do not parse
    """
    eval_test('Enterococcus species < 10,000 CFU/mL', '', '', '', 'Enterococcus species < 10,000 CFU/mL', spark)


def test_895(spark):
    """
    Culture; ignore and do not parse
    """
    eval_test('Pseudomonas aeruginosa < 10,000 CFU/mL', '', '', '', 'Pseudomonas aeruginosa < 10,000 CFU/mL', spark)


def test_896(spark):
    """
    Culture; ignore and do not parse
    """
    eval_test('Staphylococcus species > 100,000 CFU/mL', '', '', '', 'Staphylococcus species > 100,000 CFU/mL',
              spark)


def test_897(spark):
    """
    Culture; ignore and do not parse
    """
    eval_test('Non-hemolytic Streptococcus > 100,000 CFU/mL', '', '', '',
              'Non-hemolytic Streptococcus > 100,000 CFU/mL', spark)


def test_899(spark):
    """
    Reference Range; ignore and do not parse
    """
    eval_test('0 Negative 1-10 Low Positive >10 Positive', '', '', '',
              '0 Negative 1-10 Low Positive >10 Positive', spark)


def test_900(spark):
    """
    Reference Range; ignore and do not parse
    """
    eval_test('>= 1% Positive < 1% Negative', '', '', '', '>= 1% Positive < 1% Negative', spark)


def test_901(spark):
    """
    Reference Range; ignore and do not parse
    """
    eval_test('< 10% Low Proliferation >= 10% or <= 20% Intermediate Proliferation > 20% High Proliferation',
              '', '', '',
              '< 10% Low Proliferation >= 10% or <= 20% Intermediate Proliferation > 20% High Proliferation', spark)


def test_902(spark):
    """
    Reference Range; ignore and do not parse
    """
    eval_test('> 0% and < 10% Negative >= 10% Positive = 0 Negative', '', '', '',
              '> 0% and < 10% Negative >= 10% Positive = 0 Negative', spark)


def test_903(spark):
    """
    Reference Range; ignore and do not parse
    """
    eval_test('>= 1% Positive / Favorable < 1% Negative / Unfavorable', '', '', '',
              '>= 1% Positive / Favorable < 1% Negative / Unfavorable', spark)


def test_904(spark):
    """
    Reference Range; ignore and do not parse
    """
    eval_test('< 10% Low Proliferation >= 10% - =< 20% Intermediate Proliferation > 20% High Proliferation',
              '', '', '',
              '< 10% Low Proliferation >= 10% - =< 20% Intermediate Proliferation > 20% High Proliferation', spark)


def test_905(spark):
    """
    Reference Range; ignore and do not parse
    """
    eval_test('0-10% Negative for Basal-Like Carcinoma Subset > 10% Positive for Basal-Like Carcinoma Subset',
              '', '', '',
              '0-10% Negative for Basal-Like Carcinoma Subset > 10% Positive for Basal-Like Carcinoma Subset',
              spark)


def test_906(spark):
    """
    Reference Range; ignore and do not parse
    """
    eval_test('> 0% - < 10% Positive / Favorable >= 10% Positive / Unfavorable 0 Negative / Favorable', '', '',
              '', '> 0% - < 10% Positive / Favorable >= 10% Positive / Unfavorable 0 Negative / Favorable', spark)


def test_907(spark):
    """
    Reference Range; ignore and do not parse
    """
    eval_test('>= 1% Positive < 1% Negative', '', '', '', '>= 1% Positive < 1% Negative', spark)


def test_908(spark):
    """
    Reference Range; ignore and do not parse
    """
    eval_test('< 10% Low Proliferation >= 10% or <= 20% Intermediate Proliferation > 20% High Proliferation',
              '', '', '',
              '< 10% Low Proliferation >= 10% or <= 20% Intermediate Proliferation > 20% High Proliferation', spark)


def test_909(spark):
    """
    Reference Range; ignore and do not parse
    """
    eval_test('> 0% and < 10% Negative >= 10% Positive = 0 Negative', '', '', '',
              '> 0% and < 10% Negative >= 10% Positive = 0 Negative', spark)


def test_910(spark):
    """
    Reference Range; ignore and do not parse
    """
    eval_test('>= 1% Positive / Favorable < 1% Negative / Unfavorable', '', '', '',
              '>= 1% Positive / Favorable < 1% Negative / Unfavorable', spark)


def test_911(spark):
    """
    Reference Range; ignore and do not parse
    """
    eval_test('< 10% Low Proliferation >= 10% - =< 20% Intermediate Proliferation > 20% High Proliferation',
              '', '', '',
              '< 10% Low Proliferation >= 10% - =< 20% Intermediate Proliferation > 20% High Proliferation', spark)


def test_912(spark):
    """
    Reference Range; ignore and do not parse
    """
    eval_test('> 0% - < 10% Positive / Favorable >= 10% Positive / Unfavorable 0 Negative / Favorable', '', '',
              '', '> 0% - < 10% Positive / Favorable >= 10% Positive / Unfavorable 0 Negative / Favorable', spark)


def test_913(spark):
    """
    Gentic; ignore and do not parse
    """
    eval_test('-1639G>A G/A', '', '', '', '-1639G>A G/A', spark)


def test_914(spark):
    """
    Gentic; ignore and do not parse
    """
    eval_test('1236T>C C/C', '', '', '', '1236T>C C/C', spark)


def test_915(spark):
    """
    Gentic; ignore and do not parse
    """
    eval_test('2677G>T G/G', '', '', '', '2677G>T G/G', spark)


def test_916(spark):
    """
    Gentic; ignore and do not parse
    """
    eval_test('3435C>T C/T', '', '', '', '3435C>T C/T', spark)


def test_917(spark):
    """
    Gentic; ignore and do not parse
    """
    eval_test('521T>C T/T', '', '', '', '521T>C T/T', spark)


def test_918(spark):
    """
    Gentic; ignore and do not parse
    """
    eval_test('c.1297G>A G/G', '', '', '', 'c.1297G>A G/G', spark)


def test_919(spark):
    """
    Gentic; ignore and do not parse
    """
    eval_test('c.191G>A G/G', '', '', '', 'c.191G>A G/G', spark)


def test_920(spark):
    """
    Gentic; ignore and do not parse
    """
    eval_test('c.341T>C T/C', '', '', '', 'c.341T>C T/C', spark)


def test_921(spark):
    """
    Gentic; ignore and do not parse
    """
    eval_test('c.364G>A G/G', '', '', '', 'c.364G>A G/G', spark)


def test_922(spark):
    """
    Gentic; ignore and do not parse
    """
    eval_test('c.3730G>A G/A', '', '', '', 'c.3730G>A G/A', spark)


def test_923(spark):
    """
    Gentic; ignore and do not parse
    """
    eval_test('c.590G>A G/A', '', '', '', 'c.590G>A G/A', spark)


def test_924(spark):
    """
    Gentic; ignore and do not parse
    """
    eval_test('c.63-2604G>A A/A', '', '', '', 'c.63-2604G>A A/A', spark)


def test_925(spark):
    """
    Gentic; ignore and do not parse
    """
    eval_test('c.857G>A G/G', '', '', '', 'c.857G>A G/G', spark)


def test_926(spark):
    """
    Various; Ignore -more investigation needed for these types of result values
    """
    eval_test('Detected c.181C>A (p.Q61K) Tier 1A', '', '', '', 'Detected c.181C>A (p.Q61K) Tier 1A', spark)


def test_927(spark):
    """
    Various; Ignore -more investigation needed for these types of result values
    """
    eval_test(
        'EQUIVOCAL Weak to moderate complete membrane staining observed in >10% of tumor cells. Normal breast', '',
        '', '',
        'EQUIVOCAL Weak to moderate complete membrane staining observed in >10% of tumor cells. Normal breast',
        spark)


def test_928(spark):
    """
    Various; Ignore -more investigation needed for these types of result values
    """
    eval_test('gnbx2>reiso 08/28/20', '', '', '', 'gnbx2>reiso 08/28/20', spark)


def test_929(spark):
    """
    Various; Ignore -more investigation needed for these types of result values
    """
    eval_test('91935 >2; Verify Patient Age', '', '', '', '91935 >2; Verify Patient Age', spark)


def test_930(spark):
    """
    Various; Ignore -more investigation needed for these types of result values
    """
    eval_test('beta?>bap 08/28/20', '', '', '', 'beta?>bap 08/28/20', spark)


def test_931(spark):
    """
    Various; Ignore -more investigation needed for these types of result values
    """
    eval_test('08/28/20 beta>bap', '', '', '', '08/28/20 beta>bap', spark)


def test_932(spark):
    """
    Various; Ignore -more investigation needed for these types of result values
    """
    eval_test('GFR not calculated in ages less than 18 years.', '', '', '',
              'GFR not calculated in ages less than 18 years.', spark)


def test_933(spark):
    """
    Various; Ignore -more investigation needed for these types of result values
    """
    eval_test('19955 Patient <4', '', '', '', '19955 Patient <4', spark)


def test_934(spark):
    """
    Various; Ignore -more investigation needed for these types of result values
    """
    eval_test('36336 Patient <6', '', '', '', '36336 Patient <6', spark)


def test_935(spark):
    """
    Various; Ignore -more investigation needed for these types of result values
    """
    eval_test('Cefepime MIC <=1 Sensitive', '', '', '', 'Cefepime MIC <=1 Sensitive', spark)


def test_936(spark):
    """
    Various; Ignore -more investigation needed for these types of result values
    """
    eval_test('Not provided: (CAP/ASCO guidelines recommends less than 1 hour)', '', '', '',
              'Not provided: (CAP/ASCO guidelines recommends less than 1 hour)', spark)


def test_937(spark):
    """
    Various; Ignore -more investigation needed for these types of result values
    """
    eval_test('An aggregate of tan mucus is 0.4 x 0.3 x less than 0.1 cm. 1ns', '', '', '',
              'An aggregate of tan mucus is 0.4 x 0.3 x less than 0.1 cm. 1ns', spark)


def test_938(spark):
    """
    Various; Ignore -more investigation needed for these types of result values
    """
    eval_test('MULTIPLE ORGANISMS PRESENT, EACH <10,000 CFU/ML.', '', '', '',
              'MULTIPLE ORGANISMS PRESENT, EACH <10,000 CFU/ML.', spark)


def test_939(spark):
    """
    Various; Ignore -more investigation needed for these types of result values
    """
    eval_test('Not given (CAP/ASCO guidelines recommend less than 1 hour)', '', '', '',
              'Not given (CAP/ASCO guidelines recommend less than 1 hour)', spark)


def test_940(spark):
    """
    Various; Ignore -more investigation needed for these types of result values
    """
    eval_test('PULLED NOT ENOUGH LESS THAN .1ML', '', '', '', 'PULLED NOT ENOUGH LESS THAN .1ML', spark)


def test_941(spark):
    """
    Various; Ignore -more investigation needed for these types of result values
    """
    eval_test('>bap 08/29/20', '', '', '', '>bap 08/29/20', spark)


def test_942(spark):
    """
    Various; Ignore -more investigation needed for these types of result values
    """
    eval_test('>br/ch 08/27/20', '', '', '', '>br/ch 08/27/20', spark)


def test_943(spark):
    """
    Various; Ignore -more investigation needed for these types of result values
    """
    eval_test('ALL PLTS>BUMCP -31 BAG 7653', '', '', '', 'ALL PLTS>BUMCP -31 BAG 7653', spark)


def test_944(spark):
    """
    Various; Ignore -more investigation needed for these types of result values
    """
    eval_test('CORRECTED ON 09/02/2020 at 1427: PREVIOUSLY RESULTED AS >=64 R', '', '', '',
              'CORRECTED ON 09/02/2020 at 1427: PREVIOUSLY RESULTED AS >=64 R', spark)


def test_945(spark):
    """
    Various; Ignore -more investigation needed for these types of result values
    """
    eval_test('CVA HOLD, NLF>GNI-1', '', '', '', 'CVA HOLD, NLF>GNI-1', spark)


def test_946(spark):
    """
    Various; Ignore -more investigation needed for these types of result values
    """
    eval_test('LF,H2S>SUB', '', '', '', 'LF,H2S>SUB', spark)


def test_947(spark):
    """
    Various; Ignore -more investigation needed for these types of result values
    """
    eval_test('LF,NLF >GNI-1', '', '', '', 'LF,NLF >GNI-1', spark)


def test_948(spark):
    """
    Various; Ignore -more investigation needed for these types of result values
    """
    eval_test('LF,NLF>GNI-1', '', '', '', 'LF,NLF>GNI-1', spark)


def test_949(spark):
    """
    Various; Ignore -more investigation needed for these types of result values
    """
    eval_test('Sensitive >12 on BP or >15 on MH', '', '', '', 'Sensitive >12 on BP or >15 on MH', spark)


def test_950(spark):
    """
    Various; Ignore -more investigation needed for these types of result values
    """
    eval_test('bap=c4,mac=c1>reset JIC 08/28/', '', '', '', 'bap=c4,mac=c1>reset JIC 08/28/', spark)


def test_951(spark):
    """
    Various; Ignore -more investigation needed for these types of result values
    """
    eval_test('beta,c2>cna', '', '', '', 'beta,c2>cna', spark)


def test_952(spark):
    """
    Various; Ignore -more investigation needed for these types of result values
    """
    eval_test('beta,c3>cna 08/27/20', '', '', '', 'beta,c3>cna 08/27/20', spark)


def test_953(spark):
    """
    Various; Ignore -more investigation needed for these types of result values
    """
    eval_test('beta? c1>reis 08/27/20', '', '', '', 'beta? c1>reis 08/27/20', spark)


def test_954(spark):
    """
    Various; Ignore -more investigation needed for these types of result values
    """
    eval_test('beta?>cna 08/28/20', '', '', '', 'beta?>cna 08/28/20', spark)


def test_955(spark):
    """
    Various; Ignore -more investigation needed for these types of result values
    """
    eval_test('beta?>same 08/28/20', '', '', '', 'beta?>same 08/28/20', spark)


def test_956(spark):
    """
    Various; Ignore -more investigation needed for these types of result values
    """
    eval_test('beta?>same plt 08/28/20', '', '', '', 'beta?>same plt 08/28/20', spark)


def test_957(spark):
    """
    Various; Ignore -more investigation needed for these types of result values
    """
    eval_test('c2ppt alp, beta>bap 08/27/20', '', '', '', 'c2ppt alp, beta>bap 08/27/20', spark)


def test_958(spark):
    """
    Various; Ignore -more investigation needed for these types of result values
    """
    eval_test('dry,alp,beta?>same 08/28/20', '', '', '', 'dry,alp,beta?>same 08/28/20', spark)


def test_959(spark):
    """
    Various; Ignore -more investigation needed for these types of result values
    """
    eval_test('fail>gni 08/27/20', '', '', '', 'fail>gni 08/27/20', spark)


def test_960(spark):
    """
    Various; Ignore -more investigation needed for these types of result values
    """
    eval_test('fail>gpi 08/27/20', '', '', '', 'fail>gpi 08/27/20', spark)


def test_961(spark):
    """
    Various; Ignore -more investigation needed for these types of result values
    """
    eval_test('g.96405502G>A G/G', '', '', '', 'g.96405502G>A G/G', spark)


def test_962(spark):
    """
    Various; Ignore -more investigation needed for these types of result values
    """
    eval_test('ghs1>bap 08/28/20', '', '', '', 'ghs1>bap 08/28/20', spark)


def test_963(spark):
    """
    Various; Ignore -more investigation needed for these types of result values
    """
    eval_test('ghsx2?>reiso 08/28/20', '', '', '', 'ghsx2?>reiso 08/28/20', spark)


def test_964(spark):
    """
    Various; Ignore -more investigation needed for these types of result values
    """
    eval_test('gnb,c1,hsta,c1>reis both 08/27', '', '', '', 'gnb,c1,hsta,c1>reis both 08/27', spark)


def test_965(spark):
    """
    Various; Ignore -more investigation needed for these types of result values
    """
    eval_test('gnb,c4,gpc,c3?>sub cna 08/27/2', '', '', '', 'gnb,c4,gpc,c3?>sub cna 08/27/2', spark)


def test_966(spark):
    """
    Various; Ignore -more investigation needed for these types of result values
    """
    eval_test('gnbx2,ghs; hsta?>bap 08/28', '', '', '', 'gnbx2,ghs; hsta?>bap 08/28', spark)


def test_967(spark):
    """
    Various; Ignore -more investigation needed for these types of result values
    """
    eval_test('gnbx2>bm 08/29/20', '', '', '', 'gnbx2>bm 08/29/20', spark)


def test_968(spark):
    """
    Various; Ignore -more investigation needed for these types of result values
    """
    eval_test('gnbx2?>bm 08/29/20', '', '', '', 'gnbx2?>bm 08/29/20', spark)


def test_969(spark):
    """
    Various; Ignore -more investigation needed for these types of result values
    """
    eval_test('gry1,beta?>bap 08/28/20', '', '', '', 'gry1,beta?>bap 08/28/20', spark)


def test_970(spark):
    """
    Various; Ignore -more investigation needed for these types of result values
    """
    eval_test('lf,c1,nlf,c2>reis 08/27/20', '', '', '', 'lf,c1,nlf,c2>reis 08/27/20', spark)


def test_971(spark):
    """
    Various; Ignore -more investigation needed for these types of result values
    """
    eval_test('lf,c4,mlf,c3>reis 08/27/20', '', '', '', 'lf,c4,mlf,c3>reis 08/27/20', spark)


def test_972(spark):
    """
    Various; Ignore -more investigation needed for these types of result values
    """
    eval_test('lf,nlf>reis, h2s>reis', '', '', '', 'lf,nlf>reis, h2s>reis', spark)


def test_973(spark):
    """
    Various; Ignore -more investigation needed for these types of result values
    """
    eval_test('lf,nlfx2>reiso', '', '', '', 'lf,nlfx2>reiso', spark)


def test_974(spark):
    """
    Various; Ignore -more investigation needed for these types of result values
    """
    eval_test('lf,tinynlf?>reiso 08/27/20', '', '', '', 'lf,tinynlf?>reiso 08/27/20', spark)


def test_975(spark):
    """
    Various; Ignore -more investigation needed for these types of result values
    """
    eval_test('lkv>maldi 08/28/20', '', '', '', 'lkv>maldi 08/28/20', spark)


def test_976(spark):
    """
    Various; Ignore -more investigation needed for these types of result values
    """
    eval_test('nhs,mxm,alp,tan>choc 08/30/20', '', '', '', 'nhs,mxm,alp,tan>choc 08/30/20', spark)


def test_977(spark):
    """
    Various; Ignore -more investigation needed for these types of result values
    """
    eval_test('nlfx2>reis 08/27/20', '', '', '', 'nlfx2>reis 08/27/20', spark)


def test_978(spark):
    """
    Various; Ignore -more investigation needed for these types of result values
    """
    eval_test('nlfx2?>reis', '', '', '', 'nlfx2?>reis', spark)


def test_979(spark):
    """
    Various; Ignore -more investigation needed for these types of result values
    """
    eval_test('nlfx2?>reiso x2', '', '', '', 'nlfx2?>reiso x2', spark)


def test_980(spark):
    """
    Various; Ignore -more investigation needed for these types of result values
    """
    eval_test('ppt,c3=gpc>reinc 08/29/20', '', '', '', 'ppt,c3=gpc>reinc 08/29/20', spark)


def test_981(spark):
    """
    Various; Ignore -more investigation needed for these types of result values
    """
    eval_test('r1 hsta,hg;?bhs>bap 08/29/20', '', '', '', 'r1 hsta,hg;?bhs>bap 08/29/20', spark)


def test_982(spark):
    """
    Various; Ignore -more investigation needed for these types of result values
    """
    eval_test('sta>bap 08/28/20', '', '', '', 'sta>bap 08/28/20', spark)


def test_983(spark):
    """
    Various; Ignore -more investigation needed for these types of result values
    """
    eval_test('sub thio>b/ch JIC 08/31/20', '', '', '', 'sub thio>b/ch JIC 08/31/20', spark)


def test_984(spark):
    """
    Various; Ignore -more investigation needed for these types of result values
    """
    eval_test('wht,c2,alpx2,c4>reis 08/27/20', '', '', '', 'wht,c2,alpx2,c4>reis 08/27/20', spark)
