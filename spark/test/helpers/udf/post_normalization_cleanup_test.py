import datetime
import spark.helpers.udf.post_normalization_cleanup as cleanup

def test_obscure_inst_type_of_bill():
    assert cleanup.obscure_inst_type_of_bill('300') == 'X00'
    assert cleanup.obscure_inst_type_of_bill('200') == '200'

def test_age_cap():
    assert cleanup.cap_age('100') == '90'
    assert cleanup.cap_age('85') == '90'
    assert cleanup.cap_age('84') == '84'
    assert cleanup.cap_age('-1') is None
    assert cleanup.cap_age(None) is None
    assert cleanup.cap_age('') is None


def test_year_of_birth_cap():
    # should get capped
    assert cleanup.cap_year_of_birth(None, None, 1800) == 1927
    assert cleanup.cap_year_of_birth(100, None, None) == 1927
    assert cleanup.cap_year_of_birth(
        None, datetime.date(2016, 1, 1), 1915
    ) == 1927

    # should not get capped
    assert cleanup.cap_year_of_birth(
        17, datetime.date(2017, 12, 1), 2000
    ) == 2000

    # error
    assert cleanup.cap_year_of_birth(
        None, None, 'INVALID YEAR'
    ) is None


def test_clean_up_gender():
    assert cleanup.clean_up_gender('F') == 'F'
    assert cleanup.clean_up_gender('f') == 'F'
    assert cleanup.clean_up_gender('M') == 'M'
    assert cleanup.clean_up_gender('  m ') == 'M'
    assert cleanup.clean_up_gender('U') == 'U'
    assert cleanup.clean_up_gender(27) == 'U'
    assert cleanup.clean_up_gender(None) == 'U'
    assert cleanup.clean_up_gender('abcdefg222') == 'U'


def test_clean_up_ndc_code():
    # 11 digits
    assert cleanup.clean_up_ndc_code('00000000000') == '00000000000'
    assert cleanup.clean_up_ndc_code('000000001ab-00') == '00000000100'

    # 10 digits
    assert cleanup.clean_up_ndc_code('0000000000') == '0000000000'
    assert cleanup.clean_up_ndc_code('000000001-0') == '0000000010'

    # other digit counts
    assert cleanup.clean_up_ndc_code('0000000') is None
    assert cleanup.clean_up_ndc_code('abc') is None
    assert cleanup.clean_up_ndc_code(None) is None
    assert cleanup.clean_up_ndc_code('00000000000000000000000000000') is None


def test_clean_up_procedure_code():
    # long first word
    assert cleanup.clean_up_procedure_code('PROCEDURE CODE') == 'PROCEDU'

    # short first word
    assert cleanup.clean_up_procedure_code('PRO CEDURECODE') == 'PRO'

    # alphanumeric
    assert cleanup.clean_up_procedure_code('PR0!1! CEDURECODE') == 'PR0'

    # upper
    assert cleanup.clean_up_procedure_code('pro!! cedurecode') == 'PRO'

    # none
    assert cleanup.clean_up_procedure_code(None) is None


def test_clean_up_diagnosis_code():
    # no code
    assert cleanup.clean_up_diagnosis_code(None, None, None) is None

    # messy code
    assert cleanup.clean_up_diagnosis_code('v.8541', '01', None) == 'V854'

    # should clean based on date with no qual
    assert cleanup.clean_up_diagnosis_code(
        'Z6842', None, datetime.date(2016, 1, 1)
    ) == 'Z684'
    assert cleanup.clean_up_diagnosis_code(
        'Z6842', None, datetime.date(2015, 1, 1)
    ) == 'Z6842'
    assert cleanup.clean_up_diagnosis_code(
        '7999', None, datetime.date(2016, 1, 1)
    ) == '7999'
    assert cleanup.clean_up_diagnosis_code(
        '7999', None, datetime.date(2015, 1, 1)
    ) is None

    # no qualifier and no date
    # both filters are applied
    assert cleanup.clean_up_diagnosis_code(
        'Z6845', None, None
    ) == 'Z684'
    # potential false positive, could be an ICD-9 general exam code
    assert cleanup.clean_up_diagnosis_code(
        'V700', None, None
    ) is None
    # ICD-9 that should be filtered out
    assert cleanup.clean_up_diagnosis_code(
        '767.4', None, None
    ) is None

    # good ICD-10 code, do nothing
    assert cleanup.clean_up_diagnosis_code(
        'I25.10', None, None
    ) == 'I2510'
    assert cleanup.clean_up_diagnosis_code(
        'I25.10', '02', None
    ) == 'I2510'
    assert cleanup.clean_up_diagnosis_code(
        'I25.10', None, datetime.date(2016, 1, 1)
    ) == 'I2510'
    assert cleanup.clean_up_diagnosis_code(
        'I25.10', '02', datetime.date(2016, 1, 1)
    ) == 'I2510'

    # good ICD-9 code, do nothing
    assert cleanup.clean_up_diagnosis_code(
        '414.00', None, None
    ) == '41400'
    assert cleanup.clean_up_diagnosis_code(
        '414.00', '01', None
    ) == '41400'
    assert cleanup.clean_up_diagnosis_code(
        '414.00', None, datetime.date(2015, 1, 1)
    ) == '41400'
    assert cleanup.clean_up_diagnosis_code(
        '414.00', '01', datetime.date(2015, 1, 1)
    ) == '41400'

def test_zip_code_masking():
    # no zip code
    assert cleanup.mask_zip_code(None) is None

    # low population zip code
    assert cleanup.mask_zip_code("823") == "000"

    # valid zip code
    assert cleanup.mask_zip_code("190") == "190"


def test_state_validation():
    # no state
    assert cleanup.validate_state_code(None) is None

    # valid state
    assert cleanup.validate_state_code('pa') == 'PA'
    assert cleanup.validate_state_code('PA') == 'PA'
    assert cleanup.validate_state_code('PA ') == 'PA'

    # invalid state
    assert cleanup.validate_state_code('') is None
    assert cleanup.validate_state_code(9) is None
    assert cleanup.validate_state_code('inv') is None


def test_vital_sign_clean_up():
    # vital sign that we don't clean up
    assert cleanup.clean_up_vital_sign('O2_SATURATION', '85', 'PERCENT', 'M', '48', None, None, None) == '85'

    # Peter Dinklage
    # Age, no YOB
    assert cleanup.clean_up_vital_sign('HEIGHT', '53', 'INCHES', 'M', '48', None, None, None) \
        is None

    # No age, YOB and measurement date
    assert cleanup.clean_up_vital_sign('HEIGHT', '53', 'INCHES', 'M', None, '1969', datetime.datetime(2017, 8, 31), None) \
        is None

    # No age, YOB and encounter date
    assert cleanup.clean_up_vital_sign('HEIGHT', '53', 'INCHES', 'M', None, '1969', None, datetime.datetime(2017, 8, 31)) \
        is None

    # No age, no YOB, yes encounter date
    assert cleanup.clean_up_vital_sign('HEIGHT', '53', 'INCHES', 'M', None, None, None, datetime.datetime(2017, 8, 31)) \
        is None

    # No age, no gender
    assert cleanup.clean_up_vital_sign('HEIGHT', '53', 'INCHES', None, None, None, None, datetime.datetime(2017, 8, 31)) \
        is None

    # George Clooney
    # Age, no YOB
    assert cleanup.clean_up_vital_sign('HEIGHT', '73', 'INCHES', 'M', '56', None, None, None) \
        == '73'

    # No age, YOB and measurement date
    assert cleanup.clean_up_vital_sign('HEIGHT', '73', 'INCHES', 'M', None, '1961', datetime.datetime(2017, 8, 31), None) \
        == '73'

    # No age, YOB and encounter date
    assert cleanup.clean_up_vital_sign('HEIGHT', '73', 'INCHES', 'M', None, '1961', None, datetime.datetime(2017, 8, 31)) \
        == '73'

    # No age, no YOB, yes encounter date
    assert cleanup.clean_up_vital_sign('HEIGHT', '73', 'INCHES', 'M', None, None, None, datetime.datetime(2017, 8, 31)) \
        is None

    # No age, no gender
    assert cleanup.clean_up_vital_sign('HEIGHT', '73', 'INCHES', None, None, None, None, datetime.datetime(2017, 8, 31)) \
        is None

    # Shaquille O'Neal
    # Age, no YOB
    assert cleanup.clean_up_vital_sign('HEIGHT', '85', 'INCHES', 'M', '45', None, None, None) \
        is None


    # Nicole Kidman
    assert cleanup.clean_up_vital_sign('HEIGHT', '71', 'INCHES', 'F', '50', None, None, None) \
        is None

    # Halle Berry
    assert cleanup.clean_up_vital_sign('HEIGHT', '65', 'INCHES', 'F', '51', None, None, None) \
        == '65'

    # High weight
    assert cleanup.clean_up_vital_sign('WEIGHT', '260', 'POUNDS', 'F', '51', None, None, None) \
        is None

    # Low weight
    assert cleanup.clean_up_vital_sign('WEIGHT', '90', 'POUNDS', 'F', '51', None, None, None) \
        is None

    # Normal weight
    assert cleanup.clean_up_vital_sign('WEIGHT', '140', 'POUNDS', 'F', '51', None, None, None) \
        == '140'

    # High BMI index
    assert cleanup.clean_up_vital_sign('BMI', '45.6', 'INDEX', 'F', '51', None, None, None) \
        is None

    # Low BMI index
    assert cleanup.clean_up_vital_sign('BMI', '17.1', 'INDEX', 'F', '51', None, None, None) \
        is None

    # Normal BMI index
    assert cleanup.clean_up_vital_sign('BMI', '22.5', 'INDEX', 'F', '51', None, None, None) \
        == '22.5'

    # High BMI percentile
    assert cleanup.clean_up_vital_sign('BMI', '99.5', 'PERCENT', 'F', '51', None, None, None) \
        is None

    # Low BMI percentile
    assert cleanup.clean_up_vital_sign('BMI', '0.5', 'PERCENT', 'F', '51', None, None, None) \
        is None

    # Normal BMI percentile
    assert cleanup.clean_up_vital_sign('BMI', '85', 'PERCENT', 'F', '51', None, None, None) \
        == '85'
