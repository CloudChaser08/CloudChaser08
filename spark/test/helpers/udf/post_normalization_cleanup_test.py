import datetime
import spark.helpers.udf.post_normalization_cleanup as cleanup


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
