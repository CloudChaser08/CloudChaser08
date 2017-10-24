import datetime 
from dateutil.relativedelta import relativedelta #http://dateutil.readthedocs.io/en/stable/relativedelta.html
import pytest
from util.date_utils import *


kwargs={'execution_date': datetime.datetime(2017, 10, 13), \
        'ds_nodash':'20171025'}

TEMPLATE = '{}-{}-{}' #example template to input date

def test_is_date_input():
    """
    test is_date boolean is False with valid and invalid input
    """
   #year
    assert not is_date(0, 10, 1) 
    assert not is_date('It\'s...', 10, 1)
    #month
    assert not is_date(2017, 0, 1) 
    assert not is_date(2017, 13, 1) 
    assert not is_date(2017, 'Nobody expects the Sp-', 1) 
    #day
    assert not is_date(2017, 10, 0) 
    assert not is_date(2017, 10, 32) 
    assert not is_date(2017, 2, 29)

    # test is_date boolean is True with valid input
    assert is_date(2017,10,10)

def test_offset_date_with_valid_and_invalid_input():
    """
    test offset_date raises appropriate Exceptions and Errors with invalid input
    """
    with pytest.raises(Exception) as e_info:
        offset_date(1899, 10, 10)
    exception = e_info.value
    assert exception.message.startswith('Year must be >= 1900')

    with pytest.raises(ValueError) as e_info:
        offset_date(2000, 10, 0)
    exception = e_info.value
    assert exception.message.startswith('day is out of range')

    with pytest.raises(TypeError) as e_info:
        offset_date('something','comepletely', 'different')
    exception = e_info.value
    assert exception.message.startswith('an integer is required')

    with pytest.raises(ValueError) as e_info:
        offset_date(2012, 2, 30)
    exception = e_info.value
    assert exception.message.startswith('day is out of range')

    with pytest.raises(Exception) as e_info:
        offset_date(2012, 10, 10, year_offset = '')
    exception = e_info.value
    assert exception.message.startswith('year_offset must be an integer')

    with pytest.raises(ValueError) as e_info:
        offset_date(2012, -10, 10)
    exception = e_info.value
    assert exception.message.startswith('month must be in 1..12')

    with pytest.raises(ValueError) as e_info:
        offset_date(2012, 10, -30)
    exception = e_info.value
    assert exception.message.startswith('day is out of range')

    # test offset_date returns datetime object of appropriately shifted date
    
    assert offset_date(2012, 10, 1) == ('2012', '10', '01')
    assert offset_date(2012, 10, 1, year_offset = 1) == ('2013',
     '10', '01')
    assert offset_date(2012, 10, 1, month_offset = 1) == ('2012', '11', '01')
    assert offset_date(2012, 10, 1, day_offset = 1) == ('2012', '10', '02')
    # negative input for offsets
    assert offset_date(2012, 10, 1, year_offset = -1) == ('2011', '10', '01')
    assert offset_date(2012, 10, 1, month_offset = -1) == ('2012', '09', '01')
    assert offset_date(2012, 10, 1, day_offset = -1) == ('2012', '09', '30')
    # try all
    assert offset_date(2012, 10, 1, day_offset = 1, month_offset = -1, \
        year_offset = 12) == ('2024', '09', '02')

def test_date_into_template_with_all_input():
    """
    test wrapper function with all inputs given
    """
    assert insert_date_into_template(TEMPLATE, kwargs, year = 1991, day = 6, \
        month = 10, year_offset = 21, day_offset = 11, month_offset = 0) == '2012-10-17'




