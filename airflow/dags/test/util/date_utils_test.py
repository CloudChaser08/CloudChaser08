import datetime 
from dateutil.relativedelta import relativedelta #http://dateutil.readthedocs.io/en/stable/relativedelta.html
import pytest
from util.date_utils import *


kwargs={'execution_date': datetime.datetime(2017, 10, 13),'ds_nodash':'20171025'} # example airflow variable, kwargs{'execution_date'} is a datetime.datetime object

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
    with pytest.raises(Exception):
        offset_date(1899, 10, 10) 
    with pytest.raises(ValueError):
        offset_date(2000, 10, 0)
    with pytest.raises(TypeError):
        offset_date('something','comepletely', 'different')
    with pytest.raises(ValueError):
        offset_date(2012, 2, 30)
    with pytest.raises(Exception):
        offset_date(2012, 10, 10, year_offset = '')
    with pytest.raises(ValueError):
        offset_date(2012, -10, 10)
    with pytest.raises(ValueError):
        offset_date(2012, 10, -30)

    # test offset_date returns datetime object of appropriately shifted date
    
    assert offset_date(2012, 10, 1) == datetime.datetime(2012, 10, 1)
    assert offset_date(2012, 10, 1, year_offset = 1) == datetime.datetime(2013,
     10, 1)
    assert offset_date(2012, 10, 1, month_offset = 1) == datetime.datetime(2012, 11, 1)
    assert offset_date(2012, 10, 1, day_offset = 1) == datetime.datetime(2012, 10, 2)
    # negative input for offsets
    assert offset_date(2012, 10, 1, year_offset = -1) == datetime.datetime(2011, 10, 1)
    assert offset_date(2012, 10, 1, month_offset = -1) == datetime.datetime(2012, 9, 1)
    assert offset_date(2012, 10, 1, day_offset = -1) == datetime.datetime(2012, 9, 30)
    # try all
    assert offset_date(2012, 10, 1, day_offset = 1, month_offset = -1, year_offset = 12) == datetime.datetime(2024, 9, 2)

def test_date_into_template_with_all_input():
    """
    test wrapper function with all inputs given
    """
    assert date_into_template(TEMPLATE, kwargs, year = 1991, day = 06, month = 10, year_offset = 21, day_offset = 11, month_offset = 0) == '2012-10-17'




