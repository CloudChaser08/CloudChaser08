import datetime 
from dateutil.relativedelta import relativedelta #http://dateutil.readthedocs.io/en/stable/relativedelta.html
import pytest

def is_date(year, month, day):
    """
    Uses a datetime object to validate UI date.
    """
    try:
        datetime.datetime(year, month, day)
        return True
    except (ValueError, TypeError, SyntaxError):
        return False

def offset_date(year, 
                month, 
                day, 
                year_offset = None, 
                month_offset = None, 
                day_offset = None):
    """
    Adds the integers year_offset, month_offset, day_offset to the year,
    month, day. Offset values default to zero.
    """
    if year < 1900:
        raise Exception('Year must be >= 1900') #strftime requires year >= 1900

    if year_offset is None:
        year_offset = 0
    elif type(year_offset) is str:
        raise Exception('year_offset must be an integer')
    if month_offset is None:
        month_offset = 0
    elif type(month_offset) is str:
        raise Exception('month_offset must be an integer')
    if day_offset is None:
        day_offset = 0
    elif type(day_offset) is str:
        raise Exception('day_offset must be an integer')

    if year_offset == 0 and month_offset == 0 and day_offset == 0: 
        return datetime.datetime(year, month, day)                     
    else:
        return (datetime.datetime(year,month,day) + 
                relativedelta(years = year_offset, months = month_offset, days = day_offset))

def date_to_string(datetime_object):
    """
    Takes a datetime object and returns the 'ymd' formatted string
    """
    if type(datetime_object) is datetime.datetime or type(datetime_object) is datetime.date: 
        return datetime_object.strftime('%Y%m%d')
    
def date_into_template_generator(template,  # string to pass date into
                                year = None,  # user inputted year
                                month = None,  # user inputted month
                                day = None,  # user inputted day
                                year_offset = None,  # integer to add to year, defaults to 0
                                month_offset = None,  # integer to add to month, defaults to 0
                                day_offset = None,  # integer to add to day, defaults to 0
                                ):
    """
    Inserts the year, month, day into a string template. Values 
    of year, month, and day can be specified by the user individually. 
    These default to those given by the execution_date. The parameters
    year_offset, month_offset, and day_offset are integers to add to the
    respective values.   
    """
    def out(ds, kwargs):

        execution_year = kwargs['execution_date'].year if year is None else year  
        execution_month = kwargs['execution_date'].month if month is None else month    
        execution_day = kwargs['execution_date'].day if day is None else day
        
        if is_date(execution_year, execution_month, execution_day):
            pass
        else:
            raise Exception('Please enter a valid date. You entered: year: %s, month: %s, day: %s' % (year,month,day)) #print inputted date 

        date_string = date_to_string(offset_date(execution_year, execution_month, execution_day, year_offset, month_offset, day_offset))

        return template.format(
            date_string[0:4],
            date_string[4:6],
            date_string[6:8]
        )

    return out

def date_into_template(template, 
                        kwargs, 
                        year = None, 
                        month = None,  
                        day = None, 
                        year_offset = None, 
                        month_offset = None, 
                        day_offset = None, 
                        ):
    """
    Wrapper for date_into_template_generator function
    """
    return date_into_template_generator(template, year, month, day, year_offset, month_offset, day_offset)(None, kwargs)
