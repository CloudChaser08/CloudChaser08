import datetime 
from dateutil.relativedelta import relativedelta
import pytest

def is_date(year, month, day):
    """
    Uses a datetime object to validate a user-inputted date. Returns a boolean value.
    """
    try:
        datetime.datetime(year, month, day)
        return True
    except (ValueError, TypeError, SyntaxError):
        return False

def offset_date(year, 
                month, 
                day, 
                year_offset = 0, 
                month_offset = 0, 
                day_offset = 0
                ):
    """
    Adds the integers year_offset, month_offset, day_offset to the year,
    month, day. Offset values default to zero.
    """
    if year < 1900:
        raise ValueError('Year must be >= 1900') # strftime requires year 
                                                 # >= 1900

    if type(year_offset) in (str,float):
        raise TypeError('year_offset must be an integer')
    if type(month_offset) in (str,float):
        raise TypeError('month_offset must be an integer')
    if type(day_offset) in (str,float):
        raise TypeError('day_offset must be an integer')

    date = (datetime.datetime(year,month,day) + 
                relativedelta(years = year_offset, 
                    months = month_offset, 
                    days = day_offset)).strftime('%Y%m%d')
                
    return (date[0:4], date[4:6], date[6:8])
    
def generate_insert_date_into_template_function(template,  # string to pass date into
                                date, # user inputted date
                                year = None,  # user inputted year
                                month = None,  # user inputted month
                                day = None,  # user inputted day
                                year_offset = 0,  # integer to add to year, defaults to 0
                                month_offset = 0,  # integer to add to month, defaults to 0
                                day_offset = 0,  # integer to add to day, defaults to 0
                                ):
    """
    Inserts the year, month, day into a string template. The date variable can be inputted as a datetime object or one of Airflow's date variables. Values
    of year, month, and day can be specified by the user individually. 
    These default to those given by the airflow execution_date. The parameters
    year_offset, month_offset, and day_offset are integers to add to the
    respective values.   
    """

    nodash_list = ['kwargs[\'ds_nodash\']', 'kwargs[\'yesterday_ds_nodash\']', 'kwargs[\'tomorrow_ds_nodash\']', 'kwargs[\'ts_nodash\']']
    dash_list = ['kwargs[\'ds\']', 'kwargs[\'yesterday_ds\']', 'kwargs[\'tomorrow_ds\']', 'kwargs[\'ts\']']

    def out(ds, kwargs):
        
        if type(date) is datetime.datetime:
            output_year = date.year if year is None else year  
            output_month = date.month if month is None else month    
            output_day = date.day if day is None else day

        elif date in nodash_list:
            output_year = int(date[0:4]) if year is None else year  
            output_month = int(date[4:6]) if month is None else month    
            output_day = int(date[6:8]) if day is None else day

        elif date in dash_list:
            output_year = int(date[0:4]) if year is None else year  
            output_month = int(date[5:7]) if month is None else month    
            output_day = int(date[8:10]) if day is None else day

        else: 
            raise TypeError('date must be a datetime object or an Airflow date string varible (e.g. datetime.datetime.now() or kwargs[\'ds\'])')

        if not is_date(output_year, output_month, output_day):
            raise ValueError('Please enter a valid date. You entered: \
                year: {}, month: {}, day: {}'.format(year,month,day)) 

        (output_year, output_month, output_day) = offset_date(output_year,\
            output_month, output_day, year_offset, month_offset, day_offset)

        return template.format(
            output_year,
            output_month,
            output_day
        )

    return out

def insert_date_into_template(template,
                        date, 
                        kwargs, 
                        year = None, 
                        month = None,  
                        day = None, 
                        year_offset = 0, 
                        month_offset = 0, 
                        day_offset = 0, 
                        ):
    """
    Wrapper for generate_insert_date_into_template_function
    """
    return generate_insert_date_into_template_function(template, date, year, month, \
        day, year_offset, month_offset, day_offset)(None, kwargs)
