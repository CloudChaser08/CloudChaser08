import datetime 
from dateutil.relativedelta import relativedelta #http://dateutil.readthedocs.io/en/stable/relativedelta.html

def is_date(year, month, day): 

    """
    Uses a datetime object to validate UI date.
    """
    try:
        datetime.date(year, month, day)
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
    month, day. Values default to zero.
    """

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

    # If all offsets are None, return string formatted datetime object with no offset else
    # return string formatted datetime with appropriate offset
    if year_offset == 0 and month_offset == 0 and day_offset == 0:
        return datetime.date(year, month, day).strftime('%Y%m%d')
    else:
        return (datetime.datetime(year,month,day) + relativedelta(years = year_offset, months = month_offset, days = day_offset)).strftime('%Y%m%d')
    

def date_into_template_generator(template,  # string to pass date into
                            year = None,  # user inputted year
                            month = None,  # user inputted month
                            day = None,  # user inputted day
                            year_offset = None,  # integer to add to year, defaults to 0
                            month_offset = None,  # integer to add to month, defaults to 0
                            day_offset = None,  # integer to add to day, defaults to 0
                            ):
    """
    Inserts the ymd formatted date into a string template. Values 
    of year, month, and day can be specified by the user individually. 
    These default to those given by the execution_date. The parameters
    year_offset, month_offset, and day_offset are integers to add to the
    respective values.   
    """
    if year is None: 
        year = kwargs['execution_date'].year
    if month is None:
        month= kwargs['execution_date'].month
    if day is None:
        day = kwargs['execution_date'].day

    if is_date(year, month, day):
        pass
    else:
        raise Exception('Please enter a valid date.')
    date = [year, month, day] 

    date_string = offset_date(date[0], date[1], date[2], year_offset, month_offset, day_offset)

    def out(ds, kwargs):

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

    return date_into_template_generator(template, year, month, day, year_offset, month_offset, day_offset)(None, kwargs)

