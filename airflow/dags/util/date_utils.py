import datetime 
from dateutil.relativedelta import relativedelta #http://dateutil.readthedocs.io/en/stable/relativedelta.html

def date_validator(year, month, day, year_offset, month_offset, day_offset):
    
    """
    Uses a datetime object to validate user input as well as the date itself.
    """
    date_string = (datetime.date(year,month,day) + relativedelta(years = year_offset, months = month_offset, days = day_offset)).strftime('%Y%m%d')
    return {'year':date_string[0:4], 
            'month': date_string[4:6], 
            'day': date_string[6:8]}

def date_inserter_function(template,  # string to pass date into
                            year_offset,  # integer to add to year, defaults to 0
                            month_offset,  # integer to add to month, defaults to 0
                            day_offset,  # integer to add to day, defaults to 0
                            year,  # user inputted year
                            month,  # user inputted month
                            day  # user inputted day
                            ):
    """
    Inserts the ymd formatted date into a string template. Values 
    of year, month, and day can be specified by the user individually. 
    These default to those given by the execution_date. The parameters
    year_offset, month_offset, and day_offset are integers to add to the
    respective values.   

    """ 

    if year_offset is None:
        year_offset = 0
    elif type(year_offset) is str:
        raise Exception('Error: year_offset must be an integer')
        exit(0)
    if month_offset is None:
        month_offset = 0
    elif type(month_offset) is str:
        raise Exception('Error: month_offset must be an integer')
        exit(0)
    if day_offset is None:
        day_offset = 0
    elif type(day_offset) is str:
        raise Exception('Error: day_offset must be an integer')
        exit(0)

    if year is None: 
        year = kwargs['execution_date'].year
    if month is None:
        month = kwargs['execution_date'].month
    if day is None:
        day = kwargs['execution_date'].day

    date = date_validator(year, month, day, year_offset, month_offset, day_offset)

    def out(ds, kwargs):

        return template.format(
            date['year'],
            date['month'],
            date['day']
            )

    return out

def date_inserter(template, kwargs, year_offset = None, month_offset = None, day_offset = None, year = None, month = None, day = None):
    return date_inserter_function(template, year_offset, month_offset, day_offset, year, month, day)(None, kwargs)
