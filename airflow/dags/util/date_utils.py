import datetime
from datetime import timedelta

"""This is a collection of functions to retrieve and fill date information."""

def date_function(template, kwargs, date_offset, year = None, month = None, day = None):
    
    """Inserts the Ymd formatted date into a string template. Values 
    of year, month, day can be specified by the user. These default to 
    those given by the execution_date. 
    """
    date_string = (kwargs['execution_date'] + timedelta(date_offset)).strftime('%Y%m%d')

    if year == None:
        year = date_string[0:4]
    if month == None:
        month = date_string[4:6]
    if day == None:
        day = date_string[6:8]

    return template.format(
        year,
        month,
        day
    )
