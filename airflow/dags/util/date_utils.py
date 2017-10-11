import datetime
from datetime import timedelta

"""This is a collection of functions to retrieve and fill date information."""

def get_date(kwargs, date_offset):
    """ Returns date. 
    date_offset is the number of days
    To add to the date (execution_date). 
    For execution date, date_offset = 0,
    For previous date, date_offset = -1, etc. 
    """
    return (kwargs['execution_date'] + timedelta(date_offset)).strftime('%Y%m%d') # uses execution_date rather than ds for ability to add date_offset

def insert_date_function(template):
    """Inserts date into template."""
    def out(kwargs, date_offset):
        ds_nodash = get_date(kwargs, date_offset) # benefit here is ability to specify date_offset, insert execution or file date based on that choice
        return template.format(
            ds_nodash[0:4],
            ds_nodash[4:6],
            ds_nodash[6:8]
        )
    return out

def insert_date(template, kwargs, date_offset):
    """Returns formatted template."""  
    return insert_date_function(template)(ds, kwargs, date_offset)
