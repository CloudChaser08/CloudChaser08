from datetime import datetime, date
from dateutil.relativedelta import relativedelta

def is_date(year, month, day):
    """
    Uses a datetime object to validate a user-inputted date. Returns a boolean value.
    """
    try:
        datetime(year, month, day)
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

    date = (datetime(year,month,day) +
                relativedelta(years = year_offset,
                    months = month_offset,
                    days = day_offset)).strftime('%Y%m%d')

    return (int(date[0:4]), int(date[4:6]), int(date[6:8]))

def generate_insert_date_into_template_function(template,  # string to pass date into
                                fixed_year = None,  # user inputted year
                                fixed_month = None,  # user inputted month
                                fixed_day = None,  # user inputted day
                                year_format = '%Y', # format of output year
                                month_format = '%m', # format of output month
                                day_format = '%d', # format of output day
                                year_offset = 0,  # integer to add to year, defaults to 0
                                month_offset = 0,  # integer to add to month, defaults to 0
                                day_offset = 0,  # integer to add to day, defaults to 0
                                ):
    """
    Inserts the year, month, day into a string template. The date
    defaults to the execution_date, but fixed values of year, month,
    and day can be specified individually by the user withthe
    variables fixed_year, fixed_month, fixed_day. The parameters
    year_offset, month_offset, and day_offset are integers to add to
    the respective values.
    """
    def out(ds, kwargs):

        output_year = kwargs['execution_date'].year if fixed_year is None else fixed_year
        output_month = kwargs['execution_date'].month if fixed_month is None else fixed_month
        output_day = kwargs['execution_date'].day if fixed_day is None else fixed_day

        if not is_date(output_year, output_month, output_day):
            raise ValueError('Please enter a valid date. You entered: \
                year: {}, month: {}, day: {}'.format(output_year,output_month,output_day))

        (output_year, output_month, output_day) = offset_date(
            output_year, output_month, output_day, year_offset, month_offset, day_offset
        )

        formatted_year, formatted_month, formatted_day = date(
            output_year, output_month, output_day
        ).strftime('{}-{}-{}'.format(year_format, month_format, day_format)).split('-')

        return template.format(
            formatted_year,
            formatted_month,
            formatted_day
        )

    return out

def insert_date_into_template(template,
                        kwargs,
                        fixed_year = None,
                        fixed_month = None,
                        fixed_day = None,
                        year_format = '%Y',
                        month_format = '%m',
                        day_format = '%d',
                        year_offset = 0,
                        month_offset = 0,
                        day_offset = 0,
                        ):
    """
    Wrapper for generate_insert_date_into_template_function
    """
    return generate_insert_date_into_template_function(
        template, fixed_year=fixed_year, fixed_month=fixed_month, fixed_day=fixed_day,
        year_format=year_format, month_format=month_format, day_format=day_format,
        year_offset=year_offset, month_offset=month_offset, day_offset=day_offset
    )(None, kwargs)

def generate_insert_regex_into_template_function(template,
    year_regex = '\d{4}',
    month_regex =  '\d{2}',
    day_regex =  '\d{2}',
    custom_pattern = None
    ):
    """
    Inserts the year pattern, month pattern, and day pattern into a string template. User can specify a custom pattern string through the variable custom_pattern.
    """
    def out(ds, kwargs):
        if custom_pattern is None:
            return template.format(
                year_regex, month_regex, day_regex,
            )
        else:
            return template.format(
                custom_pattern,'',''
            )

    return out
