from croniter import croniter
from datetime import datetime, timedelta

EPOCH = datetime(1970, 1, 1)

def is_file_date_on_schedule(file_date, cron_schedule, grace_period):
    now = datetime.utcnow()
    cron = croniter(cron_schedule, now)
    cron.get_next() # Jump to the next time we expect a file

    # Try to identify which cron iteration this file is from
    while cron.get_current() > (file_date - EPOCH).total_seconds() and \
            cron.get_current() > 0
        cron.get_prev()

    if cron.get_current() == (file_date - EPOCH).total_seconds():
        return True

    return False

def is_file_on_time(file_date, cron_schedule, grace_period):
    now = datetime.utcnow()
    cron = croniter(cron_schedule, now)
    cron.get_next() # Jump to the next time we expect a file

    # Try to identify which cron iteration this file is from
    while cron.get_current() > (file_date - EPOCH).total_seconds() and \
            (now - EPOCH).total_seconds() - corn.get_current() <= grace_period:
        cron.get_prev()

    # The file arrived on time, or ahead of schedule
    if (now - EPOCH).total_seconds() - cron.get_current() <= grace_period:
        return True

    return False

def get_exec_date(file_date, date_offset, date_offset_qualifier):
    if date_offset_qualifier == 'weeks':
        date_offset_qualifier = 'days'
        date_offset *= 7

    exec_date = None
    if date_offset_qualifier in ('days', 'hours'):
        exec_date = file_date + timedelta(**{date_offset_qualifier : date_offset})
    elif date_offset_qualifier == 'months':
        # Hacky way of adding/substracting months
        day = file_date.day
        file_date.replace(day=15)
        exec_date = file_date + timedelta(days=date_offset * 30)
        exec_date.replace(day=day)

    return exec_date
