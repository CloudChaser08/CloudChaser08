from croniter import croniter
from datetime import datetime

EPOCH = datetime(1970, 1, 1)

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
