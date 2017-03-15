import datetime


def yyyyMMdd_to_date(date_input):
    try:
        return datetime.date(
            int(date_input[0:4]),
            int(date_input[4:6]),
            int(date_input[6:8])
        ).strftime('%Y-%m-%d')
    except:
        return None
