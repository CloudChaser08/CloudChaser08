#! /usr/bin/python
from datetime import datetime
import re


def extract_number(text):
    if text is None or text == '':
        return None

    if len(text) - len(text.replace('.','')) == 1:
        try:
            return float(re.sub('[^0-9.]', '', text))
        except Exception:
            return None
    else:
        try:
            return float(re.sub('[^0-9]', '', text))
        except Exception:
            return None


def extract_date(text, pattern, min_date=None, max_date=None):
    if text is None or text == '':
        return None
    try:
        d = datetime.strptime(text, pattern).date()
    except Exception:
        return None

    if min_date is None and max_date is None:
        return datetime.strftime(d, '%Y-%m-%d')

    if d < min_date or d > max_date:
        return None
    return datetime.strftime(d, '%Y-%m-%d')


def create_range(max):
    return ','.join(map(lambda i: str(i), range(max)))
