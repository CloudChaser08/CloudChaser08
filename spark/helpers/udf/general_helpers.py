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

def extract_date(text, pattern, min_date, max_date):
    if text is None or text == '':
        return None
    try:
        d = datetime.strptime(text, pattern).date()
    except Exception:
        return None

    if d < min_date or d > max_date:
        return None
    return datetime.strftime(d, '%Y-%m-%d')
