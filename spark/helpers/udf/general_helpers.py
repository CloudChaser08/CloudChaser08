#! /usr/bin/python
from datetime import datetime
import re
import hashlib


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

    if (
        min_date is not None and d < min_date
    ) or (
        max_date is not None and d > max_date
    ):
        return None
    else:
        # stftime throws an error if the date is before 1900
        return d.isoformat().split("T")[0]


def extract_currency(text):
    try:
        # remove non-numeric characters
        text = re.sub('[^0-9.]', '', text)

        return float(text)
    except:
        return None

    

def create_range(max):
    try:
        return ','.join(map(lambda i: str(i), range(max)))
    except:
        return None

# Takes 2 sets as colon-separated strings, and the returns the difference between
# them as a colon-separated string
def string_set_diff(s1,s2):
    if s1 is None:
        return None
    if s2 is None:
        s2 = ''

    s1s = map(lambda x : x.split('_')[0], filter(lambda x: x is not None and len(x) > 0, s1.split(':')))
    s2s = map(lambda x : x.split('_')[0], filter(lambda x: x is not None and len(x) > 0, s2.split(':')))

    return ':'.join(set(s1s).difference(set(s2s)))

# Takes a list as a colon-sparated string, and returns a unique list of values as
# a colon-separated string
def uniquify(with_dupes):
    if with_dupes is None:
        return None;
    return ':'.join(set(filter(lambda x: x is not None and len(x) > 0, with_dupes.split(':'))))

def obfuscate_hvid(hvid, salt):
    if salt is None or len(salt) == 0:
        raise ValueError("A project-specific salt must be provided to properly obfuscate the HVID")
    hvid = "" if hvid is None else hvid
    return hashlib.md5(hvid + salt).hexdigest().upper()

def slightly_obfuscate_hvid(hvid, key):
    if key is None or len(key) == 0:
        raise ValueError("A project-specific key must be provided to properly obfuscate the HVID")
    if hvid is None:
        return None
    if type(hvid) is not int:
        raise ValueError("Only integer HVIDs are expected")
    res = hvid
    # Pad cyclically so the key length is a multiple of 4
    if len(key) % 4 != 0:
        key = key + key[:4 - len(key) % 4]
    # Do multiple rounds of XORing of the id with different
    # parts of the key
    for i in xrange(len(key) / 4):
        key_p = key[i * 4 : (i + 1) * 4]
        xor = ((ord(key_p[0]) ^ (i * 4)) * (1 << 24) + \
                (ord(key_p[1]) ^ (i * 4 + 1)) * (1 << 16) + \
                (ord(key_p[2]) ^ (i * 4 + 2)) * (1 << 8) + \
                (ord(key_p[3]) ^ (i * 4 + 3)))
        res = res ^ xor
    return res

def slightly_deobfuscate_hvid(hvid, key):
    # Obfuscation and de-obfuscation are symmetric
    return slightly_obfuscate_hvid(hvid, key)
